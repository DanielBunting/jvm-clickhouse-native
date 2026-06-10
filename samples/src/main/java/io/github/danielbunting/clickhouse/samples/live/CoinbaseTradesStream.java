package io.github.danielbunting.clickhouse.samples.live;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.samples.ClickHouseEnv;
import io.github.danielbunting.clickhouse.samples.Streaming;
import io.github.danielbunting.clickhouse.samples.Streaming.Ingestor;
import io.github.danielbunting.clickhouse.types.Column;

import java.net.URI;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Live-streaming sample that ingests Coinbase trade ("match") events into ClickHouse over
 * the native TCP protocol.
 *
 * <p>Connects to the public Coinbase Exchange WebSocket feed, subscribes to the
 * {@code matches} channel for a handful of products, and ingests parsed trades through the
 * shared {@link Streaming} pieces (the Java twin of the Kotlin {@code Streaming.kt}):
 * <pre>
 *   Streaming.reconnect -> Streaming.readWebSocket -> parseTrade -> Ingestor.offer
 * </pre>
 * The {@link Ingestor}'s monitor serializes all connection use — the WebSocket callback
 * thread triggers flushes/reports inside it, so no extra lock or flusher thread is needed.
 * A JVM shutdown hook performs the final flush and closes the connection.
 *
 * <h2>Field-shape assumptions</h2>
 * <ul>
 *   <li>{@code time} is an ISO-8601 timestamp parseable by {@link Instant#parse(CharSequence)}.</li>
 *   <li>{@code price} and {@code size} are decimal strings parseable by
 *       {@link Double#parseDouble(String)}.</li>
 *   <li>{@code trade_id} is an unsigned 64-bit integer (mapped to ClickHouse {@code UInt64}).</li>
 *   <li>Messages missing any of these fields are skipped or defaulted (see {@code parseTrade}).</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runCoinbase    (Ctrl-C to stop)
 * }</pre>
 */
public final class CoinbaseTradesStream {

    /** Coinbase Exchange public market-data WebSocket endpoint (no authentication required). */
    private static final URI FEED_URI = URI.create("wss://ws-feed.exchange.coinbase.com");

    /** Subscribe message requesting the {@code matches} channel for the chosen products. */
    private static final String SUBSCRIBE_MESSAGE =
            "{\"type\":\"subscribe\","
                    + "\"product_ids\":[\"BTC-USD\",\"ETH-USD\",\"SOL-USD\"],"
                    + "\"channels\":[\"matches\"]}";

    /** Destination table for ingested trades. */
    private static final String TABLE = "crypto_trades";

    /** Maximum number of buffered rows before a flush is triggered. */
    private static final int BATCH = 1_000;

    /** Maximum age (ms) of buffered rows before a time-based flush is triggered. */
    private static final long FLUSH_INTERVAL_MS = 2_000L;

    /** Interval (ms) between throughput / aggregate status reports. */
    private static final long REPORT_INTERVAL_MS = 5_000L;

    /** How long to wait before reconnecting after the feed closes or errors (milliseconds). */
    private static final long RECONNECT_DELAY_MS = 2_000L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CoinbaseTradesStream() {
    }

    /**
     * A single Coinbase trade event. The record component names must match the ClickHouse
     * column names exactly so the bulk-insert mapper can bind them by name.
     *
     * @param time      trade execution time (DateTime64(3) UTC)
     * @param productId trading pair identifier, e.g. {@code BTC-USD}
     * @param side      aggressor side, {@code buy} or {@code sell}
     * @param price     execution price
     * @param size      executed size in base currency
     * @param tradeId   exchange-assigned trade identifier (UInt64)
     */
    public record Trade(Instant time, String productId, String side, double price, double size,
                        long tradeId) {
    }

    /**
     * Entry point. Creates the table, then loops forever: connect, subscribe, ingest, and
     * reconnect whenever the feed closes or errors.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        ClickHouseConnection conn = ClickHouseEnv.open();
        conn.execute(
                "CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                        + "time DateTime64(3,'UTC'), "
                        + "productId String, "
                        + "side String, "
                        + "price Float64, "
                        + "size Float64, "
                        + "tradeId UInt64"
                        + ") ENGINE=MergeTree ORDER BY (productId, time)");

        AtomicBoolean running = new AtomicBoolean(true);
        Ingestor<Trade> ingestor = new Ingestor<>(conn, TABLE, Trade.class,
                BATCH, FLUSH_INTERVAL_MS, REPORT_INTERVAL_MS,
                (total, rate) -> report(conn, total, rate));

        // Flush any buffered rows and close the connection on Ctrl-C / JVM shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                ingestor.close();
            } catch (RuntimeException ex) {
                System.err.println("Shutdown flush failed: " + ex.getMessage());
            }
            conn.close();
        }, "coinbase-shutdown"));

        System.out.println("Streaming Coinbase trades into '" + TABLE + "'. Press Ctrl-C to stop.");

        Streaming.reconnect("coinbase", RECONNECT_DELAY_MS, running::get, () ->
                Streaming.readWebSocket(FEED_URI, SUBSCRIBE_MESSAGE, message -> {
                    Trade trade = parseTrade(message);
                    if (trade != null) {
                        ingestor.offer(trade);
                    }
                }));
    }

    /**
     * Parses a Coinbase feed message into a {@link Trade}, or returns {@code null} when the
     * message is not a trade or is missing required fields. Missing/null fields are defaulted
     * rather than throwing.
     */
    private static Trade parseTrade(String message) {
        try {
            JsonNode node = MAPPER.readTree(message);
            String type = text(node, "type", "");
            if (!"match".equals(type) && !"last_match".equals(type)) {
                return null;
            }

            String timeText = text(node, "time", null);
            Instant time = timeText != null ? Instant.parse(timeText) : Instant.now();
            String productId = text(node, "product_id", "");
            String side = text(node, "side", "");
            double price = parseDouble(text(node, "price", null));
            double size = parseDouble(text(node, "size", null));
            long tradeId = node.hasNonNull("trade_id") ? node.get("trade_id").asLong() : 0L;

            return new Trade(time, productId, side, price, size, tradeId);
        } catch (Exception ex) {
            // Malformed/unparseable message (incl. JSON parse errors): skip it rather than killing the stream.
            return null;
        }
    }

    /** Returns the text value of {@code field} or {@code fallback} when missing/null. */
    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        return (value != null && !value.isNull()) ? value.asText() : fallback;
    }

    /** Parses a decimal string, defaulting to {@code 0.0} on null/blank/unparseable input. */
    private static double parseDouble(String value) {
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ex) {
            return 0.0;
        }
    }

    /**
     * Prints total rows ingested, rows/sec since startup, and a live per-product aggregate.
     * Runs inside the {@link Ingestor} monitor, so using the shared connection is safe.
     */
    private static void report(ClickHouseConnection conn, long total, double rate) {
        System.out.printf("ingested=%d rows, %.1f rows/sec%n", total, rate);
        try (QueryResult result = conn.query(
                "SELECT productId, count() c, round(avg(price),2) avg_price "
                        + "FROM " + TABLE + " GROUP BY productId ORDER BY c DESC")) {
            var blocks = result.blocks();
            while (blocks.hasNext()) {
                Block block = blocks.next();
                if (block.isEmpty()) {
                    continue;
                }
                Column productId = block.column(0);
                Column count = block.column(1);
                Column avgPrice = block.column(2);
                for (int row = 0; row < block.rowCount(); row++) {
                    System.out.printf("  %-10s count=%s avg_price=%s%n",
                            productId.value(row), count.value(row), avgPrice.value(row));
                }
            }
        } catch (RuntimeException ex) {
            System.err.println("Aggregate query failed: " + ex.getMessage());
        }
    }
}
