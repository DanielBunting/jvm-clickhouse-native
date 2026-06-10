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
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-volume streaming sample that ingests the Bluesky Jetstream firehose into ClickHouse.
 *
 * <p>Subscribes to the public Bluesky Jetstream WebSocket endpoint (no authentication
 * required), filtered to {@code app.bsky.feed.post} events, and ingests parsed events
 * through the shared {@link Streaming} pieces (the Java twin of the Kotlin
 * {@code Streaming.kt}):
 * <pre>
 *   Streaming.reconnect -> Streaming.readWebSocket -> parse -> Ingestor.offer
 * </pre>
 * The {@link Ingestor} flushes on the batch-size/flush-interval thresholds and reports
 * (throughput + a live aggregate) on the report interval; a JVM shutdown hook performs the
 * final flush and closes the connection.
 *
 * <p>Jetstream message shape (subject to the upstream service):
 * <pre>
 * {
 *   "did": "did:plc:...",
 *   "time_us": 1700000000000000,        // MICROSECOND epoch
 *   "kind": "commit",
 *   "commit": {
 *     "operation": "create",
 *     "collection": "app.bsky.feed.post",
 *     "record": { "text": "..." }
 *   }
 * }
 * </pre>
 * Non-commit events (e.g. {@code identity}, {@code account}) carry no {@code commit} block;
 * their collection/operation/text default to empty strings.
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runBluesky    (Ctrl-C to stop)
 * }</pre>
 */
public final class BlueskyJetstream {

    /** Table that receives the ingested Jetstream events. */
    private static final String TABLE = "bluesky_events";

    /** WebSocket endpoint for the Jetstream firehose, filtered to feed posts. */
    private static final URI JETSTREAM_URI = URI.create(
            "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post");

    /** Flush the buffer once it reaches this many rows. */
    private static final int BATCH = 1000;

    /** Flush the buffer at least this often, even when it has not reached {@link #BATCH}. */
    private static final long FLUSH_INTERVAL_MS = 2_000L;

    /** Emit a progress + aggregate report at least this often. */
    private static final long REPORT_INTERVAL_MS = 5_000L;

    /** How long to wait before reconnecting after the feed closes or errors (milliseconds). */
    private static final long RECONNECT_DELAY_MS = 2_000L;

    /** Shared JSON parser; {@link ObjectMapper} is thread-safe for read operations. */
    private static final ObjectMapper JSON = new ObjectMapper();

    private BlueskyJetstream() {
    }

    /**
     * A single Bluesky event mapped to the {@code bluesky_events} table. Record component
     * names match the column names exactly so the reflective bulk-insert mapper can bind them.
     *
     * @param time       event timestamp (DateTime64(6,'UTC'))
     * @param did        decentralized identifier of the actor
     * @param kind       Jetstream event kind (e.g. {@code commit})
     * @param collection record collection (e.g. {@code app.bsky.feed.post}), or empty
     * @param operation  commit operation (e.g. {@code create}/{@code delete}), or empty
     * @param text       post text, or empty when absent
     */
    public record BlueskyEvent(Instant time, String did, String kind,
                               String collection, String operation, String text) {
    }

    /**
     * Entry point: opens a connection, ensures the target table exists, then runs an
     * indefinite ingest loop that reconnects on WebSocket close or error.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        ClickHouseConnection conn = ClickHouseEnv.open();

        conn.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                + "time DateTime64(6,'UTC'), "
                + "did String, "
                + "kind String, "
                + "collection String, "
                + "operation String, "
                + "text String"
                + ") ENGINE=MergeTree ORDER BY time");

        AtomicBoolean running = new AtomicBoolean(true);
        Ingestor<BlueskyEvent> ingestor = new Ingestor<>(conn, TABLE, BlueskyEvent.class,
                BATCH, FLUSH_INTERVAL_MS, REPORT_INTERVAL_MS,
                (total, rate) -> report(conn, total, rate));

        // Flush remaining rows and close the connection cleanly on Ctrl-C.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down: flushing remaining rows...");
            running.set(false);
            try {
                ingestor.close();
            } catch (RuntimeException e) {
                System.err.println("Final flush failed: " + e.getMessage());
            }
            try {
                conn.close();
            } catch (Exception e) {
                System.err.println("Close failed: " + e.getMessage());
            }
        }, "bluesky-shutdown"));

        System.out.println("Streaming Bluesky Jetstream into '" + TABLE + "'. Press Ctrl-C to stop.");

        Streaming.reconnect("bluesky", RECONNECT_DELAY_MS, running::get, () ->
                Streaming.readWebSocket(JETSTREAM_URI, null, message -> {
                    BlueskyEvent event = parse(message);
                    if (event != null) {
                        ingestor.offer(event);
                    }
                }));
    }

    /**
     * Parses one Jetstream message; non-commit events keep empty collection/operation/text.
     *
     * @param message one complete JSON message
     * @return the mapped row, or {@code null} when the payload is unparseable
     */
    private static BlueskyEvent parse(String message) {
        try {
            JsonNode root = JSON.readTree(message);

            String did = textOrEmpty(root, "did");
            String kind = textOrEmpty(root, "kind");

            long timeUs = root.path("time_us").asLong(0L);
            Instant time = timeUs > 0
                    ? Instant.ofEpochSecond(timeUs / 1_000_000L, (timeUs % 1_000_000L) * 1_000L)
                    : Instant.now();

            String collection = "";
            String operation = "";
            String text = "";

            JsonNode commit = root.get("commit");
            if (commit != null && !commit.isNull()) {
                operation = textOrEmpty(commit, "operation");
                collection = textOrEmpty(commit, "collection");
                JsonNode record = commit.get("record");
                if (record != null && !record.isNull()) {
                    text = textOrEmpty(record, "text");
                }
            }

            return new BlueskyEvent(time, did, kind, collection, operation, text);
        } catch (Exception e) {
            // Malformed or unexpected payload: skip gracefully rather than tear down the stream.
            System.err.println("Skipping unparseable message: " + e.getMessage());
            return null;
        }
    }

    /** Returns the text value of the named field, or an empty string when absent/null. */
    private static String textOrEmpty(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return (value == null || value.isNull()) ? "" : value.asText("");
    }

    /**
     * Prints throughput and a live aggregate query. Runs inside the {@link Ingestor}
     * monitor, so using the shared connection is safe.
     */
    private static void report(ClickHouseConnection conn, long total, double rate) {
        System.out.printf("[bluesky] total=%d rows  %.0f rows/sec%n", total, rate);
        try (QueryResult result = conn.query(
                "SELECT collection, count() c FROM " + TABLE
                        + " GROUP BY collection ORDER BY c DESC LIMIT 5")) {
            Iterator<Block> blocks = result.blocks();
            while (blocks.hasNext()) {
                Block block = blocks.next();
                if (block.isEmpty()) {
                    continue;
                }
                Column collectionCol = block.column(0);
                Column countCol = block.column(1);
                for (int row = 0; row < block.rowCount(); row++) {
                    System.out.printf("    %-32s %s%n",
                            collectionCol.value(row), countCol.value(row));
                }
            }
        } catch (RuntimeException e) {
            System.err.println("Aggregate query failed: " + e.getMessage());
        }
    }
}
