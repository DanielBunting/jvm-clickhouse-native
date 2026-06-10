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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Live-streaming sample that ingests the Wikimedia {@code recentchange} firehose (an
 * unauthenticated Server-Sent-Events feed of every edit happening across all Wikimedia
 * projects in real time) into a ClickHouse {@code wiki_edits} table using the native-TCP
 * bulk-insert path.
 *
 * <p>Built from the shared {@link Streaming} pieces (the Java twin of the Kotlin
 * {@code Streaming.kt}):
 * <pre>
 *   Streaming.reconnect -> Streaming.readSse -> parse -> Ingestor.offer
 * </pre>
 * The {@link Ingestor} flushes on the batch-size/flush-interval thresholds and reports
 * (throughput + a live aggregate) on the report interval; a JVM shutdown hook performs the
 * final flush and closes the connection so Ctrl-C leaves no rows behind.
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runWikimedia    (Ctrl-C to stop)
 * }</pre>
 */
public final class WikimediaEditsStream {

    /** Wikimedia recentchange SSE endpoint (no authentication required). */
    private static final String STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    /**
     * Descriptive User-Agent. Wikimedia's User-Agent policy rejects generic/default
     * agents (incl. the JDK's {@code Java-http-client}) with HTTP 403, so a real one
     * is required. See https://meta.wikimedia.org/wiki/User-Agent_policy
     */
    private static final String USER_AGENT =
            "clickhouse-native-client-samples/0.1 (https://github.com/DanielBunting/CH.Native; ClickHouse Java client demo)";

    /** Destination table. */
    private static final String TABLE = "wiki_edits";

    /** Flush the buffer once it reaches this many rows. */
    private static final int BATCH = 1000;

    /** Flush the buffer at least this often (milliseconds), even if it is not full. */
    private static final long FLUSH_INTERVAL_MS = 2_000L;

    /** Print progress / the live aggregate roughly this often (milliseconds). */
    private static final long REPORT_INTERVAL_MS = 5_000L;

    /** How long to wait before reconnecting after the stream ends or errors (milliseconds). */
    private static final long RECONNECT_DELAY_MS = 2_000L;

    /** Reused JSON parser; {@link ObjectMapper} is thread-safe for read operations. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WikimediaEditsStream() {
    }

    /**
     * A single Wikimedia recentchange event mapped to the {@code wiki_edits} columns.
     *
     * <p>Record component names exactly match the ClickHouse column names so the native
     * bulk-insert mapper can bind them by name. Boolean source flags ({@code bot},
     * {@code minor}) are stored as {@code UInt8} (0/1); fields that non-edit events omit
     * default to {@code 0} / the empty string.
     *
     * @param dt        event time (UTC), mapped to {@code DateTime('UTC')}
     * @param wiki      database name of the source wiki, e.g. {@code enwiki}
     * @param domain    server host name, e.g. {@code en.wikipedia.org}
     * @param type      event type, e.g. {@code edit}, {@code new}, {@code log}
     * @param title     page title affected by the event
     * @param user      user (or IP) that triggered the event
     * @param bot       {@code 1} if the change was made by a bot, otherwise {@code 0}
     * @param minor     {@code 1} if the edit was flagged minor, otherwise {@code 0}
     * @param comment   edit summary / comment
     * @param lengthOld previous page byte length ({@code 0} if absent)
     * @param lengthNew new page byte length ({@code 0} if absent)
     * @param revOld    previous revision id ({@code 0} if absent)
     * @param revNew    new revision id ({@code 0} if absent)
     */
    public record WikiEdit(Instant dt, String wiki, String domain, String type, String title,
                           String user, int bot, int minor, String comment, long lengthOld,
                           long lengthNew, long revOld, long revNew) {
    }

    /**
     * Entry point: open a connection, create the table, then loop forever reading the SSE
     * stream and ingesting parsed events.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        ClickHouseConnection conn = ClickHouseEnv.open();

        conn.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                + "dt DateTime('UTC'), "
                + "wiki String, "
                + "domain String, "
                + "type String, "
                + "title String, "
                + "user String, "
                + "bot UInt8, "
                + "minor UInt8, "
                + "comment String, "
                + "lengthOld UInt32, "
                + "lengthNew UInt32, "
                + "revOld UInt64, "
                + "revNew UInt64"
                + ") ENGINE=MergeTree ORDER BY (wiki, dt)");

        AtomicBoolean running = new AtomicBoolean(true);
        Ingestor<WikiEdit> ingestor = new Ingestor<>(conn, TABLE, WikiEdit.class,
                BATCH, FLUSH_INTERVAL_MS, REPORT_INTERVAL_MS,
                (total, rate) -> report(conn, total, rate));

        // Flush remaining rows and close cleanly on Ctrl-C / shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                ingestor.close();
            } catch (RuntimeException e) {
                System.err.println("Shutdown flush failed: " + e.getMessage());
            } finally {
                try {
                    conn.close();
                } catch (Exception e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
            System.out.println("Shutdown complete. Total rows ingested: " + ingestor.total());
        }, "wiki-shutdown"));

        System.out.println("Streaming " + STREAM_URL + " into " + TABLE + " (Ctrl-C to stop)...");

        Streaming.reconnect("wikimedia", RECONNECT_DELAY_MS, running::get, () ->
                Streaming.readSse(STREAM_URL, USER_AGENT, running::get, json -> {
                    WikiEdit edit = parse(json);
                    if (edit != null) {
                        ingestor.offer(edit);
                    }
                }));
    }

    /**
     * Parse a single {@code recentchange} JSON event into a {@link WikiEdit}, null-safely
     * supplying defaults for the many fields that non-edit events omit.
     *
     * @param json the JSON payload (the {@code data:} body of an SSE event)
     * @return the mapped row, or {@code null} if the payload could not be parsed
     */
    private static WikiEdit parse(String json) {
        JsonNode node;
        try {
            node = MAPPER.readTree(json);
        } catch (Exception e) {
            return null;
        }

        Instant dt = extractTimestamp(node);

        String wiki = text(node, "wiki");
        String domain = text(node, "server_name");
        String type = text(node, "type");
        String title = text(node, "title");
        String user = text(node, "user");
        String comment = text(node, "comment");

        int bot = node.path("bot").asBoolean(false) ? 1 : 0;
        int minor = node.path("minor").asBoolean(false) ? 1 : 0;

        JsonNode length = node.path("length");
        long lengthOld = length.path("old").asLong(0L);
        long lengthNew = length.path("new").asLong(0L);

        JsonNode revision = node.path("revision");
        long revOld = revision.path("old").asLong(0L);
        long revNew = revision.path("new").asLong(0L);

        return new WikiEdit(dt, wiki, domain, type, title, user, bot, minor, comment,
                lengthOld, lengthNew, revOld, revNew);
    }

    /**
     * Resolve the event timestamp, preferring the ISO-8601 {@code meta.dt} field and falling
     * back to the epoch-seconds {@code timestamp} field, then to {@link Instant#now()}.
     *
     * @param node the parsed event
     * @return a non-null {@link Instant}
     */
    private static Instant extractTimestamp(JsonNode node) {
        // VERIFY: meta.dt is documented as ISO-8601, e.g. "2026-05-30T12:34:56Z"; Instant.parse
        // handles the trailing 'Z'. Offset forms with +hh:mm are NOT accepted by Instant.parse
        // and fall through to the epoch-seconds 'timestamp' field below.
        JsonNode metaDt = node.path("meta").path("dt");
        if (metaDt.isTextual()) {
            String s = metaDt.asText().trim();
            if (!s.isEmpty()) {
                try {
                    return Instant.parse(s);
                } catch (DateTimeParseException ignored) {
                    // fall through to epoch seconds
                }
            }
        }
        JsonNode ts = node.path("timestamp");
        if (ts.isNumber() || (ts.isTextual() && !ts.asText().isEmpty())) {
            long epochSeconds = ts.asLong(0L);
            if (epochSeconds > 0L) {
                return Instant.ofEpochSecond(epochSeconds);
            }
        }
        return Instant.now();
    }

    /**
     * Read a string field, returning the empty string when it is missing or null.
     *
     * @param node  the parent JSON node
     * @param field the field name
     * @return the field's text value, or {@code ""} when absent/null
     */
    private static String text(JsonNode node, String field) {
        JsonNode v = node.path(field);
        if (v.isMissingNode() || v.isNull()) {
            return "";
        }
        return v.asText("");
    }

    /**
     * Print the running total, the average ingest rate, and a small live aggregate query
     * (top wikis by edit count). Runs inside the {@link Ingestor} monitor, so using the
     * shared connection here is safe.
     *
     * @param conn  the open connection
     * @param total total rows ingested so far
     * @param rate  average rows/sec since startup
     */
    private static void report(ClickHouseConnection conn, long total, double rate) {
        System.out.printf("%n[ingested=%d rows, %.1f rows/sec]%n", total, rate);
        System.out.println("Top wikis by edit count:");
        System.out.printf("  %-24s %10s%n", "wiki", "count");
        try (QueryResult result = conn.query(
                "SELECT wiki, count() AS c FROM " + TABLE
                        + " GROUP BY wiki ORDER BY c DESC LIMIT 10")) {
            Iterator<Block> blocks = result.blocks();
            while (blocks.hasNext()) {
                Block block = blocks.next();
                if (block.isEmpty()) {
                    continue;
                }
                Column wikiCol = block.column(0);
                Column countCol = block.column(1);
                for (int row = 0; row < block.rowCount(); row++) {
                    Object wiki = wikiCol.value(row);
                    Object c = countCol.value(row);
                    System.out.printf("  %-24s %10s%n",
                            wiki == null ? "" : wiki.toString(),
                            c == null ? "0" : c.toString());
                }
            }
        } catch (RuntimeException e) {
            System.err.println("Aggregate query failed: " + e.getMessage());
        }
    }
}
