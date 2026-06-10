package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Shared plumbing for the Java live-stream samples — the Java twin of the Kotlin
 * {@code Streaming.kt}. The same shape as the Kotlin pipeline, expressed with callbacks and a
 * monitor instead of flows and a single coroutine:
 *
 * <pre>
 *   source (readSse / readWebSocket)  ->  reconnect(...)  ->  Ingestor.offer(row)
 * </pre>
 *
 * <p>All ClickHouse access (flushes and the periodic report) happens inside the
 * {@link Ingestor}'s monitor, so a source callback thread, a flush, and the shutdown hook can
 * never use the single, non-thread-safe connection concurrently.
 */
public final class Streaming {

    private Streaming() {
        // utility class
    }

    /** Receives the periodic progress report (total rows so far, average rows/sec). */
    @FunctionalInterface
    public interface Reporter {
        /**
         * Called roughly every report interval, inside the {@link Ingestor} monitor — it may
         * safely use the same connection (e.g. for a live aggregate query).
         *
         * @param total      rows ingested so far
         * @param rowsPerSec average ingest rate since the ingestor was created
         */
        void report(long total, double rowsPerSec);
    }

    /**
     * The shared ingest sink: {@link #offer} buffered rows, bulk-inserting whenever the buffer
     * reaches the batch size or the flush interval has passed, and reporting roughly every
     * report interval. {@link #close} performs the final flush — call it from a JVM shutdown
     * hook so Ctrl-C loses no buffered rows (the Java analog of the Kotlin version's
     * {@code NonCancellable} final flush).
     *
     * <p>Thread-safe: every method is synchronized, which also serializes all use of the
     * underlying single-socket connection.
     *
     * @param <T> the row type (a record whose components match the target columns)
     */
    public static final class Ingestor<T> implements AutoCloseable {

        private final ClickHouseConnection conn;
        private final String table;
        private final Class<T> type;
        private final int batchSize;
        private final long flushIntervalMs;
        private final long reportIntervalMs;
        private final Reporter reporter;

        private final List<T> buffer;
        private final long startNanos = System.nanoTime();
        private long total;
        private long lastFlushMs = System.currentTimeMillis();
        private long lastReportMs = System.currentTimeMillis();

        /**
         * @param conn             the open connection (the ingestor serializes all access to it)
         * @param table            destination table
         * @param type             row type for the bulk-insert mapper
         * @param batchSize        flush when the buffer reaches this many rows
         * @param flushIntervalMs  flush at least this often (checked on each {@link #offer})
         * @param reportIntervalMs report roughly this often (checked on each {@link #offer})
         * @param reporter         progress callback (may query over the same connection)
         */
        public Ingestor(ClickHouseConnection conn, String table, Class<T> type,
                        int batchSize, long flushIntervalMs, long reportIntervalMs,
                        Reporter reporter) {
            this.conn = conn;
            this.table = table;
            this.type = type;
            this.batchSize = batchSize;
            this.flushIntervalMs = flushIntervalMs;
            this.reportIntervalMs = reportIntervalMs;
            this.reporter = reporter;
            this.buffer = new ArrayList<>(batchSize);
        }

        /** Buffers one row, flushing and/or reporting when the size or time thresholds hit. */
        public synchronized void offer(T row) {
            buffer.add(row);
            long now = System.currentTimeMillis();
            if (buffer.size() >= batchSize || now - lastFlushMs >= flushIntervalMs) {
                flushLocked();
                lastFlushMs = now;
            }
            if (now - lastReportMs >= reportIntervalMs) {
                double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
                reporter.report(total, elapsedSec > 0 ? total / elapsedSec : 0.0);
                lastReportMs = now;
            }
        }

        /** Bulk-inserts and clears the buffer; a no-op when it is empty. */
        public synchronized void flush() {
            flushLocked();
        }

        /** Total rows ingested so far. */
        public synchronized long total() {
            return total;
        }

        /** Final flush (for the shutdown hook). Does not close the connection. */
        @Override
        public synchronized void close() {
            flushLocked();
        }

        private void flushLocked() {
            if (buffer.isEmpty()) {
                return;
            }
            try (BulkInserter<T> ins = conn.createBulkInserter(table, type)) {
                ins.init();
                ins.addRange(buffer);
                ins.complete();
                total += buffer.size();
            } finally {
                buffer.clear();
            }
        }
    }

    /**
     * Reads one Server-Sent-Events session, passing each non-empty {@code data:} payload to
     * [onData], and returns when the stream ends (or [running] turns false). Throws on
     * connect/HTTP errors — wrap in {@link #reconnect}.
     *
     * @param url       the SSE endpoint
     * @param userAgent the {@code User-Agent} to send (some feeds reject generic agents)
     * @param running   checked between events; return false to stop reading
     * @param onData    receives each event's JSON payload
     * @throws Exception on connect or read failure
     */
    public static void readSse(String url, String userAgent, BooleanSupplier running,
                               Consumer<String> onData) throws Exception {
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("Accept", "text/event-stream")
                .header("User-Agent", userAgent)
                .GET()
                .build();
        HttpResponse<Stream<String>> response =
                http.send(request, HttpResponse.BodyHandlers.ofLines());
        if (response.statusCode() != 200) {
            response.body().close();
            throw new IllegalStateException("stream returned HTTP " + response.statusCode());
        }
        try (Stream<String> lines = response.body()) {
            Iterator<String> it = lines.iterator();
            while (running.getAsBoolean() && it.hasNext()) {
                String line = it.next();
                if (line == null || !line.startsWith("data:")) {
                    continue;
                }
                String json = line.substring("data:".length()).trim();
                if (!json.isEmpty()) {
                    onData.accept(json);
                }
            }
        }
    }

    /**
     * Runs one WebSocket session: connects, optionally sends [subscribeMessage], reassembles
     * fragmented text frames, passes each complete message to [onMessage], and blocks until
     * the socket closes or errors. Throws on connect failure — wrap in {@link #reconnect}.
     *
     * @param uri              the WebSocket endpoint
     * @param subscribeMessage message to send on open, or {@code null}
     * @param onMessage        receives each complete text message (on the WebSocket thread)
     * @throws Exception on connect failure or interruption
     */
    public static void readWebSocket(URI uri, String subscribeMessage,
                                     Consumer<String> onMessage) throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        StringBuilder fragments = new StringBuilder();
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                fragments.append(data);
                if (last) {
                    String message = fragments.toString();
                    fragments.setLength(0);
                    onMessage.accept(message);
                }
                webSocket.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                System.err.println("WebSocket closed: " + statusCode + " " + reason);
                closed.countDown();
                return null;
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                System.err.println("WebSocket error: " + error.getMessage());
                closed.countDown();
            }
        };
        WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(uri, listener)
                .join();
        try {
            if (subscribeMessage != null) {
                ws.sendText(subscribeMessage, true);
            }
            closed.await();
        } finally {
            ws.abort();
        }
    }

    /** One stream session; {@link #reconnect} reruns it until {@code running} turns false. */
    @FunctionalInterface
    public interface Session {
        /**
         * Runs one session to completion (stream end, close, or error).
         *
         * @throws Exception when the session fails; {@link #reconnect} logs and retries
         */
        void run() throws Exception;
    }

    /**
     * Endless reconnect loop: run [session], log any failure, wait [delayMs], repeat — until
     * [running] turns false. The Java twin of the Kotlin {@code reconnecting} flow wrapper.
     *
     * @param label   prefix for log lines
     * @param delayMs backoff between sessions
     * @param running loop guard (e.g. flipped false by a shutdown hook)
     * @param session one stream session
     */
    public static void reconnect(String label, long delayMs, BooleanSupplier running,
                                 Session session) {
        while (running.getAsBoolean()) {
            try {
                session.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("[" + label + "] stream error ("
                        + e.getClass().getSimpleName() + "): " + e.getMessage());
            }
            if (!running.getAsBoolean()) {
                return;
            }
            System.err.println("[" + label + "] reconnecting in " + delayMs + "ms...");
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
