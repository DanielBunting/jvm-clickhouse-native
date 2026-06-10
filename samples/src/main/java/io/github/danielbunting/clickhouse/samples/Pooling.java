package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Concurrency with the connection pool — the JVM answer to CH.Native's
 * {@code Samples.Hosting} (app-lifetime integration).
 *
 * <p>A single {@code ClickHouseConnection} is one socket and a stateful protocol:
 * it must be used by one thread at a time (concurrent use fails fast with
 * {@code ConcurrentConnectionUseException} rather than corrupting the stream).
 * The unit of concurrency is therefore the {@link ClickHouseConnectionPool}:
 * {@code N} independent connections, one borrower each.
 *
 * <p>Steps performed:
 * <ol>
 *   <li>Create a pool of 4 connections (eagerly opened — fails fast if the
 *       server is unreachable).</li>
 *   <li>Run 16 tasks on 8 threads, each borrowing via {@code withConnection}
 *       (borrow → work → return, leak-proof).</li>
 *   <li>Show pool health: size vs. available before/during/after.</li>
 * </ol>
 *
 * <p>Hygiene the pool gives you for free: returned connections that were
 * poisoned (broken socket, abandoned mid-INSERT) are discarded, not recycled;
 * {@code validateOnBorrow} (default on) additionally health-checks idle
 * connections with {@code SELECT 1}; and discarded slots self-heal — the next
 * borrow opens a fresh connection.
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runPooling
 * }</pre>
 */
public final class Pooling {

    /** The table targeted by this demo. */
    private static final String TABLE = "pool_demo";

    /** Pool capacity used by the demo. */
    private static final int POOL_SIZE = 4;

    /** Number of concurrent tasks thrown at the pool. */
    private static final int TASKS = 16;

    /** Private constructor — utility-style entry point. */
    private Pooling() {}

    /**
     * Entry point. Builds the pool and runs concurrent inserts and reads through it.
     *
     * @param args ignored
     * @throws Exception if a worker task fails
     */
    public static void main(String[] args) throws Exception {
        System.out.println("=== clickhouse-native-client Pooling tour ===");

        // --------------------------------------------------------------------
        // 1. Pool construction: 4 eagerly-opened, independent connections
        // --------------------------------------------------------------------
        System.out.println("\n[1] Opening a pool of " + POOL_SIZE + " connections...");
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.create(ClickHouseEnv.config(), POOL_SIZE)) {
            System.out.println("    size=" + pool.size() + ", available=" + pool.available());

            pool.useConnection(conn -> conn.execute(
                    "CREATE OR REPLACE TABLE " + TABLE + " ("
                            + "task UInt32,"
                            + "n    UInt64"
                            + ") ENGINE = MergeTree ORDER BY (task, n)"));

            // ----------------------------------------------------------------
            // 2. 16 tasks on 8 threads, 4 connections: borrows queue fairly
            // ----------------------------------------------------------------
            System.out.println("\n[2] Running " + TASKS + " concurrent tasks on 8 threads...");
            ExecutorService executor = Executors.newFixedThreadPool(8);
            try {
                List<Future<Long>> futures = new ArrayList<>(TASKS);
                for (int t = 0; t < TASKS; t++) {
                    final int task = t;
                    futures.add(executor.submit(() ->
                            // withConnection = borrow -> work -> always return.
                            // Each lambda runs on its own borrowed connection;
                            // no two tasks ever share a socket.
                            pool.withConnection(conn -> {
                                conn.execute("INSERT INTO " + TABLE
                                        + " SELECT " + task + ", number FROM numbers(10000)");
                                return conn.executeScalar(
                                        "SELECT count() FROM " + TABLE
                                                + " WHERE task = " + task);
                            })));
                }
                long totalSeen = 0;
                for (Future<Long> f : futures) {
                    totalSeen += f.get();
                }
                System.out.println("    All tasks done; per-task counts sum to " + totalSeen);
            } finally {
                executor.shutdown();
            }

            // ----------------------------------------------------------------
            // 3. Pool state after the burst
            // ----------------------------------------------------------------
            long rows = pool.withConnection(conn ->
                    conn.executeScalar("SELECT count() FROM " + TABLE));
            System.out.println("\n[3] After the burst: available=" + pool.available()
                    + "/" + pool.size() + ", total rows=" + rows
                    + " (expected " + (TASKS * 10_000L) + ")");
        }

        System.out.println("\n=== Pooling tour complete ===");
    }
}
