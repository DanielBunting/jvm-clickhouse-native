package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Tour of the write side of the native client — the counterpart of CH.Native's
 * {@code Samples.Insert}.
 *
 * <p>{@link BulkInserter} is the library's biggest performance lever: rows are
 * accumulated into per-column primitive buffers and shipped as native column-major
 * blocks, bypassing any row-by-row SQL path entirely.
 *
 * <p>Steps performed:
 * <ol>
 *   <li>Create the demo table (including a {@code DateTime} column to show
 *       temporal mapping from {@link Instant}).</li>
 *   <li>The canonical lifecycle: {@code init()} → {@code addRange(...)} →
 *       {@code complete()} inside try-with-resources.</li>
 *   <li>A larger batch (250k rows) timed, crossing several internal block
 *       flushes (batch size is {@code ClickHouseConfig.insertBatchSize}).</li>
 *   <li>Row-at-a-time {@code add(...)} for producer-style code.</li>
 *   <li>Verification by count and checksum-style aggregates.</li>
 * </ol>
 *
 * <p>Failure semantics worth knowing: if an insert is abandoned between
 * {@code init()} and {@code complete()} (exception, early close), the connection
 * is marked poisoned rather than left desynced — a pool will discard it instead
 * of recycling it.
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runBulkInserts
 * }</pre>
 */
public final class BulkInserts {

    /**
     * A record whose component names match the demo table's columns. Temporal
     * components map to ClickHouse temporal columns ({@link Instant} → {@code DateTime}).
     *
     * @param id    row identifier
     * @param ts    event timestamp
     * @param label human-readable label
     * @param value arbitrary metric
     */
    public record Reading(long id, Instant ts, String label, double value) {}

    /** The table targeted by this demo. */
    private static final String TABLE = "bulk_demo";

    /** Private constructor — utility-style entry point. */
    private BulkInserts() {}

    /**
     * Entry point. Creates the table and demonstrates the inserter lifecycle.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        System.out.println("=== clickhouse-native-client BulkInserts tour ===");

        try (ClickHouseConnection conn = ClickHouseEnv.open()) {

            // ----------------------------------------------------------------
            // 1. DDL
            // ----------------------------------------------------------------
            System.out.println("\n[1] Creating table '" + TABLE + "'...");
            conn.execute("CREATE OR REPLACE TABLE " + TABLE + " ("
                    + "id    UInt64,"
                    + "ts    DateTime,"
                    + "label String,"
                    + "value Float64"
                    + ") ENGINE = MergeTree ORDER BY id");
            System.out.println("    Done.");

            // ----------------------------------------------------------------
            // 2. The canonical lifecycle on a small batch
            // ----------------------------------------------------------------
            // init()    sends the INSERT and reads the target schema;
            // addRange  buffers rows into per-column primitive arrays;
            // complete  flushes the final block and the empty terminator.
            // try-with-resources guarantees close() even on failure.
            Instant now = Instant.now();
            List<Reading> small = List.of(
                    new Reading(1, now, "boot", 0.1),
                    new Reading(2, now, "warm", 0.5),
                    new Reading(3, now, "steady", 0.9));

            System.out.println("\n[2] Canonical lifecycle: init -> addRange -> complete ("
                    + small.size() + " rows)...");
            try (BulkInserter<Reading> inserter = conn.createBulkInserter(TABLE, Reading.class)) {
                inserter.init();
                inserter.addRange(small);
                inserter.complete();
            }
            System.out.println("    Inserted " + small.size() + " rows.");

            // ----------------------------------------------------------------
            // 3. A real batch, timed
            // ----------------------------------------------------------------
            int n = 250_000;
            System.out.println("\n[3] Bulk-inserting " + n + " rows (crosses multiple "
                    + "block flushes)...");
            List<Reading> batch = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                batch.add(new Reading(1000L + i, now, "batch", i / 1000.0));
            }
            long started = System.nanoTime();
            try (BulkInserter<Reading> inserter = conn.createBulkInserter(TABLE, Reading.class)) {
                inserter.init();
                inserter.addRange(batch);
                inserter.complete();
            }
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            System.out.printf("    %,d rows in %,d ms (%,.0f rows/s)%n",
                    n, elapsedMs, n * 1000.0 / Math.max(elapsedMs, 1));

            // ----------------------------------------------------------------
            // 4. Row-at-a-time add() — producer-style
            // ----------------------------------------------------------------
            // add(row) buffers; the inserter flushes a block automatically each
            // time the configured batch size fills. Same lifecycle around it.
            System.out.println("\n[4] Producer-style add() loop (500 rows)...");
            try (BulkInserter<Reading> inserter = conn.createBulkInserter(TABLE, Reading.class)) {
                inserter.init();
                for (int i = 0; i < 500; i++) {
                    inserter.add(new Reading(900_000L + i, now, "producer", i * 1.0));
                }
                inserter.complete();
            }
            System.out.println("    Done.");

            // ----------------------------------------------------------------
            // 5. Verify
            // ----------------------------------------------------------------
            long count = conn.executeScalar("SELECT count() FROM " + TABLE);
            long distinctLabels = conn.executeScalar(
                    "SELECT uniqExact(label) FROM " + TABLE);
            System.out.println("\n[5] Verification: count=" + count
                    + " (expected " + (small.size() + n + 500) + "), "
                    + "distinct labels=" + distinctLabels + " (expected 5)");

        }

        System.out.println("\n=== BulkInserts tour complete ===");
    }
}
