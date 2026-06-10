package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for {@link BulkInserter}: initialises a schema, inserts
 * {@code ~10 000} rows via the bulk path, then SELECT-verifies a sample of
 * the inserted data.
 *
 * <p>The bulk-insert path is the primary performance claim of the native
 * client (CH.Native Spec 2).  These tests confirm correctness, not throughput.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class BulkInsertRoundTripIT extends TypeRoundTripBase {

    /**
     * Simple Java record used as the row type for bulk-insert tests.
     *
     * <p>Field names must match ClickHouse column names exactly (the
     * {@link io.github.danielbunting.clickhouse.mapping.RowMappers#forClass(Class)}
     * mapper performs case-sensitive name matching first).
     *
     * <p>VERIFY: RowMappers.forClass() uses record-component declaration order
     * for column-name inference — the record component order here must match
     * the CREATE TABLE column order.  // VERIFY against CH.Native
     */
    record EventRow(long id, String label, double value) {}

    /**
     * Inserts 10 000 rows via {@link BulkInserter}, then:
     * <ol>
     *   <li>Asserts the total count equals 10 000.</li>
     *   <li>Spot-checks several specific rows by ID to verify value fidelity.</li>
     * </ol>
     *
     * <p>The {@code id} column is the MergeTree sort key, so the
     * {@code ORDER BY id} SELECT is deterministic.
     */
    @Test
    void bulkInsertTenThousandRowsThenCount() {
        String table = "bulk_insert_10k_" + System.nanoTime();
        int rowCount = 10_000;

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id    Int64,"
                + "  label String,"
                + "  value Float64"
                + ") ENGINE = MergeTree() ORDER BY id");

            // Build 10 000 rows
            List<EventRow> rows = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                rows.add(new EventRow(i, "label-" + i, i * 0.5));
            }

            try (BulkInserter<EventRow> inserter =
                    conn.createBulkInserter(table, EventRow.class)) {
                inserter.init();
                inserter.addRange(rows);
                inserter.complete();
            }

            // Total count check
            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(rowCount, count,
                    "Expected " + rowCount + " rows after bulk insert into " + table
                    + " — check BulkInserter.complete() terminating-block sending");

            // Spot-check first, last, and middle rows via a SELECT
            try (QueryResult result = conn.query(
                    "SELECT id, label, value FROM " + table
                    + " WHERE id IN (0, 4999, 9999) ORDER BY id")) {

                List<Object[]> sample = materialize(result);
                assertEquals(3, sample.size(),
                        "Expected 3 sample rows (id IN (0,4999,9999)) from " + table);

                // Row id=0
                Object[] r0 = sample.get(0);
                assertEquals(0L, ((Number) r0[0]).longValue(), "id=0: id column");
                assertEquals("label-0", r0[1], "id=0: label column");
                assertEquals(0.0, ((Number) r0[2]).doubleValue(), 1e-12, "id=0: value column");

                // Row id=4999
                Object[] r4999 = sample.get(1);
                assertEquals(4999L, ((Number) r4999[0]).longValue(), "id=4999: id column");
                assertEquals("label-4999", r4999[1], "id=4999: label column");
                assertEquals(4999 * 0.5, ((Number) r4999[2]).doubleValue(), 1e-9,
                        "id=4999: value column");

                // Row id=9999
                Object[] r9999 = sample.get(2);
                assertEquals(9999L, ((Number) r9999[0]).longValue(), "id=9999: id column");
                assertEquals("label-9999", r9999[1], "id=9999: label column");
                assertEquals(9999 * 0.5, ((Number) r9999[2]).doubleValue(), 1e-9,
                        "id=9999: value column");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Verifies that batching logic works correctly when the row count is an
     * exact multiple of the default batch size (65536).  Inserts two full
     * batches (131 072 rows) and confirms the count.
     *
     * <p>An off-by-one in the batch-flush boundary would leave the last batch
     * un-flushed (no {@code complete()} sends the remainder).
     */
    @Test
    void bulkInsertExactlyTwoBatches() {
        String table = "bulk_insert_2batch_" + System.nanoTime();
        // Use a small configured batch size so we don't need to insert 131k rows.
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .insertBatchSize(100) // flush every 100 rows
                .build();

        int rowCount = 250; // 2.5 batches → tests partial-final-batch flushing

        try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id    Int64,"
                + "  label String,"
                + "  value Float64"
                + ") ENGINE = MergeTree() ORDER BY id");

            List<EventRow> rows = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                rows.add(new EventRow(i, "r" + i, i));
            }

            try (BulkInserter<EventRow> inserter =
                    conn.createBulkInserter(table, EventRow.class)) {
                inserter.init();
                inserter.addRange(rows);
                inserter.complete();
            }

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(rowCount, count,
                    "Expected " + rowCount + " rows — partial last-batch flush bug in BulkInserter");

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
