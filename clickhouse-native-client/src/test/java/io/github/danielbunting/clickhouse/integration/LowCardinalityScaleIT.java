package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import io.github.danielbunting.clickhouse.types.Column;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scale / format-variation integration tests for {@code LowCardinality(T)} that drive the
 * <i>conditional</i> branches of
 * {@link io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec} which the
 * small-cardinality, single-block {@link LowCardinalityIT} never reaches:
 *
 * <ul>
 *   <li><b>Index key width.</b> The dictionary-index integer width is chosen from the dictionary
 *       size: {@code UInt8} (≤ ~256 distinct), {@code UInt16} (≤ ~65536), {@code UInt32} (beyond).
 *       {@link LowCardinalityIT} only exercises {@code UInt8}. Here ~300 distinct forces
 *       {@code UInt16} and ~70000 distinct forces {@code UInt32} on both read and write.</li>
 *   <li><b>Cross-block / global dictionary.</b> The single-block <i>additional-keys</i> form is the
 *       only one covered today; a result that spans multiple native data blocks may use a
 *       shared/updated dictionary. {@link #multiBlockLowCardinalityDecode()} probes it.</li>
 * </ul>
 *
 * <p>Data is generated server-side via {@code numbers(N)} (no giant Java literals). Datasets are
 * kept "just large enough" to span multiple ~65k-row native blocks (200k–300k) to avoid Docker
 * memory pressure under parallel runs. Assertions are spot-checks + total {@code count()} +
 * {@code countDistinct} rather than every row or any internal wire layout.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*LowCardinalityScaleIT"}
 */
@Tag("integration")
class LowCardinalityScaleIT extends TypeRoundTripBase {

    /** Row mirroring {@code (id UInt64, lc LowCardinality(String))}; field names match columns. */
    record LcRow(long id, String lc) {}

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    /** First column of every materialised row, as a (possibly null) String. */
    private List<String> stringColumn(List<Object[]> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (Object[] r : rows) {
            out.add(r[0] == null ? null : r[0].toString());
        }
        return out;
    }

    /** Single scalar value from a one-row, one-column query. */
    private long scalarLong(ClickHouseConnection conn, String sql) {
        List<Object[]> rows = decode(conn, sql);
        assertEquals(1, rows.size(), "expected a single scalar row from: " + sql);
        return ((Number) rows.get(0)[0]).longValue();
    }

    // -------------------------------------------------------------------------
    // Test: UInt16 key width (decode)
    // -------------------------------------------------------------------------

    /**
     * DECODE, {@code UInt16} index keys. 300 distinct string values over 100k rows pushes the
     * dictionary past 256 entries, so the server picks {@code UInt16} index keys. The codec must
     * read that width correctly. Spot-checks at the boundaries + full count + countDistinct.
     */
    @Test
    void uint16KeyWidthDecodeRoundTrips() {
        withTable("lc_u16_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(String))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table
                    + " SELECT number, toString(number % 300) FROM numbers(100000)");

            assertEquals(100000L, scalarLong(conn, "SELECT count() FROM " + table),
                    "all 100k rows must be present");
            assertEquals(300L, scalarLong(conn, "SELECT uniqExact(lc) FROM " + table),
                    "exactly 300 distinct LowCardinality values (forces UInt16 index keys)");

            // Spot-check a representative set of ids: boundaries + a scattered interior sample.
            long[] ids = {0, 1, 299, 300, 12345, 65535, 65536, 99998, 99999};
            for (long id : ids) {
                List<Object[]> rows = decode(conn,
                        "SELECT lc FROM " + table + " WHERE id = " + id);
                assertEquals(1, rows.size(), "exactly one row for id=" + id);
                assertEquals(String.valueOf(id % 300), stringColumn(rows).get(0),
                        "UInt16-keyed LowCardinality(String) value for id=" + id);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Test: UInt32 key width (decode) — most likely to break
    // -------------------------------------------------------------------------

    /**
     * DECODE, {@code UInt32} index keys. {@code number % 70000} over 200k rows yields 70000 distinct
     * values — beyond the 65536 {@code UInt16} ceiling — forcing the server to emit {@code UInt32}
     * index keys. This is the read path the spec flags as most likely to break (wrong width / cliff).
     */
    @Test
    void uint32KeyWidthDecodeRoundTrips() {
        withTable("lc_u32w_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(String))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table
                    + " SELECT number, toString(number % 70000) FROM numbers(200000)");

            assertEquals(200000L, scalarLong(conn, "SELECT count() FROM " + table),
                    "all 200k rows must be present");
            assertEquals(70000L, scalarLong(conn, "SELECT uniqExact(lc) FROM " + table),
                    "exactly 70000 distinct values (>65536 → forces UInt32 index keys)");

            // Spot-checks straddling the UInt16 ceiling so a wrong-width decode would diverge.
            long[] ids = {0, 65535, 65536, 65537, 69999, 70000, 199998, 199999};
            for (long id : ids) {
                List<Object[]> rows = decode(conn,
                        "SELECT lc FROM " + table + " WHERE id = " + id);
                assertEquals(1, rows.size(), "exactly one row for id=" + id);
                assertEquals(String.valueOf(id % 70000), stringColumn(rows).get(0),
                        "UInt32-keyed LowCardinality(String) value for id=" + id);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Test: multi-block / cross-block (global) dictionary (decode)
    // -------------------------------------------------------------------------

    /**
     * DECODE across multiple native data blocks. A 300k-row {@code numbers()} stream spans several
     * ~65k-row server blocks. The codec was built for the single-block additional-keys form and
     * rejects {@code NeedGlobalDictionary}; if the server uses a shared/updated dictionary across
     * blocks this either throws or mis-maps. Asserts >1 non-empty block, correct values per block,
     * and the total row count.
     */
    @Test
    void multiBlockLowCardinalityDecode() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            long total = 0;
            int nonEmptyBlocks = 0;
            try (QueryResult result = conn.query(
                    "SELECT number AS n, toString(number % 500) AS lc FROM numbers(300000)")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    nonEmptyBlocks++;
                    Column nCol = block.column(0);
                    Column lcCol = block.column(1);
                    int rowCount = block.rowCount();
                    for (int r = 0; r < rowCount; r++) {
                        long n = ((Number) nCol.value(r)).longValue();
                        Object lc = lcCol.value(r);
                        assertEquals(String.valueOf(n % 500), lc == null ? null : lc.toString(),
                                "row n=" + n + " LowCardinality value across blocks");
                    }
                    total += rowCount;
                }
            }
            assertTrue(nonEmptyBlocks > 1,
                    "300k rows must span >1 non-empty native block (saw " + nonEmptyBlocks + ")");
            assertEquals(300000L, total, "all 300k rows must decode across blocks");
        }
    }

    // -------------------------------------------------------------------------
    // Test: high-cardinality encode (UInt16 key width) + multi-batch framing
    // -------------------------------------------------------------------------

    /**
     * ENCODE choosing {@code UInt16} key width. Bulk-insert 100k rows drawing on 300 distinct
     * strings; the client must build the dictionary and pick {@code UInt16} index keys. Read back
     * spot-checks + count + countDistinct.
     */
    @Test
    void highCardinalityEncodeRoundTrips() {
        withTable("lc_u16_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt64, lc LowCardinality(String))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<LcRow> input = new ArrayList<>(100000);
            for (long i = 0; i < 100000; i++) {
                input.add(new LcRow(i, String.valueOf(i % 300)));
            }
            try (BulkInserter<LcRow> inserter = conn.createBulkInserter(table, LcRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertEquals(100000L, scalarLong(conn, "SELECT count() FROM " + table),
                    "all 100k bulk-inserted rows must be present");
            assertEquals(300L, scalarLong(conn, "SELECT uniqExact(lc) FROM " + table),
                    "encode must build a 300-entry dictionary (UInt16 index keys)");

            long[] ids = {0, 1, 299, 300, 50000, 99998, 99999};
            for (long id : ids) {
                List<Object[]> rows = decode(conn,
                        "SELECT lc FROM " + table + " WHERE id = " + id);
                assertEquals(1, rows.size(), "exactly one row for id=" + id);
                assertEquals(String.valueOf(id % 300), stringColumn(rows).get(0),
                        "encoded UInt16-keyed LowCardinality(String) value for id=" + id);
            }
        });
    }

    /**
     * ENCODE across many flush boundaries. A small {@code insertBatchSize} (1000) flushes the 100k
     * rows as ~100 separate blocks, exercising the per-block dictionary/version framing on write.
     * Each block independently re-encodes a 300-entry dictionary; read back asserts the values
     * survived the multi-block write.
     */
    @Test
    void multiBatchEncodeRoundTrips() {
        String table = "lc_multibatch_encode_" + System.nanoTime();
        ClickHouseConfig batched = ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .insertBatchSize(1000)
                .build();
        try (ClickHouseConnection conn = ClickHouseConnection.open(batched)) {
            try {
                conn.execute("CREATE TABLE " + table + " (id UInt64, lc LowCardinality(String))"
                        + " ENGINE = MergeTree() ORDER BY id");

                List<LcRow> input = new ArrayList<>(100000);
                for (long i = 0; i < 100000; i++) {
                    input.add(new LcRow(i, String.valueOf(i % 300)));
                }
                try (BulkInserter<LcRow> inserter = conn.createBulkInserter(table, LcRow.class)) {
                    inserter.init();
                    inserter.addRange(input);
                    inserter.complete();
                }

                assertEquals(100000L, scalarLong(conn, "SELECT count() FROM " + table),
                        "all 100k rows flushed across ~100 batches must be present");
                assertEquals(300L, scalarLong(conn, "SELECT uniqExact(lc) FROM " + table),
                        "multi-batch encode must preserve the 300 distinct values");

                long[] ids = {0, 999, 1000, 1001, 50000, 99999};
                for (long id : ids) {
                    List<Object[]> rows = decode(conn,
                            "SELECT lc FROM " + table + " WHERE id = " + id);
                    assertEquals(1, rows.size(), "exactly one row for id=" + id);
                    assertEquals(String.valueOf(id % 300), stringColumn(rows).get(0),
                            "multi-batch encoded value for id=" + id + " (batch-boundary check)");
                }
            } finally {
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test: Nullable LowCardinality at scale
    // -------------------------------------------------------------------------

    /**
     * DECODE {@code LowCardinality(Nullable(String))} at scale. ~300-valued domain over 100k rows
     * with ~10% NULLs scattered ({@code number % 10 = 0}). NULL is dictionary slot 0; this checks
     * the slot-0 convention holds under a >256-entry dictionary (UInt16 keys) and that NULLs land
     * in exactly the right rows.
     */
    @Test
    void nullableLowCardinalityAtScale() {
        withTable("lc_nstr_scale", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, lc LowCardinality(Nullable(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " SELECT number,"
                    + " if(number % 10 = 0, NULL, toString(number % 300)) FROM numbers(100000)");

            assertEquals(100000L, scalarLong(conn, "SELECT count() FROM " + table),
                    "all 100k rows present");
            assertEquals(10000L,
                    scalarLong(conn, "SELECT countIf(lc IS NULL) FROM " + table),
                    "exactly 10% of rows (every 10th) must be NULL");
            // Non-null distinct count: a row's value is (number % 300), but only rows where
            // number % 10 != 0 are non-null. The values that are multiples of 10 (0,10,...,290 →
            // 30 of them) can only be produced when number % 10 == 0 — i.e. exactly the NULL rows —
            // so they never appear as non-null. Hence 300 - 30 = 270 distinct non-null values, which
            // (plus the NULL slot) keeps the dictionary above the 256-entry UInt16 threshold.
            assertEquals(270L, scalarLong(conn,
                    "SELECT uniqExact(lc) FROM " + table + " WHERE lc IS NOT NULL"),
                    "270 distinct non-null values (>256 → UInt16 keys; multiples of 10 only on NULL rows)");

            // Spot-check: every 10th id is NULL, others equal number % 300.
            long[] ids = {0, 1, 9, 10, 11, 299, 300, 50000, 50001, 99998, 99999};
            for (long id : ids) {
                List<Object[]> rows = decode(conn,
                        "SELECT lc FROM " + table + " WHERE id = " + id);
                assertEquals(1, rows.size(), "exactly one row for id=" + id);
                String actual = stringColumn(rows).get(0);
                if (id % 10 == 0) {
                    assertNull(actual, "row id=" + id + " must decode as NULL");
                } else {
                    assertEquals(String.valueOf(id % 300), actual,
                            "non-null Nullable LowCardinality value for id=" + id);
                }
            }
        });
    }
}
