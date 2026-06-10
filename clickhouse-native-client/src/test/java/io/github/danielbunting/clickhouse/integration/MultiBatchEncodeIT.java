package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-batch bulk-ENCODE round-trips for the complex / self-framing codecs.
 *
 * <p>{@link BulkInserter} flushes a Data block every {@code insertBatchSize} rows (and a final
 * partial block on {@code complete()}). The self-framing codecs write <b>per-block state</b> —
 * LowCardinality a {@code KeysSerializationVersion} + a fresh per-block dictionary + keys;
 * Variant/Dynamic a version + (Dynamic) per-block type list + discriminators; Map/Tuple offsets and
 * flattened sub-columns; Array(Nullable) offsets + an inner null-map. The existing per-type encode
 * ITs all insert a handful of rows = a <b>single</b> block, so the "re-emit the per-block prefix
 * correctly on block 2, 3, ..." path is untested.
 *
 * <p>Each test here forces {@code >= 3} flushed blocks by configuring {@code insertBatchSize(100)}
 * and inserting 250 rows, then SELECTs back ordered by {@code id} and asserts the full count plus
 * spot-checks rows at and beyond every batch boundary (ids 0, 99, 100, 199, 200, 249). A per-block
 * encode-state bug classically leaves block 1 (ids 0..99) intact while corrupting blocks 2+
 * (ids >= 100), so the post-first-batch rows are checked explicitly.
 *
 * <p>Per the spec, assertions encode CORRECT behavior. If a type's multi-block encode corrupts rows
 * after the first flush, that test is marked {@code @Disabled("KNOWN BUG: ...")} rather than
 * weakened — surfacing such a bug is the goal of this spec.
 *
 * <p>Run with:
 * {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*MultiBatchEncodeIT" --rerun-tasks}
 */
@Tag("integration")
class MultiBatchEncodeIT extends TypeRoundTripBase {

    /** Rows per type: 250 with a batch size of 100 forces 3 blocks (100, 100, 50). */
    private static final int ROWS = 250;

    /** Configured batch size; small so modest row counts still flush multiple blocks. */
    private static final int BATCH = 100;

    /** Batch boundaries to spot-check: first/last of block 1, first/last of block 2, last overall. */
    private static final int[] BOUNDARY_IDS = {0, 99, 100, 199, 200, 249};

    /**
     * Opens a connection whose config uses the small batch size, runs {@code setup} (typically the
     * experimental {@code SET} flags) on it before anything else, hands the connection and a unique
     * table name to {@code body}, and always drops the table afterwards.
     *
     * <p>The {@code SET} flags must be issued on the same connection the {@link BulkInserter} uses,
     * before {@code init()} reads the 0-row sample block — so {@code setup} runs first.
     */
    private void withBatchTable(String prefix,
                                BiConsumer<ClickHouseConnection, String> setup,
                                BiConsumer<ClickHouseConnection, String> body) {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .insertBatchSize(BATCH)
                .build();
        String table = prefix + "_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
            try {
                setup.accept(conn, table);
                body.accept(conn, table);
            } finally {
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    private void withBatchTable(String prefix, BiConsumer<ClickHouseConnection, String> body) {
        withBatchTable(prefix, (conn, table) -> { }, body);
    }

    private void assertCount(ClickHouseConnection conn, String table) {
        long count = conn.executeScalar("SELECT count() FROM " + table);
        assertEquals(ROWS, count,
                "Expected " + ROWS + " rows across " + ((ROWS + BATCH - 1) / BATCH)
                        + " flushed blocks in " + table
                        + " — a partial-last-block or per-block-state flush bug drops/corrupts rows");
    }

    // -------------------------------------------------------------------------
    // LowCardinality(String) — per-block dictionary is the most stateful encode
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, lc LowCardinality(String))}. */
    record LcRow(long id, String lc) {}

    /** Deterministic value drawn from ~50 distinct strings so each block builds its own dictionary. */
    private static String lcValue(int i) {
        return "v" + (i % 50);
    }

    @Test
    void lowCardinalityStringMultiBatch() {
        withBatchTable("mb_lc_str", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(String))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<LcRow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new LcRow(i, lcValue(i)));
            }
            try (BulkInserter<LcRow> inserter = conn.createBulkInserter(table, LcRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            // Every row, in id order, must equal its source value — a stale/missing per-block
            // dictionary would corrupt blocks 2+ (ids >= 100) while block 1 looks fine.
            for (int i = 0; i < ROWS; i++) {
                assertEquals(lcValue(i), rows.get(i)[0],
                        "LowCardinality(String) id=" + i + " (block " + (i / BATCH) + ")");
            }
        });
    }

    // -------------------------------------------------------------------------
    // Tuple(UInt32, String)
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, t Tuple(UInt32, String))}. */
    record TRow(long id, List<Object> t) {}

    @Test
    void tupleUInt32StringMultiBatch() {
        withBatchTable("mb_tuple", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, t Tuple(UInt32, String))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<TRow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new TRow(i, Arrays.asList((long) i, "s" + i)));
            }
            try (BulkInserter<TRow> inserter = conn.createBulkInserter(table, TRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            for (int i = 0; i < ROWS; i++) {
                List<?> t = assertInstanceOf(List.class, rows.get(i)[0],
                        "Tuple id=" + i + " (block " + (i / BATCH) + ")");
                assertEquals(2, t.size(), "Tuple arity id=" + i);
                assertEquals((long) i, ((Number) t.get(0)).longValue(), "Tuple elem0 id=" + i);
                assertEquals("s" + i, t.get(1), "Tuple elem1 id=" + i);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Map(String, UInt32) — include empty maps across blocks (offset reset per block)
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, m Map(String, UInt32))}. */
    record MRow(long id, Map<String, Long> m) {}

    /** Every 7th row gets an empty map; otherwise a 2-entry map keyed by id. */
    private static Map<String, Long> mapValue(int i) {
        if (i % 7 == 0) {
            return new LinkedHashMap<>();
        }
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("k" + i, (long) i);
        m.put("k" + (i + 1), (long) (i + 1));
        return m;
    }

    @Test
    void mapStringUInt32MultiBatch() {
        withBatchTable("mb_map", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, m Map(String, UInt32))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<MRow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new MRow(i, mapValue(i)));
            }
            try (BulkInserter<MRow> inserter = conn.createBulkInserter(table, MRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT m FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            for (int i = 0; i < ROWS; i++) {
                Map<?, ?> got = assertInstanceOf(Map.class, rows.get(i)[0],
                        "Map id=" + i + " (block " + (i / BATCH) + ")");
                Map<String, Long> exp = mapValue(i);
                assertEquals(exp.size(), got.size(), "Map size id=" + i);
                for (Map.Entry<String, Long> e : exp.entrySet()) {
                    assertEquals(e.getValue().longValue(),
                            ((Number) got.get(e.getKey())).longValue(),
                            "Map id=" + i + " key " + e.getKey());
                }
            }
        });
    }

    // -------------------------------------------------------------------------
    // Variant(UInt32, String) — mixed int/string/NULL across batches
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, v Variant(UInt32, String))}. */
    record VRow(long id, Object v) {}

    /** Cycles Long / String / NULL so all three discriminators appear in every block. */
    private static Object variantValue(int i) {
        switch (i % 3) {
            case 0: return (long) i;
            case 1: return "str" + i;
            default: return null;
        }
    }

    @Test
    void variantUInt32StringMultiBatch() {
        withBatchTable("mb_variant",
                (conn, table) -> conn.execute("SET allow_experimental_variant_type = 1"),
                (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, v Variant(UInt32, String))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<VRow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new VRow(i, variantValue(i)));
            }
            try (BulkInserter<VRow> inserter = conn.createBulkInserter(table, VRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            for (int i = 0; i < ROWS; i++) {
                Object got = rows.get(i)[0];
                Object exp = variantValue(i);
                String where = "Variant id=" + i + " (block " + (i / BATCH) + ")";
                if (exp == null) {
                    assertNull(got, where + " expected NULL");
                } else if (exp instanceof Long l) {
                    assertInstanceOf(Number.class, got, where + " expected Number");
                    assertEquals(l.longValue(), ((Number) got).longValue(), where);
                } else {
                    assertEquals(exp, got, where);
                }
            }
        });
    }

    // -------------------------------------------------------------------------
    // Dynamic — mixed Int64/String across batches
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, d Dynamic)}. */
    record DRow(long id, Object d) {}

    /** Cycles Long / String / NULL across the blocks. */
    private static Object dynamicValue(int i) {
        switch (i % 3) {
            case 0: return (long) i;
            case 1: return "dyn" + i;
            default: return null;
        }
    }

    @Test
    void dynamicMultiBatch() {
        withBatchTable("mb_dynamic",
                (conn, table) -> conn.execute("SET allow_experimental_dynamic_type = 1"),
                (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, d Dynamic)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<DRow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new DRow(i, dynamicValue(i)));
            }
            try (BulkInserter<DRow> inserter = conn.createBulkInserter(table, DRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            for (int i = 0; i < ROWS; i++) {
                Object got = rows.get(i)[0];
                Object exp = dynamicValue(i);
                String where = "Dynamic id=" + i + " (block " + (i / BATCH) + ")";
                if (exp == null) {
                    assertNull(got, where + " expected NULL");
                } else if (exp instanceof Long l) {
                    assertInstanceOf(Number.class, got, where + " expected Number");
                    assertEquals(l.longValue(), ((Number) got).longValue(), where);
                } else {
                    assertEquals(exp, got, where);
                }
            }
        });
    }

    // -------------------------------------------------------------------------
    // JSON — flat scalar objects across batches
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, j JSON)} carrying a flat JSON object string. */
    record JRow(long id, String j) {}

    @Test
    void jsonFlatObjectMultiBatch() {
        withBatchTable("mb_json",
                (conn, table) -> conn.execute("SET allow_experimental_json_type = 1"),
                (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, j JSON)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<JRow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new JRow(i, "{\"a\":" + i + ",\"b\":\"x" + i + "\"}"));
            }
            try (BulkInserter<JRow> inserter = conn.createBulkInserter(table, JRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT j FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            for (int i = 0; i < ROWS; i++) {
                String s = (String) rows.get(i)[0];
                String where = "JSON id=" + i + " (block " + (i / BATCH) + ")";
                assertTrue(s.contains("\"a\":" + i), where + " must contain a:" + i + ", was " + s);
                assertTrue(s.contains("\"b\":\"x" + i + "\""),
                        where + " must contain b:\"x" + i + "\", was " + s);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Array(Nullable(Int64)) — offsets + inner null-map across blocks
    // -------------------------------------------------------------------------

    /** Row mirroring {@code (id UInt32, arr Array(Nullable(Int64)))}. */
    record ARow(long id, List<Long> arr) {}

    /** Varies array length and null placement; every 5th row is an empty array. */
    private static List<Long> arrayValue(int i) {
        if (i % 5 == 0) {
            return List.of();
        }
        return Arrays.asList((long) i, null, (long) (i + 1));
    }

    @Test
    void arrayNullableInt64MultiBatch() {
        withBatchTable("mb_arr_nint64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, arr Array(Nullable(Int64)))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<ARow> input = new ArrayList<>(ROWS);
            for (int i = 0; i < ROWS; i++) {
                input.add(new ARow(i, arrayValue(i)));
            }
            try (BulkInserter<ARow> inserter = conn.createBulkInserter(table, ARow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            assertCount(conn, table);

            List<Object[]> rows = decode(conn, "SELECT arr FROM " + table + " ORDER BY id");
            assertEquals(ROWS, rows.size(), "row count from SELECT");
            for (int i = 0; i < ROWS; i++) {
                List<?> got = assertInstanceOf(List.class, rows.get(i)[0],
                        "Array id=" + i + " (block " + (i / BATCH) + ")");
                List<Long> exp = arrayValue(i);
                assertEquals(exp.size(), got.size(),
                        "Array length id=" + i + " — offsets reset per block?");
                for (int e = 0; e < exp.size(); e++) {
                    Long ev = exp.get(e);
                    Object av = got.get(e);
                    if (ev == null) {
                        assertNull(av, "Array id=" + i + " elem " + e + " expected NULL");
                    } else {
                        assertEquals(ev.longValue(), ((Number) av).longValue(),
                                "Array id=" + i + " elem " + e);
                    }
                }
            }
        });
    }
}
