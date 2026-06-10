package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips the ClickHouse {@code Map(K, V)} codec against a real server in both
 * directions: DECODE (raw {@code INSERT ... VALUES (map(...))}) and ENCODE (bulk insert
 * of a mapped record whose field is a {@code java.util.Map}).
 *
 * <p>On the wire {@code Map(K, V)} is identical to {@code Array(Tuple(K, V))}: cumulative
 * {@code UInt64} offsets then a flattened {@code Tuple(K, V)} entries column. A Map cell
 * decodes to a {@link LinkedHashMap} preserving insertion (wire) order. Numeric values
 * are asserted via {@link Number} so the test is independent of the inner box type.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*MapTypesIT"}
 */
@Tag("integration")
class MapTypesIT extends TypeRoundTripBase {

    /** Asserts a Map cell equals expected keys -> numeric (long) values, order-independent. */
    private void assertLongValueMap(Object cell, Map<?, Long> expected, String label) {
        Map<?, ?> map = assertInstanceOf(Map.class, cell,
                label + ": expected a java.util.Map (Map codec) but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(expected.size(), map.size(), label + ": map size mismatch");
        for (Map.Entry<?, Long> e : expected.entrySet()) {
            Object actual = map.get(e.getKey());
            assertInstanceOf(Number.class, actual, label + " key " + e.getKey() + ": missing/not numeric");
            assertEquals(e.getValue().longValue(), ((Number) actual).longValue(),
                    label + " key " + e.getKey());
        }
    }

    /**
     * DECODE: {@code Map(String, UInt32)} via raw VALUES, including an empty map. Each
     * cell must decode to a LinkedHashMap with the expected entries.
     */
    @Test
    void mapStringUInt32DecodeRoundTrips() {
        withTable("map_str_u32", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, m Map(String, UInt32)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, m) VALUES"
                    + " (1, map('a', 10, 'b', 20)), (2, map())");

            List<Object[]> rows = decode(conn,
                    "SELECT id, m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            Map<String, Long> expected = new LinkedHashMap<>();
            expected.put("a", 10L);
            expected.put("b", 20L);
            assertLongValueMap(rows.get(0)[1], expected, "Row 1 Map(String,UInt32)");

            Map<?, ?> empty = assertInstanceOf(Map.class, rows.get(1)[1], "Row 2 empty map");
            assertTrue(empty.isEmpty(), "Row 2 Map(String,UInt32): expected empty map");
        });
    }

    /**
     * DECODE: {@code Map(String, String)} — variable-width keys and values, plus an
     * empty map.
     */
    @Test
    void mapStringStringDecodeRoundTrips() {
        withTable("map_str_str", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, m Map(String, String)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, m) VALUES"
                    + " (1, map('k1', 'v1', 'k2', 'unicode: éàü')), (2, map())");

            List<Object[]> rows = decode(conn,
                    "SELECT id, m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            Map<?, ?> m0 = assertInstanceOf(Map.class, rows.get(0)[1], "Row 1 Map(String,String)");
            assertEquals(2, m0.size(), "Row 1 size");
            assertEquals("v1", m0.get("k1"), "Row 1 k1");
            assertEquals("unicode: éàü", m0.get("k2"), "Row 1 k2 UTF-8");

            Map<?, ?> empty = assertInstanceOf(Map.class, rows.get(1)[1], "Row 2 empty map");
            assertTrue(empty.isEmpty(), "Row 2 Map(String,String): expected empty map");
        });
    }

    /**
     * DECODE: {@code Map(Int64, Float64)} — numeric keys and values, exercises mixed
     * fixed-width key/value sub-columns.
     */
    @Test
    void mapInt64Float64DecodeRoundTrips() {
        withTable("map_i64_f64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, m Map(Int64, Float64)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, m) VALUES"
                    + " (1, map(-5, 1.5, 9223372036854775807, 2.5)), (2, map())");

            List<Object[]> rows = decode(conn,
                    "SELECT id, m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            Map<?, ?> m0 = assertInstanceOf(Map.class, rows.get(0)[1], "Row 1 Map(Int64,Float64)");
            assertEquals(2, m0.size(), "Row 1 size");
            assertEquals(1.5, ((Number) m0.get(-5L)).doubleValue(), 0.0, "Row 1 key -5");
            assertEquals(2.5, ((Number) m0.get(Long.MAX_VALUE)).doubleValue(), 0.0,
                    "Row 1 key Long.MAX_VALUE");

            Map<?, ?> empty = assertInstanceOf(Map.class, rows.get(1)[1], "Row 2 empty map");
            assertTrue(empty.isEmpty(), "Row 2 Map(Int64,Float64): expected empty map");
        });
    }

    /**
     * A record whose {@code m} field is a {@code Map(String, UInt32)} payload, used to
     * exercise the ENCODE path: the mapper routes the Map through {@code codec.set}.
     *
     * @param id the row id (UInt32)
     * @param m  the map payload ({@code Map(String, UInt32)})
     */
    record MapRow(long id, Map<String, Long> m) {}

    /**
     * ENCODE: bulk-insert {@code Map(String, UInt32)} rows built as Java maps (including
     * an empty map), then read them back and assert the map entries round-trip. Confirms
     * the Map encode (offsets + flattened Tuple(K, V) entries) matches the server.
     */
    @Test
    void mapStringUInt32BulkInsertRoundTrips() {
        withTable("map_str_u32_bulk", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, m Map(String, UInt32)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            Map<String, Long> m1 = new LinkedHashMap<>();
            m1.put("x", 1L);
            m1.put("y", 4_294_967_295L);
            List<MapRow> input = List.of(
                    new MapRow(1, m1),
                    new MapRow(2, new LinkedHashMap<>()),
                    new MapRow(3, Map.of("only", 42L)));

            try (BulkInserter<MapRow> inserter =
                    conn.createBulkInserter(table, MapRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT id, m FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk-inserted rows");

            Map<String, Long> exp1 = new LinkedHashMap<>();
            exp1.put("x", 1L);
            exp1.put("y", 4_294_967_295L);
            assertLongValueMap(rows.get(0)[1], exp1, "Bulk row 1");

            Map<?, ?> empty = assertInstanceOf(Map.class, rows.get(1)[1], "Bulk row 2 empty map");
            assertTrue(empty.isEmpty(), "Bulk row 2: expected empty map");

            assertLongValueMap(rows.get(2)[1], Map.of("only", 42L), "Bulk row 3");
        });
    }
}
