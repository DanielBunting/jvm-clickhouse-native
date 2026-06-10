package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Composite type-nesting matrix integration tests.
 *
 * <p>Single-type tests exercise each codec in isolation; this class drives the
 * <em>composition</em> of {@code Array} / {@code Map} / {@code Tuple} /
 * {@code Nullable} / {@code LowCardinality} — the places where offsets,
 * null-maps, heterogeneous tuple sub-columns and dictionaries interleave and
 * tend to break. Each combination lives in its own test method so a failure
 * pinpoints the exact nesting.
 *
 * <p>Two directions are exercised:
 * <ul>
 *   <li><b>DECODE</b> — raw {@code INSERT ... VALUES} using {@code map(...)},
 *       {@code [...]} and {@code (...)} literals (server encodes); the client
 *       decodes and we assert the Java structure element-by-element.</li>
 *   <li><b>ENCODE</b> — for the bindable shapes (List / Map fields) bulk-insert
 *       a mapped record, then read back via the same decode path.</li>
 * </ul>
 *
 * <p>Decoded shapes: {@code Array}/{@code Tuple} → {@link List}; {@code Map} →
 * {@link LinkedHashMap}; {@code Nullable} null cells → {@code null}. Numbers are
 * asserted via {@link Number#longValue()} so the test is independent of the
 * inner box type.
 *
 * <p>Run with:
 * {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*CompositeNestingIT"}
 */
@Tag("integration")
class CompositeNestingIT extends TypeRoundTripBase {

    // ------------------------------------------------------------------ helpers

    /** Casts a cell to a List, failing with a codec-aware message if it is not. */
    private List<?> asList(Object cell, String label) {
        return assertInstanceOf(List.class, cell,
                label + ": expected a java.util.List but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
    }

    /** Casts a cell to a Map, failing with a codec-aware message if it is not. */
    private Map<?, ?> asMap(Object cell, String label) {
        return assertInstanceOf(Map.class, cell,
                label + ": expected a java.util.Map but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
    }

    /** Asserts a List of nullable longs (null entries compared by identity). */
    private void assertNullableLongList(Object cell, Long[] expected, String label) {
        List<?> list = asList(cell, label);
        assertEquals(expected.length, list.size(), label + ": length mismatch");
        for (int i = 0; i < expected.length; i++) {
            Object e = list.get(i);
            if (expected[i] == null) {
                assertNull(e, label + "[" + i + "]: expected null element");
            } else {
                assertEquals(expected[i].longValue(), ((Number) e).longValue(),
                        label + "[" + i + "]: element value mismatch");
            }
        }
    }

    /** Asserts a List of (nullable) Strings element-by-element. */
    private void assertStringList(Object cell, List<String> expected, String label) {
        List<?> list = asList(cell, label);
        assertEquals(expected, list, label + ": string list mismatch");
    }

    // ====================================================================
    // 1. Array(Nullable(Int64)) — null-map inside array offsets
    // ====================================================================

    @Test
    void arrayNullableInt64DecodeRoundTrips() {
        withTable("nest_arr_nint64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(Nullable(Int64)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, a) VALUES"
                    + " (1, [1, NULL, 3]),"     // mixed
                    + " (2, []),"               // empty container
                    + " (3, [NULL]),"           // all-null
                    + " (4, [9223372036854775807, -9223372036854775808])");

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size(), "Expected 4 rows");
            assertNullableLongList(rows.get(0)[0], new Long[] {1L, null, 3L}, "Row 1 mixed");
            assertNullableLongList(rows.get(1)[0], new Long[] {}, "Row 2 empty");
            assertNullableLongList(rows.get(2)[0], new Long[] {null}, "Row 3 all-null");
            assertNullableLongList(rows.get(3)[0],
                    new Long[] {Long.MAX_VALUE, Long.MIN_VALUE}, "Row 4 extremes");
        });
    }

    /** Record for Array(Nullable(Int64)) encode; null elements allowed in the list. */
    record ArrNullableInt64Row(long id, List<Long> a) {}

    @Test
    void arrayNullableInt64EncodeRoundTrips() {
        withTable("nest_arr_nint64_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(Nullable(Int64)))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<ArrNullableInt64Row> input = List.of(
                    new ArrNullableInt64Row(1, Arrays.asList(1L, null, 3L)),
                    new ArrNullableInt64Row(2, List.of()),
                    new ArrNullableInt64Row(3, Arrays.asList((Long) null)),
                    new ArrNullableInt64Row(4, Arrays.asList(Long.MAX_VALUE, Long.MIN_VALUE)));

            try (BulkInserter<ArrNullableInt64Row> inserter =
                         conn.createBulkInserter(table, ArrNullableInt64Row.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size(), "Expected 4 bulk rows");
            assertNullableLongList(rows.get(0)[0], new Long[] {1L, null, 3L}, "Enc row 1 mixed");
            assertNullableLongList(rows.get(1)[0], new Long[] {}, "Enc row 2 empty");
            assertNullableLongList(rows.get(2)[0], new Long[] {null}, "Enc row 3 all-null");
            assertNullableLongList(rows.get(3)[0],
                    new Long[] {Long.MAX_VALUE, Long.MIN_VALUE}, "Enc row 4 extremes");
        });
    }

    // ====================================================================
    // 2. Map(String, Array(UInt32)) — array nested in map value
    // ====================================================================

    @Test
    void mapStringArrayUInt32DecodeRoundTrips() {
        withTable("nest_map_arr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, m Map(String, Array(UInt32)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, m) VALUES"
                    + " (1, map('a', [1, 2], 'b', [])),"   // value array incl. empty
                    + " (2, map())");                      // empty map

            List<Object[]> rows = decode(conn, "SELECT m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            Map<?, ?> m0 = asMap(rows.get(0)[0], "Row 1");
            assertEquals(2, m0.size(), "Row 1 size");
            assertNullableLongList(m0.get("a"), new Long[] {1L, 2L}, "Row 1 ['a']");
            assertNullableLongList(m0.get("b"), new Long[] {}, "Row 1 ['b'] empty array");

            assertTrue(asMap(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty map");
        });
    }

    /** Record for Map(String, Array(UInt32)) encode. */
    record MapStringArrayRow(long id, Map<String, List<Long>> m) {}

    @Test
    void mapStringArrayUInt32EncodeRoundTrips() {
        withTable("nest_map_arr_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, m Map(String, Array(UInt32)))"
                    + " ENGINE = MergeTree() ORDER BY id");

            Map<String, List<Long>> m1 = new LinkedHashMap<>();
            m1.put("a", List.of(1L, 2L));
            m1.put("b", List.of());
            List<MapStringArrayRow> input = List.of(
                    new MapStringArrayRow(1, m1),
                    new MapStringArrayRow(2, new LinkedHashMap<>()));

            try (BulkInserter<MapStringArrayRow> inserter =
                         conn.createBulkInserter(table, MapStringArrayRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 bulk rows");
            Map<?, ?> m0 = asMap(rows.get(0)[0], "Enc row 1");
            assertEquals(2, m0.size(), "Enc row 1 size");
            assertNullableLongList(m0.get("a"), new Long[] {1L, 2L}, "Enc row 1 ['a']");
            assertNullableLongList(m0.get("b"), new Long[] {}, "Enc row 1 ['b'] empty array");
            assertTrue(asMap(rows.get(1)[0], "Enc row 2").isEmpty(), "Enc row 2 empty map");
        });
    }

    // ====================================================================
    // 3. Map(LowCardinality(String), UInt32) — dictionary key inside map
    // ====================================================================

    @Test
    void mapLowCardinalityStringUInt32DecodeRoundTrips() {
        withTable("nest_map_lckey", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, m Map(LowCardinality(String), UInt32))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, m) VALUES"
                    + " (1, map('x', 1, 'y', 2)),"
                    + " (2, map())");

            List<Object[]> rows = decode(conn, "SELECT m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            Map<?, ?> m0 = asMap(rows.get(0)[0], "Row 1");
            assertEquals(2, m0.size(), "Row 1 size");
            assertEquals(1L, ((Number) m0.get("x")).longValue(), "Row 1 key x");
            assertEquals(2L, ((Number) m0.get("y")).longValue(), "Row 1 key y");

            assertTrue(asMap(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty map");
        });
    }

    // ====================================================================
    // 4. Tuple(Array(UInt32), Map(String, String)) — array + map as elements
    // ====================================================================

    @Test
    void tupleArrayMapDecodeRoundTrips() {
        withTable("nest_tuple_arr_map", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, t Tuple(Array(UInt32), Map(String, String)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES"
                    + " (1, ([1, 2, 3], map('k', 'v'))),"
                    + " (2, ([], map()))");

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            List<?> t0 = asList(rows.get(0)[0], "Row 1 tuple");
            assertEquals(2, t0.size(), "Row 1 tuple arity");
            assertNullableLongList(t0.get(0), new Long[] {1L, 2L, 3L}, "Row 1 elem0 array");
            Map<?, ?> m0 = asMap(t0.get(1), "Row 1 elem1 map");
            assertEquals(1, m0.size(), "Row 1 map size");
            assertEquals("v", m0.get("k"), "Row 1 map[k]");

            List<?> t1 = asList(rows.get(1)[0], "Row 2 tuple");
            assertNullableLongList(t1.get(0), new Long[] {}, "Row 2 elem0 empty array");
            assertTrue(asMap(t1.get(1), "Row 2 elem1").isEmpty(), "Row 2 empty map");
        });
    }

    // ====================================================================
    // 5. Tuple(UInt32, Nullable(String)) — null inside a tuple element
    // ====================================================================

    @Test
    void tupleUInt32NullableStringDecodeRoundTrips() {
        withTable("nest_tuple_nstr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, t Tuple(UInt32, Nullable(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES"
                    + " (1, (7, 'x')),"
                    + " (2, (8, NULL))");

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            List<?> t0 = asList(rows.get(0)[0], "Row 1 tuple");
            assertEquals(7L, ((Number) t0.get(0)).longValue(), "Row 1 elem0");
            assertEquals("x", t0.get(1), "Row 1 elem1");

            List<?> t1 = asList(rows.get(1)[0], "Row 2 tuple");
            assertEquals(8L, ((Number) t1.get(0)).longValue(), "Row 2 elem0");
            assertNull(t1.get(1), "Row 2 elem1: expected null inside tuple");
        });
    }

    /** Record for Tuple(UInt32, Nullable(String)) encode; tuple bound as a List. */
    record TupleNullableRow(long id, List<Object> t) {}

    @Test
    void tupleUInt32NullableStringEncodeRoundTrips() {
        withTable("nest_tuple_nstr_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, t Tuple(UInt32, Nullable(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<TupleNullableRow> input = List.of(
                    new TupleNullableRow(1, Arrays.asList(7L, "x")),
                    new TupleNullableRow(2, Arrays.asList(8L, null)));

            try (BulkInserter<TupleNullableRow> inserter =
                         conn.createBulkInserter(table, TupleNullableRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 bulk rows");
            List<?> t0 = asList(rows.get(0)[0], "Enc row 1 tuple");
            assertEquals(7L, ((Number) t0.get(0)).longValue(), "Enc row 1 elem0");
            assertEquals("x", t0.get(1), "Enc row 1 elem1");
            List<?> t1 = asList(rows.get(1)[0], "Enc row 2 tuple");
            assertEquals(8L, ((Number) t1.get(0)).longValue(), "Enc row 2 elem0");
            assertNull(t1.get(1), "Enc row 2 elem1: expected null inside tuple");
        });
    }

    // ====================================================================
    // 6. Nullable(Tuple(...)) — top-level Nullable wrapping a composite.
    //    Verify whether the SERVER accepts the CREATE at all.
    // ====================================================================

    /**
     * SERVER-REJECTED (verified across the supported matrix): ClickHouse refuses
     * {@code Nullable(Tuple(...))} at CREATE time — {@code Tuple} is not a Nullable-eligible
     * inner type. The spec asks us to verify this combination; the finding is that the server
     * rejects the DDL, so we assert the rejection rather than a round-trip. This is a server
     * limitation, not a client bug (our client faithfully relays the server exception).
     *
     * <p>The exact wording differs by server version — 25.x says the type "cannot be inside
     * Nullable", whereas 26.x says "Nullable Tuple type is not allowed" — so the assertion
     * matches either phrasing rather than one literal string.
     */
    @Test
    void nullableTupleIsRejectedByServer() {
        withTable("nest_nullable_tuple", (conn, table) -> {
            ServerException ex = assertThrows(ServerException.class, () ->
                    conn.execute("CREATE TABLE " + table
                            + " (id UInt32, t Nullable(Tuple(UInt32, String)))"
                            + " ENGINE = MergeTree() ORDER BY id"),
                    "Nullable(Tuple(...)) CREATE should be rejected by the server");
            String msg = ex.getMessage();
            assertTrue(msg != null
                            && msg.contains("Nullable")
                            && (msg.contains("cannot be inside") || msg.contains("not allowed")),
                    "Expected a Nullable(Tuple) rejection from the server, got: " + msg);
        });
    }

    // ====================================================================
    // 7. Array(Tuple(UInt32, String)) — Nested/Map-like shape
    // ====================================================================

    @Test
    void arrayTupleDecodeRoundTrips() {
        withTable("nest_arr_tuple", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(Tuple(UInt32, String)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, a) VALUES"
                    + " (1, [(1, 'a'), (2, 'b')]),"
                    + " (2, [])");

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            List<?> a0 = asList(rows.get(0)[0], "Row 1 outer array");
            assertEquals(2, a0.size(), "Row 1 array length");
            List<?> e0 = asList(a0.get(0), "Row 1 elem0 tuple");
            assertEquals(1L, ((Number) e0.get(0)).longValue(), "Row 1 elem0[0]");
            assertEquals("a", e0.get(1), "Row 1 elem0[1]");
            List<?> e1 = asList(a0.get(1), "Row 1 elem1 tuple");
            assertEquals(2L, ((Number) e1.get(0)).longValue(), "Row 1 elem1[0]");
            assertEquals("b", e1.get(1), "Row 1 elem1[1]");

            assertTrue(asList(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty array");
        });
    }

    // ====================================================================
    // 8. Array(LowCardinality(String)) — dictionary inside array
    // ====================================================================

    @Test
    void arrayLowCardinalityStringDecodeRoundTrips() {
        withTable("nest_arr_lc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(LowCardinality(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, a) VALUES"
                    + " (1, ['red', 'red', 'blue']),"
                    + " (2, [])");

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");
            assertStringList(rows.get(0)[0], List.of("red", "red", "blue"), "Row 1");
            assertTrue(asList(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty array");
        });
    }

    /** Record for Array(LowCardinality(String)) encode. */
    record ArrLcStringRow(long id, List<String> a) {}

    @Test
    void arrayLowCardinalityStringEncodeRoundTrips() {
        withTable("nest_arr_lc_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(LowCardinality(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<ArrLcStringRow> input = List.of(
                    new ArrLcStringRow(1, List.of("red", "red", "blue")),
                    new ArrLcStringRow(2, List.of()));

            try (BulkInserter<ArrLcStringRow> inserter =
                         conn.createBulkInserter(table, ArrLcStringRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 bulk rows");
            assertStringList(rows.get(0)[0], List.of("red", "red", "blue"), "Enc row 1");
            assertTrue(asList(rows.get(1)[0], "Enc row 2").isEmpty(), "Enc row 2 empty array");
        });
    }

    // ====================================================================
    // 9. Array(Array(Nullable(String))) — recursion + null
    // ====================================================================

    @Test
    void arrayArrayNullableStringDecodeRoundTrips() {
        withTable("nest_arr_arr_nstr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(Array(Nullable(String))))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, a) VALUES"
                    + " (1, [['a', NULL], []]),"
                    + " (2, [])");

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            List<?> outer = asList(rows.get(0)[0], "Row 1 outer");
            assertEquals(2, outer.size(), "Row 1 outer length");
            assertEquals(Arrays.asList("a", null), asList(outer.get(0), "Row 1 inner0"),
                    "Row 1 inner0 mixed null");
            assertTrue(asList(outer.get(1), "Row 1 inner1").isEmpty(), "Row 1 inner1 empty");

            assertTrue(asList(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty outer");
        });
    }

    // ====================================================================
    // 10. Map(String, Nullable(Int64)) — null-map inside map values
    // ====================================================================

    @Test
    void mapStringNullableInt64DecodeRoundTrips() {
        withTable("nest_map_nint64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, m Map(String, Nullable(Int64)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, m) VALUES"
                    + " (1, map('a', 1, 'b', NULL)),"
                    + " (2, map())");

            List<Object[]> rows = decode(conn, "SELECT m FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            Map<?, ?> m0 = asMap(rows.get(0)[0], "Row 1");
            assertEquals(2, m0.size(), "Row 1 size");
            assertEquals(1L, ((Number) m0.get("a")).longValue(), "Row 1 key a");
            assertTrue(m0.containsKey("b"), "Row 1 must contain key b");
            assertNull(m0.get("b"), "Row 1 key b: expected null map value");

            assertTrue(asMap(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty map");
        });
    }

    // ====================================================================
    // 11. Array(LowCardinality(Nullable(String))) — dictionary + null in array
    // ====================================================================

    @Test
    void arrayLowCardinalityNullableStringDecodeRoundTrips() {
        withTable("nest_arr_lc_nstr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, a Array(LowCardinality(Nullable(String))))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, a) VALUES"
                    + " (1, ['a', NULL, 'a']),"
                    + " (2, []),"
                    + " (3, [NULL])");

            List<Object[]> rows = decode(conn, "SELECT a FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 rows");
            assertStringList(rows.get(0)[0], Arrays.asList("a", null, "a"), "Row 1 mixed");
            assertTrue(asList(rows.get(1)[0], "Row 2").isEmpty(), "Row 2 expected empty array");
            assertStringList(rows.get(2)[0], Arrays.asList((String) null), "Row 3 all-null");
        });
    }
}
