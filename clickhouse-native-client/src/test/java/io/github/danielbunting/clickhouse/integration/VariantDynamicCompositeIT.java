package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DECODE round-trips for composite member types inside {@code Variant} and {@code Dynamic}
 * columns, plus JSON sub-path column access — the composite cases the reference client
 * exercises in client-v2 {@code DataTypeTests} ({@code testVariantWith*},
 * {@code testDynamicWith*}, {@code testJSONSubPathAccess}).
 *
 * <p>Server-encoded literals are read back through {@link
 * io.github.danielbunting.clickhouse.types.codec.VariantColumnCodec} /
 * {@link io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec}, so each test
 * pins the decoded Java shape (member values arrive exactly as the member type's own codec
 * would decode them) and the server-reported {@code variantType()}/{@code dynamicType()}.
 *
 * <p>Server quirk pinned during characterization (not asserted here): a bare {@code NULL}
 * literal in a multi-row {@code VALUES} list for a {@code Dynamic} column is not always
 * stored as NULL — next to {@code Array} rows it becomes the {@code String} {@code 'NULL'},
 * and next to {@code Time} rows it becomes {@code Time 00:00:00} (confirmed server-side via
 * {@code toString(d)}, so it is not a client decode issue). NULL rows are therefore asserted
 * only in the shapes where the server genuinely stores NULL; the plain NULL round-trip is
 * already covered by {@code DynamicTypesIT}.
 */
@Tag("integration")
class VariantDynamicCompositeIT extends TypeRoundTripBase {

    private static void enableExperimentalTypes(io.github.danielbunting.clickhouse.ClickHouseConnection conn) {
        conn.execute("SET allow_experimental_variant_type = 1");
        conn.execute("SET allow_experimental_dynamic_type = 1");
        conn.execute("SET allow_experimental_json_type = 1");
        conn.execute("SET enable_time_time64_type = 1");
    }

    /** Asserts a decoded cell is a {@link Number} with the given long value. */
    private static void assertLongValue(long expected, Object cell) {
        assertInstanceOf(Number.class, cell, "expected a Number, was "
                + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(expected, ((Number) cell).longValue());
    }

    // ------------------------------------------------------------------ Variant

    /** Variant with a Decimal member: string rows, decimal rows, and NULL each keep their shape. */
    @Test
    void variantWithDecimalMemberDecodes() {
        withTable("variant_dec", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(String, Decimal32(4))) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, '10.202'), (2, 10.1233::Decimal32(4)), (3, NULL)");

            List<Object[]> rows = decode(conn,
                    "SELECT v, variantType(v) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            // A numeric-looking string stays in the String member — no silent coercion.
            assertEquals("10.202", rows.get(0)[0]);
            assertEquals("String", rows.get(0)[1]);

            Object dec = rows.get(1)[0];
            assertInstanceOf(BigDecimal.class, dec);
            assertEquals(0, new BigDecimal("10.1233").compareTo((BigDecimal) dec));
            assertEquals("Decimal(9, 4)", rows.get(1)[1], "Decimal32(4) canonicalizes to Decimal(9, 4)");

            assertNull(rows.get(2)[0]);
            assertEquals("None", rows.get(2)[1]);
        });
    }

    /** Variant with an Array member: filled and empty arrays plus String and NULL rows. */
    @Test
    void variantWithArrayMemberDecodes() {
        withTable("variant_arr", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(Array(UInt32), String)) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, [1, 2, 3]), (2, 'str'), (3, NULL), (4, [])");

            List<Object[]> rows = decode(conn,
                    "SELECT v, variantType(v) FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size());

            // UInt32 elements widen to Long, exactly as a standalone Array(UInt32) decodes.
            assertEquals(List.of(1L, 2L, 3L), rows.get(0)[0]);
            assertEquals("Array(UInt32)", rows.get(0)[1]);

            assertEquals("str", rows.get(1)[0]);
            assertEquals("String", rows.get(1)[1]);

            assertNull(rows.get(2)[0]);
            assertEquals("None", rows.get(2)[1]);

            assertEquals(List.of(), rows.get(3)[0], "empty array is an Array row, not NULL");
            assertEquals("Array(UInt32)", rows.get(3)[1]);
        });
    }

    /** Variant with a Map member: map rows decode to Map, scalar rows to String, NULL to null. */
    @Test
    void variantWithMapMemberDecodes() {
        withTable("variant_map", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(Map(String, Int64), String)) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, map('a', 1, 'b', 2)), (2, 'plain'), (3, NULL)");

            List<Object[]> rows = decode(conn,
                    "SELECT v, variantType(v) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            assertEquals(Map.of("a", 1L, "b", 2L), rows.get(0)[0]);
            assertEquals("Map(String, Int64)", rows.get(0)[1]);

            assertEquals("plain", rows.get(1)[0]);
            assertEquals("String", rows.get(1)[1]);

            assertNull(rows.get(2)[0]);
            assertEquals("None", rows.get(2)[1]);
        });
    }

    /**
     * Variant with an Enum member: name literals and ordinal casts both land in the Enum member
     * and decode to the enum NAME (matching {@code B6CodecsTest} Enum semantics); {@code true}
     * lands in the Bool member.
     */
    @Test
    void variantWithEnumMemberDecodes() {
        withTable("variant_enum", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(Bool, Enum('stopped' = 1, 'running' = 2)))"
                    + " ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, 'stopped'), (2, true),"
                    + " (3, CAST(2, 'Enum(''stopped'' = 1, ''running'' = 2)'))");

            List<Object[]> rows = decode(conn,
                    "SELECT v, variantType(v) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            assertEquals("stopped", rows.get(0)[0]);
            assertEquals("Enum8('stopped' = 1, 'running' = 2)", rows.get(0)[1]);

            assertEquals(Boolean.TRUE, rows.get(1)[0]);
            assertEquals("Bool", rows.get(1)[1]);

            assertEquals("running", rows.get(2)[0], "ordinal cast decodes back to the enum name");
            assertEquals("Enum8('stopped' = 1, 'running' = 2)", rows.get(2)[1]);
        });
    }

    /** Variant with a Time64 member: Time64 rows decode to Duration (beyond-24h supported). */
    @Test
    void variantWithTime64MemberDecodes() {
        withTable("variant_t64", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(Time64, String)) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, '30:33:30'::Time64(3)), (2, 'str'), (3, NULL)");

            List<Object[]> rows = decode(conn,
                    "SELECT v, variantType(v) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            assertEquals(Duration.ofHours(30).plusMinutes(33).plusSeconds(30), rows.get(0)[0]);
            assertEquals("Time64(3)", rows.get(0)[1], "bare Time64 canonicalizes to Time64(3)");

            assertEquals("str", rows.get(1)[0]);
            assertEquals("String", rows.get(1)[1]);

            assertNull(rows.get(2)[0]);
            assertEquals("None", rows.get(2)[1]);
        });
    }

    // ------------------------------------------------------------------ Dynamic

    /** Dynamic holding arrays of different element types: each row keeps its own array shape. */
    @Test
    void dynamicHoldingArraysDecodes() {
        withTable("dynamic_arr", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, [1, 2, 3]::Array(Int64)), (2, ['a', 'b']::Array(String))");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            assertEquals(List.of(1L, 2L, 3L), rows.get(0)[0]);
            assertEquals("Array(Int64)", rows.get(0)[1]);

            assertEquals(List.of("a", "b"), rows.get(1)[0]);
            assertEquals("Array(String)", rows.get(1)[1]);
        });
    }

    /** Dynamic holding a map alongside a scalar and a NULL row. */
    @Test
    void dynamicHoldingMapsDecodes() {
        withTable("dynamic_map", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, map('k1', 11, 'k2', 22)::Map(String, Int64)), (2, 'scalar'), (3, NULL)");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            // The server may narrow the stored value type (observed: Map(String, Int8)),
            // so assert values numerically and the type by its Map(String, ...) shape.
            Object m = rows.get(0)[0];
            assertInstanceOf(Map.class, m);
            Map<?, ?> map = (Map<?, ?>) m;
            assertEquals(2, map.size());
            assertLongValue(11, map.get("k1"));
            assertLongValue(22, map.get("k2"));
            assertTrue(String.valueOf(rows.get(0)[1]).startsWith("Map(String,"),
                    "dynamicType should be a Map(String, ...) shape, was " + rows.get(0)[1]);

            assertEquals("scalar", rows.get(1)[0]);
            assertEquals("String", rows.get(1)[1]);

            assertNull(rows.get(2)[0]);
            assertEquals("None", rows.get(2)[1]);
        });
    }

    /** Dynamic holding Time64(3) and Time values: both decode to Duration at their precision. */
    @Test
    void dynamicHoldingTime64Decodes() {
        withTable("dynamic_t64", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, '30:33:30.123'::Time64(3)), (2, '11:22:33'::Time)");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            assertEquals(Duration.ofHours(30).plusMinutes(33).plusSeconds(30).plusMillis(123),
                    rows.get(0)[0]);
            assertEquals("Time64(3)", rows.get(0)[1]);

            assertEquals(Duration.ofHours(11).plusMinutes(22).plusSeconds(33), rows.get(1)[0]);
            assertEquals("Time", rows.get(1)[1]);
        });
    }

    /** Dynamic holding nested composites: unnamed Tuple, Array(Map), and a named Tuple. */
    @Test
    void dynamicHoldingNestedCompositesDecodes() {
        withTable("dynamic_nested", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, tuple(1, 'x')::Tuple(UInt32, String)),"
                    + " (2, [map('a', 1)]::Array(Map(String, Int64))),"
                    + " (3, tuple('n', [1, 2])::Tuple(name String, vals Array(UInt8)))");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            // Tuples decode to a List of member values.
            assertEquals(List.of(1L, "x"), rows.get(0)[0]);
            assertEquals("Tuple(UInt32, String)", rows.get(0)[1]);

            Object arrOfMap = rows.get(1)[0];
            assertInstanceOf(List.class, arrOfMap);
            List<?> l = (List<?>) arrOfMap;
            assertEquals(1, l.size());
            assertInstanceOf(Map.class, l.get(0));
            assertLongValue(1, ((Map<?, ?>) l.get(0)).get("a"));
            assertTrue(String.valueOf(rows.get(1)[1]).startsWith("Array(Map(String,"),
                    "dynamicType should be Array(Map(String, ...)), was " + rows.get(1)[1]);

            // Named tuple: same List shape, names live only in the type string.
            Object named = rows.get(2)[0];
            assertInstanceOf(List.class, named);
            List<?> nl = (List<?>) named;
            assertEquals("n", nl.get(0));
            assertInstanceOf(List.class, nl.get(1));
            List<?> vals = (List<?>) nl.get(1);
            assertEquals(2, vals.size());
            assertLongValue(1, vals.get(0));
            assertLongValue(2, vals.get(1));
            assertEquals("Tuple(name String, vals Array(UInt8))", rows.get(2)[1]);
        });
    }

    /**
     * Dynamic holding FixedString: decodes to the logical content WITHOUT NUL padding —
     * unlike a plain {@code FixedString} column, which keeps its padded width (see
     * {@code StringExtraTypesIT}); the padding is already absent on the Dynamic wire.
     */
    @Test
    void dynamicHoldingFixedStringDecodes() {
        withTable("dynamic_fs", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, 'abc'::FixedString(5)), (2, 42::Int64)");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            Object fs = rows.get(0)[0];
            assertInstanceOf(String.class, fs);
            assertEquals("abc", fs, "no NUL padding on the Dynamic wire");
            assertEquals("FixedString(5)", rows.get(0)[1]);

            assertLongValue(42, rows.get(1)[0]);
            assertEquals("Int64", rows.get(1)[1]);
        });
    }

    /**
     * A JSON value stored in a {@code Dynamic} column decodes to its JSON text (was
     * knownBug 32: the JSON member's serialization prefix is read before the Dynamic
     * discriminators and its body after them — see
     * {@code DynamicColumnCodec.readFlattened}).
     */
    @Test
    void dynamicHoldingJsonDecodes() {
        withTable("dynamic_json", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, '{\"name\": \"r1\", \"value\": 0.1}'::JSON)");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size());
            assertEquals("JSON", rows.get(0)[1]);

            Object j = rows.get(0)[0];
            assertInstanceOf(String.class, j, "JSON inside Dynamic should decode like a JSON column (a String)");
            assertTrue(((String) j).contains("\"name\":\"r1\""), "was " + j);
        });
    }

    /**
     * Same wire shape as {@link #dynamicHoldingJsonDecodes}, with a JSON value containing an
     * array path (the reference client's {@code testDynamicWithJSONWithArrays}); the array
     * path renders back as a JSON array.
     */
    @Test
    void dynamicHoldingJsonWithArraysDecodes() {
        withTable("dynamic_json_arr", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, '{\"a\": [1,2,3]}'::JSON)");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size());
            assertEquals("JSON", rows.get(0)[1]);

            Object j = rows.get(0)[0];
            assertInstanceOf(String.class, j, "JSON-with-arrays inside Dynamic should decode to a String");
            assertTrue(((String) j).contains("\"a\":[1,2,3]"), "was " + j);
        });
    }

    /**
     * Dynamic holding Variant-cast values: the server unwraps the Variant and stores the active
     * member directly ({@code dynamicType} reports {@code Int64}/{@code String}, not
     * {@code Variant(...)}), so the client sees plain member values.
     */
    @Test
    void dynamicHoldingVariantValuesUnwrapsToMembers() {
        withTable("dynamic_var", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, CAST(42::Int64, 'Variant(String, Int64)')),"
                    + " (2, CAST('s', 'Variant(String, Int64)'))");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            assertLongValue(42, rows.get(0)[0]);
            assertEquals("Int64", rows.get(0)[1]);

            assertEquals("s", rows.get(1)[0]);
            assertEquals("String", rows.get(1)[1]);
        });
    }

    // ------------------------------------------------------------------ JSON sub-paths

    /**
     * JSON sub-path columns ({@code j.a.b}) decode as Dynamic-typed columns: present paths
     * yield their typed value, absent paths yield NULL.
     */
    @Test
    void jsonSubPathAccessDecodesTypedColumns() {
        withTable("json_subpath", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, j) VALUES"
                    + " (1, '{\"a\": {\"b\": 42}, \"c\": \"str\", \"arr\": [1,2]}'),"
                    + " (2, '{\"a\": {\"b\": 7}}')");

            List<Object[]> rows = decode(conn,
                    "SELECT j.a.b, j.c, j.arr, j.missing FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            Object[] r1 = rows.get(0);
            assertLongValue(42, r1[0]);
            assertEquals("str", r1[1]);
            assertInstanceOf(List.class, r1[2]);
            List<?> arr = (List<?>) r1[2];
            assertEquals(2, arr.size());
            assertLongValue(1, arr.get(0));
            assertLongValue(2, arr.get(1));
            assertNull(r1[3], "a path that appears in no row decodes as NULL");

            Object[] r2 = rows.get(1);
            assertLongValue(7, r2[0]);
            assertNull(r2[1], "path absent for this row is NULL");
            assertNull(r2[2]);
            assertNull(r2[3]);
        });
    }

    /** A typed sub-path hint ({@code JSON(a.b Int64)}) reads back through the declared type. */
    @Test
    void jsonTypedSubPathHintDecodes() {
        withTable("json_subpath_typed", (conn, table) -> {
            enableExperimentalTypes(conn);
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, j JSON(a.b Int64)) ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, j) VALUES"
                    + " (1, '{\"a\": {\"b\": 42}, \"c\": 1.5}')");

            List<Object[]> rows = decode(conn,
                    "SELECT j.a.b, j.c FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size());

            assertLongValue(42, rows.get(0)[0]);
            // Untyped paths stay Dynamic; a JSON number with a fraction decodes as Float64.
            assertEquals(1.5d, rows.get(0)[1]);
        });
    }
}
