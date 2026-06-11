package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Cross-client round-trips for the experimental / newer types: Variant,
 * Dynamic, JSON, BFloat16 and Time/Time64. The native side gates each type
 * with the same {@code SET allow_experimental_*} flags the per-type ITs use;
 * the official connections carry the matching {@code clickhouse_setting_*}
 * properties.
 *
 * <p>Official-driver read gaps (0.9.0) and their workarounds:
 * <ul>
 *   <li>{@code Time}/{@code Time64} have no {@code ClickHouseDataType}
 *       constant — raw reads throw, so official legs read
 *       {@code toString(t)} and parse via {@code parseClickHouseTime};</li>
 *   <li>{@code BFloat16} has no {@code BinaryStreamReader} case — official
 *       legs read {@code CAST(b AS Float32)}, which still validates the
 *       native-encoded stored bytes through the server cast.</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientExperimentalTypesIT extends CrossClientRoundTripBase {

    /** Variant(UInt32, String): per-row runtime type, including NULL. */
    @Test
    void variant() {
        record Row(int id, Object v) {}
        Map<String, String> settings = Map.of("allow_experimental_variant_type", "1");
        List<Object[]> expected = rowsOf(
                row(1L, 42L),
                row(2L, "text"),
                row(3L, null),
                row(4L, 0L));

        withTable("xc_variant", (conn, table) -> {
            conn.execute("SET allow_experimental_variant_type = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  v  Variant(UInt32, String)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, v", rowsOf(
                    row(1L, 42),
                    row(2L, "text"),
                    row(3L, null),
                    row(4L, 0)), settings);
            assertRowsMatch("variant native decode", expected,
                    decode(conn, "SELECT id, v FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 42L),
                    new Row(2, "text"),
                    new Row(3, null),
                    new Row(4, 0L));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            // Native decode re-checks the bulk-inserted rows incl. the NULL.
            assertRowsMatch("variant native decode (after encode)", expected,
                    decode(conn, "SELECT id, v FROM " + table + " ORDER BY id"));
            // KNOWN BUG in clickhouse-jdbc 0.9.0: reading a Variant NULL row
            // throws IndexOutOfBoundsException (discriminator -1 used as a
            // type-list index in BinaryStreamReader.readVariant), so the
            // official leg reads only the non-NULL rows.
            List<Object[]> nonNull = rowsOf(
                    row(1L, 42L),
                    row(2L, "text"),
                    row(4L, 0L));
            assertRowsMatch("variant official read (non-NULL rows)", nonNull,
                    officialSelect(settings, "SELECT id, v FROM " + table
                            + " WHERE v IS NOT NULL ORDER BY id"));
        });
    }

    /** Dynamic: runtime-typed values (Int64 / String / NULL). */
    @Test
    void dynamic() {
        record Row(int id, Object d) {}
        Map<String, String> settings = Map.of("allow_experimental_dynamic_type", "1");
        List<Object[]> expected = rowsOf(
                row(1L, 42L),
                row(2L, "hello"),
                row(3L, null),
                row(4L, -1L));

        withTable("xc_dynamic", (conn, table) -> {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  d  Dynamic"
                    + ") ENGINE = MergeTree() ORDER BY id");

            // SERVER QUIRK (pinned by this layout): if a VALUES list mixes a
            // bare NULL with explicitly cast literals (42::Int64), the
            // server's VALUES type inference coerces the NULL through the cast
            // type and stores Dynamic Int64 0 instead of Dynamic NULL —
            // dynamicType(d) itself reports Int64. Keeping the NULL in a
            // cast-free statement preserves it.
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, 42::Int64), (2, 'hello'), (3, NULL)");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES (4, -1::Int64)");
            assertBothClientsRead("dynamic neutral", conn,
                    "SELECT id, d FROM " + table + " ORDER BY id",
                    "SELECT id, d FROM " + table + " ORDER BY id",
                    expected, settings);

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 42L),
                    new Row(2, "hello"),
                    new Row(3, null),
                    new Row(4, -1L));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("dynamic official read", expected,
                    officialSelect(settings, "SELECT id, d FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * JSON: flat objects with alphabetically-ordered keys and int/string
     * leaves, so the native client's reconstructed string (sorted keys, NULL
     * paths omitted) matches the inserted text exactly. The official leg
     * reads the driver's binary-JSON mode, which materializes
     * {@code Map<String, Object>} with absent paths omitted — asserted
     * against structurally equivalent Map anchors. (The driver's
     * JSON-as-string read mode is broken against 25.8 server output: the
     * server honors {@code output_format_binary_write_json_as_string} but the
     * 0.9.0 reader still parses binary and hits EOF.)
     */
    @Test
    void json() {
        record Row(int id, String j) {}
        Map<String, String> settings = Map.of("allow_experimental_json_type", "1");
        List<Object[]> nativeExpected = rowsOf(
                row(1L, "{\"a\":1,\"b\":\"x\"}"),
                row(2L, "{\"a\":2}"),
                row(3L, "{\"b\":\"only\"}"));
        Map<String, Object> m1 = new java.util.LinkedHashMap<>();
        m1.put("a", 1L);
        m1.put("b", "x");
        List<Object[]> officialExpected = rowsOf(
                row(1L, m1),
                row(2L, Map.of("a", 2L)),
                row(3L, Map.of("b", "only")));

        withTable("xc_json", (conn, table) -> {
            conn.execute("SET allow_experimental_json_type = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  j  JSON"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, j", rowsOf(
                    row(1L, "{\"a\":1,\"b\":\"x\"}"),
                    row(2L, "{\"a\":2}"),
                    row(3L, "{\"b\":\"only\"}")), settings);
            assertRowsMatch("json native decode", nativeExpected,
                    decode(conn, "SELECT id, j FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, "{\"a\":1,\"b\":\"x\"}"),
                    new Row(2, "{\"a\":2}"),
                    new Row(3, "{\"b\":\"only\"}"));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("json native decode (after encode)", nativeExpected,
                    decode(conn, "SELECT id, j FROM " + table + " ORDER BY id"));
            assertRowsMatch("json official read (map mode)", officialExpected,
                    officialSelect(settings, "SELECT id, j FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * BFloat16 with exactly-representable values (8-bit exponent, 7-bit
     * mantissa). The official driver cannot read raw BFloat16, so its leg
     * reads {@code CAST(b AS Float32)} — still validating the stored bytes.
     */
    @Test
    void bfloat16() {
        record Row(int id, float b, Float nb) {}
        Map<String, String> settings = Map.of("allow_experimental_bfloat16_type", "1");
        List<Object[]> expected = rowsOf(
                row(1L, 0.0f, null),
                row(2L, 1.5f, -0.25f),
                row(3L, -3.0f, 256.0f));

        withTable("xc_bf16", (conn, table) -> {
            conn.execute("SET allow_experimental_bfloat16_type = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  b  BFloat16,"
                    + "  nb Nullable(BFloat16)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, b, nb", expected, settings);
            assertRowsMatch("bfloat16 native decode", expected,
                    decode(conn, "SELECT id, b, nb FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 0.0f, null),
                    new Row(2, 1.5f, -0.25f),
                    new Row(3, -3.0f, 256.0f));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("bfloat16 official read (via Float32 cast)", expected,
                    officialSelect(settings, "SELECT id, CAST(b AS Float32),"
                            + " CAST(nb AS Nullable(Float32)) FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * Time (second precision, hours may exceed 24 up to 999:59:59). Official
     * insert binds {@link LocalTime} for sub-24h rows and a string literal for
     * the extreme; the official read leg goes through {@code toString(t)}.
     */
    @Test
    void time() {
        record Row(int id, Duration t) {}
        Map<String, String> settings = Map.of("enable_time_time64_type", "1");
        Duration midday = Duration.ofHours(12).plusMinutes(30).plusSeconds(45);
        Duration max = Duration.ofHours(999).plusMinutes(59).plusSeconds(59);
        List<Object[]> expected = rowsOf(
                row(1L, Duration.ZERO),
                row(2L, midday),
                row(3L, max));

        withTable("xc_time", (conn, table) -> {
            conn.execute("SET enable_time_time64_type = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t  Time"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, t", rowsOf(
                    row(1L, LocalTime.of(0, 0, 0)),
                    row(2L, LocalTime.of(12, 30, 45)),
                    row(3L, "999:59:59")), settings);
            assertRowsMatch("time native decode", expected,
                    decode(conn, "SELECT id, t FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, Duration.ZERO),
                    new Row(2, midday),
                    new Row(3, max));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("time official read (via toString)", expected,
                    officialSelect(settings,
                            "SELECT id, toString(t) FROM " + table + " ORDER BY id"));
        });
    }

    /** Time64(3) and Time64(9) with fractional-second anchors. */
    @Test
    void time64() {
        record Row(int id, Duration t3, Duration t9) {}
        Map<String, String> settings = Map.of("enable_time_time64_type", "1");
        Duration ms = Duration.ofHours(12).plusMinutes(30).plusSeconds(45).plusMillis(123);
        Duration ns = Duration.ofHours(12).plusMinutes(30).plusSeconds(45).plusNanos(123456789L);
        List<Object[]> expected = rowsOf(
                row(1L, Duration.ZERO, Duration.ZERO),
                row(2L, ms, ns),
                row(3L, Duration.ofMillis(1), Duration.ofNanos(1)));

        withTable("xc_time64", (conn, table) -> {
            conn.execute("SET enable_time_time64_type = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t3 Time64(3),"
                    + "  t9 Time64(9)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, t3, t9", rowsOf(
                    row(1L, "00:00:00.000", "00:00:00.000000000"),
                    row(2L, "12:30:45.123", "12:30:45.123456789"),
                    row(3L, "00:00:00.001", "00:00:00.000000001")), settings);
            assertRowsMatch("time64 native decode", expected,
                    decode(conn, "SELECT id, t3, t9 FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, Duration.ZERO, Duration.ZERO),
                    new Row(2, ms, ns),
                    new Row(3, Duration.ofMillis(1), Duration.ofNanos(1)));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("time64 official read (via toString)", expected,
                    officialSelect(settings, "SELECT id, toString(t3), toString(t9)"
                            + " FROM " + table + " ORDER BY id"));
        });
    }
}
