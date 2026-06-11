package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cross-client round-trips for the wrapped and container types: Nullable
 * scalars with interleaved nulls, Array (including empty arrays and nullable
 * elements), Map (including the empty map) and LowCardinality (plain and
 * nullable) — the families where null-map, offsets and dictionary handling
 * are wire-format-sensitive on the native side.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientNestedTypesIT extends CrossClientRoundTripBase {

    /** Nullable(Int32), Nullable(String), Nullable(DateTime64) with interleaved nulls. */
    @Test
    void nullableScalars() {
        record Row(int id, Integer i, String s, Instant dt) {}
        String columns = "id, i, s, dt";
        Instant t1 = Instant.parse("2024-03-15T12:34:56.789Z");
        List<Object[]> expected = rowsOf(
                row(1L, 42, "present", t1),
                row(2L, null, null, null),
                row(3L, -7, "", t1),
                row(4L, null, "only string", null));

        withTable("xc_null", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  i  Nullable(Int32),"
                    + "  s  Nullable(String),"
                    + "  dt Nullable(DateTime64(3, 'UTC'))"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("nullable native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 42, "present", t1),
                    new Row(2, null, null, null),
                    new Row(3, -7, "", t1),
                    new Row(4, null, "only string", null));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("nullable official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id",
                            null, null, null, OffsetDateTime.class));
        });
    }

    /** Array(Int32) including the empty array, and Array(Nullable(String)). */
    @Test
    void arrays() {
        record Row(int id, List<Integer> a, List<String> ns) {}
        String columns = "id, a, ns";
        List<Object[]> expected = rowsOf(
                row(1L, List.of(), List.of()),
                row(2L, List.of(1, 2, 3), Arrays.asList("x", null, "")),
                row(3L, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                        Arrays.asList(null, "snow ❄️")));

        withTable("xc_arr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  a  Array(Int32),"
                    + "  ns Array(Nullable(String))"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("array native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, List.of(), List.of()),
                    new Row(2, List.of(1, 2, 3), Arrays.asList("x", null, "")),
                    new Row(3, List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                            Arrays.asList(null, "snow ❄️")));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("array official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Map(String, Int64) including the empty map; insertion order preserved. */
    @Test
    void maps() {
        record Row(int id, Map<String, Long> m) {}
        Map<String, Long> empty = new LinkedHashMap<>();
        Map<String, Long> two = new LinkedHashMap<>();
        two.put("alpha", 1L);
        two.put("beta", -2L);
        Map<String, Long> extremes = new LinkedHashMap<>();
        extremes.put("min", Long.MIN_VALUE);
        extremes.put("max", Long.MAX_VALUE);
        extremes.put("", 0L);
        List<Object[]> expected = rowsOf(
                row(1L, empty),
                row(2L, two),
                row(3L, extremes));

        withTable("xc_map", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  m  Map(String, Int64)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, m", expected);
            assertRowsMatch("map native decode", expected,
                    decode(conn, "SELECT id, m FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, empty),
                    new Row(2, two),
                    new Row(3, extremes));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("map official read", expected,
                    officialSelect("SELECT id, m FROM " + table + " ORDER BY id"));
        });
    }

    /** LowCardinality(String) and LowCardinality(Nullable(String)). */
    @Test
    void lowCardinality() {
        record Row(int id, String lc, String lcn) {}
        String columns = "id, lc, lcn";
        List<Object[]> expected = rowsOf(
                row(1L, "red", "tagged"),
                row(2L, "green", null),
                row(3L, "red", ""),
                row(4L, "", "tagged"));

        withTable("xc_lc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  lc  LowCardinality(String),"
                    + "  lcn LowCardinality(Nullable(String))"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("low-cardinality native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, "red", "tagged"),
                    new Row(2, "green", null),
                    new Row(3, "red", ""),
                    new Row(4, "", "tagged"));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("low-cardinality official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }
}
