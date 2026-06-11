package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Cross-client round-trips for the composite and wrapper types: FixedString,
 * SimpleAggregateFunction, Tuple (unnamed, named, nested), Nested, and
 * doubly-nested arrays. Where one side's encoder cannot participate (no native
 * {@code BulkInserter} support for Nested), the test falls back to a neutral
 * raw-SQL insert and cross-validates both decoders via
 * {@code assertBothClientsRead}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientCompositeTypesIT extends CrossClientRoundTripBase {

    /**
     * FixedString(8) and Nullable(FixedString(4)). The native codec strips
     * trailing NULs on decode; the official driver returns the full padded N
     * bytes — {@code cellMatches} tolerates the padding, so both sides are
     * asserted against the logical (unpadded) strings.
     */
    @Test
    void fixedStrings() {
        record Row(int id, String fs, String nfs) {}
        String columns = "id, fs, nfs";
        List<Object[]> expected = rowsOf(
                row(1L, "exactly8", "abcd"),
                row(2L, "abc", null),
                row(3L, "", "x"),
                row(4L, "héllo", "é"));

        withTable("xc_fixstr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  fs  FixedString(8),"
                    + "  nfs Nullable(FixedString(4))"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("fixed string native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, "exactly8", "abcd"),
                    new Row(2, "abc", null),
                    new Row(3, "", "x"),
                    new Row(4, "héllo", "é"));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("fixed string official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * SimpleAggregateFunction is transparent on the wire — plain values of the
     * inner type. Single insert, no merges, so values pass through untouched.
     */
    @Test
    void simpleAggregateFunctions() {
        record Row(int id, long s, String m) {}
        String columns = "id, s, m";
        List<Object[]> expected = rowsOf(
                row(1L, 100L, "alpha"),
                row(2L, 0L, ""),
                row(3L, Long.MAX_VALUE, "zzz"));

        withTable("xc_saf", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  s  SimpleAggregateFunction(sum, UInt64),"
                    + "  m  SimpleAggregateFunction(max, String)"
                    + ") ENGINE = AggregatingMergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("SAF native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 100L, "alpha"),
                    new Row(2, 0L, ""),
                    new Row(3, Long.MAX_VALUE, "zzz"));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("SAF official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Unnamed, named and composite tuples (named tuples are wire-identical). */
    @Test
    void tuples() {
        record Row(int id, List<Object> t, List<Object> nt, List<Object> ct) {}
        String columns = "id, t, nt, ct";
        List<Object[]> expected = rowsOf(
                row(1L, List.of(11L, "alpha"), List.of(100L, "foo"),
                        Arrays.asList(-5, List.of((short) 1, (short) 2))),
                row(2L, List.of(0L, ""), List.of(0L, ""),
                        Arrays.asList(null, List.of())));

        withTable("xc_tuple", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t  Tuple(UInt32, String),"
                    + "  nt Tuple(a UInt32, s String),"
                    + "  ct Tuple(Nullable(Int32), Array(Int16))"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, rowsOf(
                    row(1L, new com.clickhouse.data.Tuple(11, "alpha"),
                            new com.clickhouse.data.Tuple(100, "foo"),
                            new com.clickhouse.data.Tuple(-5, List.of(1, 2))),
                    row(2L, new com.clickhouse.data.Tuple(0, ""),
                            new com.clickhouse.data.Tuple(0, ""),
                            new com.clickhouse.data.Tuple(null, List.of()))));
            assertRowsMatch("tuple native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, List.of(11L, "alpha"), List.of(100L, "foo"),
                            Arrays.asList(-5, List.of((short) 1, (short) 2))),
                    new Row(2, List.of(0L, ""), List.of(0L, ""),
                            Arrays.asList(null, List.of())));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("tuple official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * Nested(a UInt32, b String) under the default {@code flatten_nested = 1}.
     * Direction A: the official client binds array literals for the flattened
     * sub-columns {@code n.a}/{@code n.b}; native reads both the whole column
     * ({@code SELECT n} reassembles {@code Array(Tuple)}) and the sub-columns.
     * Direction B is replaced by a neutral leg: the native client has no
     * Nested {@code BulkInserter} support (record fields cannot map to dotted
     * sub-columns), so raw SQL inserts and both clients cross-validate reads.
     */
    @Test
    void nestedColumn() {
        List<Object[]> wholeColumn = rowsOf(
                row(1L, List.of(List.of(10L, "x"), List.of(20L, "y"))),
                row(2L, List.of()));
        List<Object[]> subColumns = rowsOf(
                row(1L, List.of(10L, 20L), List.of("x", "y")),
                row(2L, List.of(), List.of()));

        withTable("xc_nested", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  n  Nested(a UInt32, b String)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            // Direction A: official client binds the flattened sub-column arrays.
            officialInsert(table, "id, n.a, n.b", rowsOf(
                    row(1L, List.of(10, 20), List.of("x", "y")),
                    row(2L, List.of(), List.of())));
            assertRowsMatch("nested native decode (whole column)", wholeColumn,
                    decode(conn, "SELECT id, n FROM " + table + " ORDER BY id"));
            assertRowsMatch("nested native decode (sub-columns)", subColumns,
                    decode(conn, "SELECT id, n.a, n.b FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            // Neutral leg: server parses literals; both clients read back.
            conn.execute("INSERT INTO " + table + " (id, n.a, n.b) VALUES"
                    + " (1, [10, 20], ['x', 'y']),"
                    + " (2, [], [])");
            assertBothClientsRead("nested sub-columns", conn,
                    "SELECT id, n.a, n.b FROM " + table + " ORDER BY id", subColumns);
            assertRowsMatch("nested official read (whole column)", wholeColumn,
                    officialSelect("SELECT id, n FROM " + table + " ORDER BY id"));
        });
    }

    /** Array(Array(Int32)) and Array(Array(Nullable(String))) incl. [] and [[]]. */
    @Test
    void nestedArrays() {
        record Row(int id, List<List<Integer>> aa, List<List<String>> ans) {}
        String columns = "id, aa, ans";
        List<Object[]> expected = rowsOf(
                row(1L, List.of(), List.of()),
                row(2L, List.of(List.of()), List.of(List.of())),
                row(3L, List.of(List.of(1), List.of(2, 3)),
                        List.of(Arrays.asList("x", null), List.of(""))),
                row(4L, List.of(List.of(Integer.MIN_VALUE, Integer.MAX_VALUE), List.of(), List.of(0)),
                        List.of(Arrays.asList((String) null))));

        withTable("xc_arrarr", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  aa  Array(Array(Int32)),"
                    + "  ans Array(Array(Nullable(String)))"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("nested arrays native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, List.of(), List.of()),
                    new Row(2, List.of(List.of()), List.of(List.of())),
                    new Row(3, List.of(List.of(1), List.of(2, 3)),
                            List.of(Arrays.asList("x", null), List.of(""))),
                    new Row(4, List.of(List.of(Integer.MIN_VALUE, Integer.MAX_VALUE),
                                    List.of(), List.of(0)),
                            List.of(Arrays.asList((String) null))));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            // Object.class typed reads: the driver's java.sql.Array conversion
            // fails on doubly-nested arrays; the raw ArrayValue carrier is
            // handled by normalizeOfficial.
            assertRowsMatch("nested arrays official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id",
                            null, Object.class, Object.class));
        });
    }
}
