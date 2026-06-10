package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import io.github.danielbunting.clickhouse.types.Column;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Integration tests for the recursive {@code Array(T)} codec and {@code Nullable(T)}
 * null-map handling against a real ClickHouse server.
 *
 * <p>Every test INSERTs known literal rows (or bulk-inserts a matching Java record),
 * SELECTs them back through {@link QueryResult}, and asserts the returned
 * {@link java.util.List} structure and element values are exactly equal to the input.
 * Array values must come back as nested {@code List}s; null cells must decode to
 * {@code null}.
 *
 * <p>Coverage:
 * <ul>
 *   <li>{@code Array(UInt32)} — flat numeric array, exercises the offsets section.</li>
 *   <li>{@code Array(String)} — flat variable-width array.</li>
 *   <li>{@code Array(Nullable(String))} — inner null-map inside an array.</li>
 *   <li>{@code Nullable(Int64)} — both null and non-null rows.</li>
 *   <li>{@code Array(Array(UInt32))} — nested array, exercises the recursive codec.</li>
 * </ul>
 *
 * <p>Assertions use round-trip equality (insert value == selected value) rather than
 * hand-computed constants, and use {@link Number#longValue()} so they hold regardless
 * of whether UInt32 boxes as {@code Integer} or {@code Long}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class ArrayAndNullableIT extends IntegrationTestBase {

    /**
     * Builds a default config pointing at the test container.
     *
     * @return a fresh {@link ClickHouseConfig}
     */
    private ClickHouseConfig config() {
        return ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .build();
    }

    /**
     * Reads all blocks from a {@link QueryResult} and materialises every
     * {@code (column, row)} value into a row-major {@code List<Object[]>}.
     *
     * <p>Values are read through {@link Column#value(int)}, which is null-aware:
     * a null cell (per the column's null-map) yields {@code null}; an array cell
     * yields a {@link java.util.List}.
     *
     * @param result the query result (iterated by this method; caller closes it)
     * @return list of rows, each a {@code Object[]} of boxed/null values
     */
    private List<Object[]> materialize(QueryResult result) {
        List<Object[]> rows = new ArrayList<>();
        Iterator<Block> blocks = result.blocks();
        while (blocks.hasNext()) {
            Block block = blocks.next();
            if (block.isEmpty()) {
                continue;
            }
            int colCount = block.columnCount();
            int rowCount = block.rowCount();
            for (int r = 0; r < rowCount; r++) {
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    row[c] = block.column(c).value(r);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Asserts that an array cell is a {@link List} whose elements equal the
     * expected longs (read via {@link Number#longValue()} so the test is
     * independent of the inner box type).
     *
     * @param cell     the materialised cell value (expected to be a {@code List})
     * @param expected the expected element values, in order
     * @param label    a human-readable label for failure messages
     */
    private void assertLongArray(Object cell, long[] expected, String label) {
        List<?> list = assertInstanceOf(List.class, cell,
                label + ": expected a java.util.List (Array codec) but got "
                + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(expected.length, list.size(),
                label + ": array length mismatch — offsets section decoded wrong slice");
        for (int i = 0; i < expected.length; i++) {
            Object e = list.get(i);
            assertEquals(expected[i], ((Number) e).longValue(),
                    label + "[" + i + "]: element value mismatch");
        }
    }

    /**
     * Round-trips an {@code Array(UInt32)} column: empty array, single element,
     * and a multi-element array. Confirms the offsets section slices the flat
     * value section back into one {@code List} per row.
     */
    @Test
    void arrayUInt32RoundTrips() {
        String table = "arr_u32_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  arr Array(UInt32)"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, arr) VALUES"
                + " (1, []),"
                + " (2, [42]),"
                + " (3, [1, 2, 3, 4294967295])");

            try (QueryResult result = conn.query(
                    "SELECT id, arr FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);
                assertEquals(3, rows.size(), "Expected 3 rows from " + table);

                assertLongArray(rows.get(0)[1], new long[] {}, "Row 1 Array(UInt32) empty");
                assertLongArray(rows.get(1)[1], new long[] {42}, "Row 2 Array(UInt32) single");
                assertLongArray(rows.get(2)[1], new long[] {1, 2, 3, 4_294_967_295L},
                        "Row 3 Array(UInt32) with UInt32 max");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips an {@code Array(String)} column: empty array, and arrays with
     * empty/ASCII/UTF-8 elements. Exercises variable-width inner values inside
     * the array value section.
     */
    @Test
    void arrayStringRoundTrips() {
        String table = "arr_str_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  arr Array(String)"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, arr) VALUES"
                + " (1, []),"
                + " (2, ['a']),"
                + " (3, ['', 'hello', 'unicode: éàü'])");

            try (QueryResult result = conn.query(
                    "SELECT id, arr FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);
                assertEquals(3, rows.size(), "Expected 3 rows from " + table);

                assertEquals(List.of(), rows.get(0)[1],
                        "Row 1 Array(String): expected empty list");
                assertEquals(List.of("a"), rows.get(1)[1],
                        "Row 2 Array(String): expected ['a']");
                assertEquals(Arrays.asList("", "hello", "unicode: éàü"), rows.get(2)[1],
                        "Row 3 Array(String): empty + ASCII + UTF-8 elements must round-trip");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips an {@code Array(Nullable(String))} column, where the null-map
     * lives on the inner element type. A null element must decode to {@code null}
     * inside the returned {@code List}, while adjacent non-null elements remain
     * intact.
     */
    @Test
    void arrayNullableStringRoundTrips() {
        String table = "arr_nstr_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  arr Array(Nullable(String))"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, arr) VALUES"
                + " (1, ['x', NULL, 'y']),"
                + " (2, [NULL, NULL]),"
                + " (3, ['only'])");

            try (QueryResult result = conn.query(
                    "SELECT id, arr FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);
                assertEquals(3, rows.size(), "Expected 3 rows from " + table);

                assertEquals(Arrays.asList("x", null, "y"), rows.get(0)[1],
                        "Row 1 Array(Nullable(String)): inner null-map bit not honoured");
                assertEquals(Arrays.asList(null, null), rows.get(1)[1],
                        "Row 2 Array(Nullable(String)): all-null inner elements");
                assertEquals(List.of("only"), rows.get(2)[1],
                        "Row 3 Array(Nullable(String)): single non-null element");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a {@code Nullable(Int64)} column with both null and non-null
     * rows, including the Int64 extremes. A null-map bug would surface as a
     * non-null value reading as null (or vice versa).
     */
    @Test
    void nullableInt64RoundTrips() {
        String table = "nullable_i64_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id UInt32,"
                + "  n  Nullable(Int64)"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, n) VALUES"
                + " (1, 0),"
                + " (2, NULL),"
                + " (3, 9223372036854775807),"
                + " (4, -9223372036854775808),"
                + " (5, NULL)");

            try (QueryResult result = conn.query(
                    "SELECT id, n FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);
                assertEquals(5, rows.size(), "Expected 5 rows from " + table);

                assertEquals(0L, ((Number) rows.get(0)[1]).longValue(),
                        "Row 1 Nullable(Int64): expected 0");
                assertNull(rows.get(1)[1],
                        "Row 2 Nullable(Int64): expected null — null-map bit not read");
                assertEquals(Long.MAX_VALUE, ((Number) rows.get(2)[1]).longValue(),
                        "Row 3 Nullable(Int64): expected Long.MAX_VALUE");
                assertEquals(Long.MIN_VALUE, ((Number) rows.get(3)[1]).longValue(),
                        "Row 4 Nullable(Int64): expected Long.MIN_VALUE");
                assertNull(rows.get(4)[1],
                        "Row 5 Nullable(Int64): expected null");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a nested {@code Array(Array(UInt32))} column. Each row decodes
     * to a {@code List<List<Long>>}; this exercises the recursive offsets/value
     * splitting where the inner codec is itself an array codec.
     */
    @Test
    void nestedArrayUInt32RoundTrips() {
        String table = "arr_arr_u32_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  arr Array(Array(UInt32))"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, arr) VALUES"
                + " (1, []),"
                + " (2, [[]]),"
                + " (3, [[1], [2, 3]]),"
                + " (4, [[10, 20], [], [30]])");

            try (QueryResult result = conn.query(
                    "SELECT id, arr FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);
                assertEquals(4, rows.size(), "Expected 4 rows from " + table);

                // Row 1: outer array empty.
                assertOuterArray(rows.get(0)[1], new long[][] {}, "Row 1 Array(Array(UInt32)) empty outer");
                // Row 2: one empty inner array.
                assertOuterArray(rows.get(1)[1], new long[][] {{}}, "Row 2 nested [[]]");
                // Row 3: [[1],[2,3]]
                assertOuterArray(rows.get(2)[1], new long[][] {{1}, {2, 3}}, "Row 3 nested [[1],[2,3]]");
                // Row 4: [[10,20],[],[30]]
                assertOuterArray(rows.get(3)[1], new long[][] {{10, 20}, {}, {30}},
                        "Row 4 nested [[10,20],[],[30]]");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Asserts a nested-array cell is a {@code List<List<...>>} matching the
     * expected ragged 2-D long matrix.
     *
     * @param cell     the materialised cell (expected outer {@code List})
     * @param expected expected inner arrays, in order
     * @param label    label for failure messages
     */
    private void assertOuterArray(Object cell, long[][] expected, String label) {
        List<?> outer = assertInstanceOf(List.class, cell,
                label + ": expected outer java.util.List but got "
                + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(expected.length, outer.size(),
                label + ": outer array length mismatch — recursive offsets decoded wrong");
        for (int i = 0; i < expected.length; i++) {
            assertLongArray(outer.get(i), expected[i], label + " inner[" + i + "]");
        }
    }

    /**
     * Record whose component names match the {@code Array(UInt32)} table columns,
     * used to exercise the WRITE/encode path through {@link BulkInserter}.
     *
     * @param id  the row id (UInt32)
     * @param arr the array payload (Array(UInt32)); supplied as {@code List<Long>}
     */
    record ArrRow(long id, List<Long> arr) {}

    /**
     * Exercises the array WRITE path: bulk-inserts {@code Array(UInt32)} rows
     * defined as a Java record (field {@code arr} is a {@code List<Long>}), then
     * SELECTs them back and asserts the list structure round-trips. Confirms the
     * recursive {@code Array} encode (offsets + flattened values) matches what
     * the server decodes.
     */
    @Test
    void arrayUInt32BulkInsertRoundTrips() {
        String table = "arr_u32_bulk_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  arr Array(UInt32)"
                + ") ENGINE = MergeTree() ORDER BY id");

            List<ArrRow> input = List.of(
                    new ArrRow(1, List.of()),
                    new ArrRow(2, List.of(7L)),
                    new ArrRow(3, List.of(100L, 200L, 4_294_967_295L)));

            try (BulkInserter<ArrRow> inserter =
                    conn.createBulkInserter(table, ArrRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            try (QueryResult result = conn.query(
                    "SELECT id, arr FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);
                assertEquals(3, rows.size(), "Expected 3 bulk-inserted rows from " + table);

                assertLongArray(rows.get(0)[1], new long[] {},
                        "Bulk row 1 Array(UInt32): empty array encode");
                assertLongArray(rows.get(1)[1], new long[] {7},
                        "Bulk row 2 Array(UInt32): single element encode");
                assertLongArray(rows.get(2)[1], new long[] {100, 200, 4_294_967_295L},
                        "Bulk row 3 Array(UInt32): multi element encode");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
