package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips the ClickHouse {@code Tuple(...)} codec against a real server in both
 * directions: DECODE (raw {@code INSERT ... VALUES}) and ENCODE (bulk insert of a
 * mapped record whose tuple field is a {@code List<Object>}).
 *
 * <p>A Tuple column decodes to a {@code java.util.List} of its element values, in
 * declaration order; numeric elements are asserted via {@link Number#longValue()} so
 * the test is independent of the inner box type.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class TupleTypesIT extends TypeRoundTripBase {

    /** Asserts a tuple cell is a List whose i-th element equals the expected value. */
    private void assertTuple(Object cell, List<Object> expected, String label) {
        List<?> list = assertInstanceOf(List.class, cell,
                label + ": expected a java.util.List (Tuple codec) but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(expected.size(), list.size(), label + ": tuple arity mismatch");
        for (int i = 0; i < expected.size(); i++) {
            Object exp = expected.get(i);
            Object act = list.get(i);
            if (exp instanceof Number n) {
                assertEquals(n.longValue(), ((Number) act).longValue(),
                        label + " element[" + i + "]");
            } else {
                assertEquals(exp, act, label + " element[" + i + "]");
            }
        }
    }

    /**
     * DECODE: {@code Tuple(UInt32, String)} via raw VALUES; each cell must decode to a
     * 2-element List of [number, string].
     */
    @Test
    void tupleUInt32StringDecodeRoundTrips() {
        withTable("tuple_u32_str", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, t Tuple(UInt32, String)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES"
                    + " (1, (10, 'a')), (2, (20, 'b'))");

            List<Object[]> rows = decode(conn,
                    "SELECT id, t FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");
            assertTuple(rows.get(0)[1], Arrays.asList(10L, "a"), "Row 1");
            assertTuple(rows.get(1)[1], Arrays.asList(20L, "b"), "Row 2");
        });
    }

    /**
     * DECODE: a 3-element {@code Tuple(Int64, Float64, String)} — exercises mixed
     * element widths in a single tuple sub-column layout.
     */
    @Test
    void tupleThreeElementDecodeRoundTrips() {
        withTable("tuple_three", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, t Tuple(Int64, Float64, String)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES"
                    + " (1, (-5, 1.5, 'x')), (2, (9223372036854775807, 2.5, 'y'))");

            List<Object[]> rows = decode(conn,
                    "SELECT id, t FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            List<?> t0 = assertInstanceOf(List.class, rows.get(0)[1], "Row 1 tuple");
            assertEquals(-5L, ((Number) t0.get(0)).longValue(), "Row 1 Int64");
            assertEquals(1.5, ((Number) t0.get(1)).doubleValue(), 0.0, "Row 1 Float64");
            assertEquals("x", t0.get(2), "Row 1 String");

            List<?> t1 = assertInstanceOf(List.class, rows.get(1)[1], "Row 2 tuple");
            assertEquals(Long.MAX_VALUE, ((Number) t1.get(0)).longValue(), "Row 2 Int64");
            assertEquals(2.5, ((Number) t1.get(1)).doubleValue(), 0.0, "Row 2 Float64");
            assertEquals("y", t1.get(2), "Row 2 String");
        });
    }

    /**
     * DECODE: a NAMED tuple {@code Tuple(a UInt32, b String)} — field names must be
     * stripped during parsing; the wire layout is identical to the unnamed form.
     */
    @Test
    void namedTupleDecodeRoundTrips() {
        withTable("tuple_named", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, t Tuple(a UInt32, b String)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES"
                    + " (1, (100, 'foo')), (2, (200, 'bar'))");

            List<Object[]> rows = decode(conn,
                    "SELECT id, t FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");
            assertTuple(rows.get(0)[1], Arrays.asList(100L, "foo"), "Named row 1");
            assertTuple(rows.get(1)[1], Arrays.asList(200L, "bar"), "Named row 2");
        });
    }

    /**
     * A record whose {@code t} field is a {@code List<Object>}, exercising the ENCODE
     * path: the object mapper routes the List through {@code codec.set(List)}.
     *
     * @param id the row id (UInt32)
     * @param t  the tuple payload ({@code Tuple(UInt32, String)}) as a List
     */
    record TupleRow(long id, List<Object> t) {}

    /**
     * ENCODE: bulk-insert {@code Tuple(UInt32, String)} rows built as Java Lists, then
     * read them back and assert the tuple structure round-trips. Confirms the Tuple
     * encode (element sub-columns, no offsets) matches what the server decodes.
     */
    @Test
    void tupleBulkInsertRoundTrips() {
        withTable("tuple_bulk", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id UInt32, t Tuple(UInt32, String)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<TupleRow> input = List.of(
                    new TupleRow(1, Arrays.asList(11L, "alpha")),
                    new TupleRow(2, Arrays.asList(22L, "beta")),
                    new TupleRow(3, Arrays.asList(4_294_967_295L, "")));

            try (BulkInserter<TupleRow> inserter =
                    conn.createBulkInserter(table, TupleRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT id, t FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk-inserted rows");
            assertTuple(rows.get(0)[1], Arrays.asList(11L, "alpha"), "Bulk row 1");
            assertTuple(rows.get(1)[1], Arrays.asList(22L, "beta"), "Bulk row 2");
            assertTuple(rows.get(2)[1], Arrays.asList(4_294_967_295L, ""), "Bulk row 3");
        });
    }
}
