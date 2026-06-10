package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips the ClickHouse {@code Nested(...)} type against a real 25.6 server.
 *
 * <p><b>Empirical wire facts (verified on 25.6).</b> A {@code Nested(a T1, b T2)} column is
 * stored FLATTENED — one {@code Array(T)} sub-column per field. How it is announced over the
 * Native protocol depends on how you SELECT it:
 * <ul>
 *   <li>{@code SELECT n}        -> a single column of type {@code Array(Tuple(a UInt32, b String))}
 *       (the server re-assembles the flattened sub-columns into an array-of-tuples). This is
 *       structurally an {@code Array(Tuple(...))} and decodes through the existing
 *       Array + Tuple codecs to {@code List<List<Object>>} (one tuple {@code [a, b]} per
 *       inner element).</li>
 *   <li>{@code SELECT n.a, n.b} -> two columns of type {@code Array(UInt32)} and
 *       {@code Array(String)} — the flattened sub-columns, each already supported.</li>
 * </ul>
 *
 * <p>So nothing in the Nested path is "new" on the wire: the whole-column form is
 * {@code Array(Tuple(...))} and the flattened form is {@code Array(T)}, both already handled.
 * The parser additionally maps a literal {@code Nested(...)} type string to
 * {@code Array(Tuple(...))} for completeness (the server is not observed to emit that string
 * for SELECT, but the mapping is structurally exact). These tests assert both reachable
 * forms round-trip.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class NestedTypesIT extends TypeRoundTripBase {

    /**
     * DECODE the whole Nested column: {@code SELECT n} is reported as
     * {@code Array(Tuple(a UInt32, b String))} and decodes to a {@code List} of 2-element
     * tuples {@code [a, b]} per table row.
     */
    @Test
    void nestedWholeColumnDecodeRoundTrips() {
        withTable("nested_whole", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, n Nested(a UInt32, b String))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, n.a, n.b) VALUES"
                    + " (1, [10, 20], ['x', 'y']), (2, [30], ['z'])");

            List<Object[]> rows = decode(conn, "SELECT n FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            // Row 1: two nested entries [10,'x'] and [20,'y'].
            List<?> n1 = assertInstanceOf(List.class, rows.get(0)[0], "Row 1 Nested");
            assertEquals(2, n1.size(), "Row 1 entry count");
            assertTuple(n1.get(0), 10L, "x", "Row 1 entry 0");
            assertTuple(n1.get(1), 20L, "y", "Row 1 entry 1");

            // Row 2: one nested entry [30,'z'].
            List<?> n2 = assertInstanceOf(List.class, rows.get(1)[0], "Row 2 Nested");
            assertEquals(1, n2.size(), "Row 2 entry count");
            assertTuple(n2.get(0), 30L, "z", "Row 2 entry 0");
        });
    }

    /**
     * DECODE the flattened sub-columns: {@code SELECT n.a, n.b} yields {@code Array(UInt32)}
     * and {@code Array(String)} columns — the on-disk representation of a Nested column.
     */
    @Test
    void nestedFlattenedSubColumnsDecodeRoundTrip() {
        withTable("nested_flat", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, n Nested(a UInt32, b String))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, n.a, n.b) VALUES"
                    + " (1, [10, 20], ['x', 'y']), (2, [30], ['z'])");

            List<Object[]> rows = decode(conn,
                    "SELECT n.a, n.b FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");

            // Row 1: n.a = [10, 20], n.b = ['x', 'y'].
            assertLongArray(rows.get(0)[0], List.of(10L, 20L), "Row 1 n.a");
            assertEquals(List.of("x", "y"), stringList(rows.get(0)[1]), "Row 1 n.b");

            // Row 2: n.a = [30], n.b = ['z'].
            assertLongArray(rows.get(1)[0], List.of(30L), "Row 2 n.a");
            assertEquals(List.of("z"), stringList(rows.get(1)[1]), "Row 2 n.b");
        });
    }

    // --- helpers -----------------------------------------------------------

    private void assertTuple(Object cell, long a, String b, String label) {
        List<?> t = assertInstanceOf(List.class, cell, label + ": expected a Tuple List");
        assertEquals(2, t.size(), label + ": tuple arity");
        assertEquals(a, ((Number) t.get(0)).longValue(), label + " a");
        assertEquals(b, t.get(1), label + " b");
    }

    private void assertLongArray(Object cell, List<Long> expected, String label) {
        List<?> arr = assertInstanceOf(List.class, cell, label + ": expected an Array List");
        List<Long> got = new ArrayList<>(arr.size());
        for (Object o : arr) {
            got.add(((Number) o).longValue());
        }
        assertEquals(expected, got, label);
    }

    private List<String> stringList(Object cell) {
        List<?> arr = assertInstanceOf(List.class, cell, "expected an Array List");
        List<String> got = new ArrayList<>(arr.size());
        for (Object o : arr) {
            got.add(o == null ? null : o.toString());
        }
        return got;
    }
}
