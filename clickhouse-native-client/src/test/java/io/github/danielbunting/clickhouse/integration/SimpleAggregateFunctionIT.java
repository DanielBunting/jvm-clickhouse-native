package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips a {@code SimpleAggregateFunction(sum, UInt64)} column against a real
 * server. On the wire this type is IDENTICAL to its underlying value type
 * ({@code UInt64} here), so the parser transparently resolves it to the underlying
 * codec and the column must decode as a plain {@code UInt64} (Long raw bits).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class SimpleAggregateFunctionIT extends TypeRoundTripBase {

    /**
     * DECODE: insert raw values into a {@code SimpleAggregateFunction(sum, UInt64)}
     * column under an AggregatingMergeTree; the values must read back as UInt64.
     */
    @Test
    void simpleAggregateFunctionUInt64DecodeRoundTrips() {
        withTable("saf_u64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " k UInt32, v SimpleAggregateFunction(sum, UInt64)"
                    + ") ENGINE = AggregatingMergeTree ORDER BY k");
            conn.execute("INSERT INTO " + table + " VALUES (1, 100), (2, 250)");

            List<Object[]> rows = decode(conn,
                    "SELECT v FROM " + table + " ORDER BY k");
            assertEquals(2, rows.size(), "Expected 2 rows");
            assertEquals(100L, ((Number) rows.get(0)[0]).longValue(),
                    "Row 1 SimpleAggregateFunction(sum, UInt64) must decode as UInt64 100");
            assertEquals(250L, ((Number) rows.get(1)[0]).longValue(),
                    "Row 2 SimpleAggregateFunction(sum, UInt64) must decode as UInt64 250");
        });
    }

    /**
     * SAF over further inner types (reference: client-v2 QueryTests
     * #testReadingSimpleAggregateFunction2): the wire shape is just the inner type, so
     * max/String, anyLast/Nullable(String) (incl. a NULL), and min/Date all decode via
     * their inner codecs.
     */
    @Test
    void simpleAggregateFunctionOverStringsDatesAndNullablesDecodes() {
        withTable("saf_mixed", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " k UInt32,"
                    + " s SimpleAggregateFunction(max, String),"
                    + " n SimpleAggregateFunction(anyLast, Nullable(String)),"
                    + " d SimpleAggregateFunction(min, Date)"
                    + ") ENGINE = AggregatingMergeTree ORDER BY k");
            conn.execute("INSERT INTO " + table + " VALUES"
                    + " (1, 'alpha', 'x', '2021-03-04'), (2, 'beta', NULL, '1970-01-01')");

            List<Object[]> rows = decode(conn,
                    "SELECT s, n, d FROM " + table + " ORDER BY k");
            assertEquals(2, rows.size());
            assertEquals("alpha", rows.get(0)[0]);
            assertEquals("x", rows.get(0)[1]);
            assertEquals(java.time.LocalDate.of(2021, 3, 4), rows.get(0)[2]);
            assertEquals("beta", rows.get(1)[0]);
            org.junit.jupiter.api.Assertions.assertNull(rows.get(1)[1],
                    "NULL survives SimpleAggregateFunction(anyLast, Nullable(String))");
            assertEquals(java.time.LocalDate.ofEpochDay(0), rows.get(1)[2]);
        });
    }

    /** Record for the SAF bulk-encode round trip (field {@code v} is the inner UInt64). */
    record SafRow(long k, long v) {}

    /**
     * ENCODE (reference: client-v2 writeSimpleAggregateFunctionTests): unlike raw
     * {@code AggregateFunction} states (which are rejected), a SAF column is stored as
     * its inner type, so the bulk-insert write path drives it directly.
     */
    @Test
    void simpleAggregateFunctionBulkEncodeRoundTrips() {
        withTable("saf_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " k UInt32, v SimpleAggregateFunction(sum, UInt64)"
                    + ") ENGINE = AggregatingMergeTree ORDER BY k");

            List<SafRow> input = List.of(new SafRow(1, 100), new SafRow(2, 250));
            try (var inserter = conn.createBulkInserter(table, SafRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY k");
            assertEquals(2, rows.size());
            assertEquals(100L, ((Number) rows.get(0)[0]).longValue());
            assertEquals(250L, ((Number) rows.get(1)[0]).longValue());
        });
    }
}
