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
}
