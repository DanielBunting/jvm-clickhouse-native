package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ConcurrentConnectionUseException;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.util.Iterator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the lazy, {@code Spliterator}-backed {@code query(sql, Class)}
 * mapped-stream path (improvement 05): maps a large result set without eagerly
 * materializing a {@code List}, and the connection is reusable for the next query once
 * the stream is closed/exhausted (the guard is released via the stream's {@code onClose}).
 */
@Tag("integration")
class MappedStreamIT extends IntegrationTestBase {

    /**
     * Mapped row type. Component names match the selected columns ({@code n}).
     *
     * @param n the row value, from column {@code n} ({@code UInt64})
     */
    public record NumRow(long n) {}

    private ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    @Test
    void mapsManyRowsLazilyAndConnectionIsReusableAfterClose() {
        int rowCount = 1_000_000;
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            // Exhaust the stream: sum 0..rowCount-1 and verify against the count.
            long sum;
            try (Stream<NumRow> stream =
                    conn.query("SELECT number AS n FROM numbers(" + rowCount + ")", NumRow.class)) {
                sum = stream.mapToLong(NumRow::n).sum();
            }
            long expected = (long) rowCount * (rowCount - 1) / 2;
            assertEquals(expected, sum, "lazy mapped stream must yield every row exactly once");

            // The connection is free again after the stream closed: a follow-up query works.
            assertEquals(rowCount, conn.executeScalar("SELECT count() FROM numbers(" + rowCount + ")"),
                    "connection must be reusable after the mapped stream is closed");
        }
    }

    @Test
    void closedButUnconsumedStreamReleasesGuardAndConnectionIsReusable() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            // Open a mapped stream and close it WITHOUT consuming it.
            Stream<NumRow> stream = conn.query("SELECT number AS n FROM numbers(100000)", NumRow.class);
            stream.close();

            // Guard released -> the connection serves the next query.
            assertEquals(100000L, conn.executeScalar("SELECT count() FROM numbers(100000)"),
                    "closing an unconsumed mapped stream must release the guard");
        }
    }

    @Test
    void midStreamConcurrentUseFailsFastThenRecoversAfterClose() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            Stream<NumRow> stream = conn.query("SELECT number AS n FROM numbers(100000)", NumRow.class);
            Iterator<NumRow> it = stream.iterator();
            assertTrue(it.hasNext());
            it.next(); // mid-stream: connection is in use, guard held

            assertThrows(ConcurrentConnectionUseException.class,
                    () -> conn.executeScalar("SELECT 1"));

            stream.close();
            // After close the connection recovers (no half-consumed packet state).
            assertEquals(1L, conn.executeScalar("SELECT 1"),
                    "connection must recover after the mapped stream is closed");
        }
    }
}
