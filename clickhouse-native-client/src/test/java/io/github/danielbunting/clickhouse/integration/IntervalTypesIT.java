package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Period;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Decodes ClickHouse {@code Interval*} result columns through a real server.
 *
 * <p><b>Storability.</b> {@code Interval*} is usually treated as a transient
 * expression type, but on 25.6 it is in fact storable: {@code CREATE TABLE
 * (i IntervalDay) ENGINE = MergeTree}, {@code INSERT ... VALUES (toIntervalDay(5))},
 * and {@code SELECT i} all succeed, and {@code toTypeName(i)} reports
 * {@code IntervalDay}. Both paths are exercised below — an expression result column
 * ({@code SELECT toIntervalDay(5)}) and a stored {@code IntervalDay} column. In every
 * case the server emits the interval as a fixed 8-byte {@code Int64} count of the
 * unit, which the client decodes into a {@link java.time.Duration} (fixed-length
 * units) or {@link java.time.Period} (calendar units: Month/Quarter/Year).
 *
 * <p>Covers a representative set spanning both Java mappings: Second / Day (Duration)
 * and Month / Year (Period).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class IntervalTypesIT extends TypeRoundTripBase {

    /** Returns the single value of the single returned row (column index 0). */
    private Object singleCell(List<Object[]> rows, String label) {
        assertEquals(1, rows.size(), label + ": expected exactly 1 row");
        return rows.get(0)[0];
    }

    /** {@code IntervalSecond} (fixed-length) decodes to a {@link Duration}. */
    @Test
    void intervalSecondDecodesToDuration() {
        try (var conn = io.github.danielbunting.clickhouse.ClickHouseConnection.open(config())) {
            Object cell = singleCell(
                    decode(conn, "SELECT toIntervalSecond(45) AS i"), "IntervalSecond");
            Duration d = assertInstanceOf(Duration.class, cell,
                    "IntervalSecond must decode to a Duration");
            assertEquals(Duration.ofSeconds(45), d, "IntervalSecond(45)");
        }
    }

    /** {@code IntervalDay} (fixed-length) decodes to a {@link Duration} of N days. */
    @Test
    void intervalDayDecodesToDuration() {
        try (var conn = io.github.danielbunting.clickhouse.ClickHouseConnection.open(config())) {
            Object cell = singleCell(
                    decode(conn, "SELECT INTERVAL 5 DAY AS i"), "IntervalDay");
            Duration d = assertInstanceOf(Duration.class, cell,
                    "IntervalDay must decode to a Duration");
            assertEquals(Duration.ofDays(5), d, "INTERVAL 5 DAY");
        }
    }

    /** {@code IntervalMonth} (calendar) decodes to a {@link Period} of N months. */
    @Test
    void intervalMonthDecodesToPeriod() {
        try (var conn = io.github.danielbunting.clickhouse.ClickHouseConnection.open(config())) {
            Object cell = singleCell(
                    decode(conn, "SELECT toIntervalMonth(7) AS i"), "IntervalMonth");
            Period p = assertInstanceOf(Period.class, cell,
                    "IntervalMonth must decode to a Period");
            assertEquals(7, p.toTotalMonths(), "IntervalMonth(7) total months");
        }
    }

    /** {@code IntervalYear} (calendar) decodes to a {@link Period} of N years. */
    @Test
    void intervalYearDecodesToPeriod() {
        try (var conn = io.github.danielbunting.clickhouse.ClickHouseConnection.open(config())) {
            Object cell = singleCell(
                    decode(conn, "SELECT toIntervalYear(3) AS i"), "IntervalYear");
            Period p = assertInstanceOf(Period.class, cell,
                    "IntervalYear must decode to a Period");
            assertEquals(Period.ofYears(3), p, "IntervalYear(3)");
        }
    }

    /**
     * STORED COLUMN: {@code Interval*} is storable on 25.6. Create a MergeTree table
     * with an {@code IntervalDay} column, insert {@code toIntervalDay(5)}, and decode
     * it back to {@code Duration.ofDays(5)} through the table read path.
     */
    @Test
    void intervalDayStoredColumnDecodes() {
        withTable("itv_day", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (i IntervalDay) ENGINE = MergeTree() ORDER BY tuple()");
            conn.execute("INSERT INTO " + table + " VALUES (toIntervalDay(5))");

            Object cell = singleCell(
                    decode(conn, "SELECT i FROM " + table), "stored IntervalDay");
            Duration d = assertInstanceOf(Duration.class, cell,
                    "stored IntervalDay must decode to a Duration");
            assertEquals(Duration.ofDays(5), d, "stored IntervalDay(5)");
        });
    }
}
