package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips {@code Date}, {@code DateTime} and {@code DateTime64} through a
 * real server in both directions — DECODE (raw {@code INSERT ... VALUES} with
 * string literals) and ENCODE (bulk insert of a mapped record with
 * {@link LocalDate} / {@link Instant} fields) — plus the
 * {@code query(sql, Class)} mapped-read path.
 *
 * <p>Codec return types: {@code Date} decodes to {@link LocalDate},
 * {@code DateTime}/{@code DateTime64} decode to {@link Instant}, so assertions
 * compare those objects directly with {@code .equals}. Every {@code DateTime} /
 * {@code DateTime64} column pins an explicit timezone:
 * <ul>
 *   <li>{@code 'UTC'} columns make the decoded {@link Instant} deterministic
 *       regardless of the container session timezone.</li>
 *   <li>One {@code DateTime('Europe/Berlin')} column proves the column timezone
 *       is honored: a wall-clock literal {@code 2024-03-15 12:30:00} (Berlin is
 *       UTC+1 in March) must decode to the absolute instant {@code 11:30:00Z}.</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class TemporalTypesIT extends TypeRoundTripBase {

    // ----------------------------------------------------------------- Date

    record DateRow(int id, LocalDate d) {}

    /** Date: epoch, a normal value, and a value near the Date max (2149-06-06). */
    @Test
    void dateRoundTrip() {
        LocalDate epoch  = LocalDate.parse("1970-01-01");
        LocalDate normal = LocalDate.parse("2024-03-15");
        LocalDate nearMax = LocalDate.parse("2149-06-06");
        List<DateRow> input = List.of(
                new DateRow(1, epoch),
                new DateRow(2, normal),
                new DateRow(3, nearMax));

        // DECODE: literal INSERT, client decodes to LocalDate.
        withTable("date_dec", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  d  Date"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, '1970-01-01'), (2, '2024-03-15'), (3, '2149-06-06')");

            List<Object[]> rows = decode(conn, "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 Date rows");
            assertDate(rows.get(0)[0], epoch);
            assertDate(rows.get(1)[0], normal);
            assertDate(rows.get(2)[0], nearMax);
        });

        // ENCODE + MAPPED-READ.
        withTable("date_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  d  Date"
                    + ") ENGINE = MergeTree() ORDER BY id");
            try (BulkInserter<DateRow> inserter =
                    conn.createBulkInserter(table, DateRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk Date rows");
            assertDate(rows.get(0)[0], epoch);
            assertDate(rows.get(1)[0], normal);
            assertDate(rows.get(2)[0], nearMax);

            List<DateRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, d FROM " + table + " ORDER BY id", DateRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, "Mapped Date records must equal the inserted records");
        });
    }

    // ------------------------------------------------------------- DateTime

    record DateTimeRow(int id, Instant dt) {}

    /**
     * DateTime('UTC'): epoch, a value beyond 2^31 seconds (2038-01-19 03:14:08),
     * and a normal value. Decoded as {@link Instant}.
     */
    @Test
    void dateTimeUtcRoundTrip() {
        Instant epoch   = Instant.parse("1970-01-01T00:00:00Z");
        Instant past2038 = Instant.parse("2038-01-19T03:14:08Z"); // > 2^31 - 1 seconds
        Instant normal  = Instant.parse("2024-03-15T12:30:45Z");
        List<DateTimeRow> input = List.of(
                new DateTimeRow(1, epoch),
                new DateTimeRow(2, past2038),
                new DateTimeRow(3, normal));

        // DECODE.
        withTable("dt_dec", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  dt DateTime('UTC')"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, dt) VALUES"
                    + " (1, '1970-01-01 00:00:00'),"
                    + " (2, '2038-01-19 03:14:08'),"
                    + " (3, '2024-03-15 12:30:45')");

            List<Object[]> rows = decode(conn, "SELECT dt FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 DateTime rows");
            assertInstant(rows.get(0)[0], epoch);
            assertInstant(rows.get(1)[0], past2038);
            assertInstant(rows.get(2)[0], normal);
        });

        // ENCODE + MAPPED-READ.
        withTable("dt_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  dt DateTime('UTC')"
                    + ") ENGINE = MergeTree() ORDER BY id");
            try (BulkInserter<DateTimeRow> inserter =
                    conn.createBulkInserter(table, DateTimeRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT dt FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk DateTime rows");
            assertInstant(rows.get(0)[0], epoch);
            assertInstant(rows.get(1)[0], past2038);
            assertInstant(rows.get(2)[0], normal);

            List<DateTimeRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, dt FROM " + table + " ORDER BY id", DateTimeRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, "Mapped DateTime records must equal the inserted records");
        });
    }

    /**
     * DateTime('Europe/Berlin'): a wall-clock literal must decode to the correct
     * ABSOLUTE instant. Berlin is UTC+1 in March, so {@code 2024-03-15 12:30:00}
     * local is {@code 11:30:00Z} — proving the column timezone is honored and not
     * silently treated as UTC.
     */
    @Test
    void dateTimeBerlinTimezoneIsHonored() {
        Instant expected = Instant.parse("2024-03-15T11:30:00Z"); // 12:30 Berlin (UTC+1)
        withTable("dt_tz", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  dt DateTime('Europe/Berlin')"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, dt) VALUES"
                    + " (1, '2024-03-15 12:30:00')");

            List<Object[]> rows = decode(conn, "SELECT dt FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), "Expected 1 row");
            assertInstant(rows.get(0)[0], expected);
        });
    }

    // ----------------------------------------------------------- DateTime64

    record DateTime64Row(int id, Instant ts) {}

    /**
     * DateTime64(p,'UTC') for p in {0,3,6,9}: each value carries sub-second
     * precision matching the column scale, asserted as {@link Instant} equality.
     */
    @Test
    void dateTime64RoundTrip() {
        roundTripDt64(0, "2024-03-15 12:30:45",            Instant.parse("2024-03-15T12:30:45Z"));
        roundTripDt64(3, "2024-03-15 12:30:45.123",        Instant.parse("2024-03-15T12:30:45.123Z"));
        roundTripDt64(6, "2024-03-15 12:30:45.123456",     Instant.parse("2024-03-15T12:30:45.123456Z"));
        roundTripDt64(9, "2024-03-15 12:30:45.123456789",  Instant.parse("2024-03-15T12:30:45.123456789Z"));
    }

    private void roundTripDt64(int precision, String literal, Instant expected) {
        String chType = "DateTime64(" + precision + ", 'UTC')";

        // DECODE.
        withTable("dt64p" + precision + "_dec", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  ts " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, ts) VALUES (1, '" + literal + "')");

            List<Object[]> rows = decode(conn, "SELECT ts FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), chType + ": expected 1 row");
            assertInstant(rows.get(0)[0], expected);
        });

        // ENCODE + MAPPED-READ.
        withTable("dt64p" + precision + "_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  ts " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");
            List<DateTime64Row> input = List.of(new DateTime64Row(1, expected));
            try (BulkInserter<DateTime64Row> inserter =
                    conn.createBulkInserter(table, DateTime64Row.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT ts FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), chType + " encode: expected 1 row");
            assertInstant(rows.get(0)[0], expected);

            List<DateTime64Row> mapped;
            try (var stream = conn.query(
                    "SELECT id, ts FROM " + table + " ORDER BY id", DateTime64Row.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, chType + ": mapped records must equal inserted records");
        });
    }

    // --------------------------------------------------------------- helpers

    private static void assertDate(Object actual, LocalDate expected) {
        assertInstanceOf(LocalDate.class, actual,
                "Date must decode to LocalDate, got "
                + (actual == null ? "null" : actual.getClass().getName()));
        assertEquals(expected, actual, "Date value");
    }

    private static void assertInstant(Object actual, Instant expected) {
        assertInstanceOf(Instant.class, actual,
                "DateTime/DateTime64 must decode to Instant, got "
                + (actual == null ? "null" : actual.getClass().getName()));
        assertEquals(expected, actual, "Instant value");
    }
}
