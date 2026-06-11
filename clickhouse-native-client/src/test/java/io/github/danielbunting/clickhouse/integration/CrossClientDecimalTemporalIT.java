package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Cross-client round-trips for Decimal (all four wire widths) and the temporal
 * types (Date, Date32, DateTime with timezone, DateTime64 at millisecond /
 * microsecond / nanosecond precision). These are the families with the highest
 * cross-client risk: backing-integer width and scale for decimals, tick
 * scaling, negative-tick floor division and timezone-as-metadata semantics for
 * DateTime64.
 *
 * <p>Official-client temporal reads go through the typed JDBC 4.2 accessor
 * ({@code getObject(i, OffsetDateTime.class)}) because the untyped accessor is
 * session-timezone ambiguous; inserts bind {@link ZonedDateTime} values in the
 * column's zone so the bound value is unambiguous regardless of how the driver
 * interprets it.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientDecimalTemporalIT extends CrossClientRoundTripBase {

    /** Decimal at all four wire widths: zero, negative, near-max magnitude. */
    @Test
    void decimalsAllWidths() {
        record Row(int id, BigDecimal d32, BigDecimal d64, BigDecimal d128, BigDecimal d256) {}
        String columns = "id, d32, d64, d128, d256";
        List<Object[]> expected = rowsOf(
                row(1L,
                        new BigDecimal("0.00"),
                        new BigDecimal("0.0000"),
                        new BigDecimal("0.00000000"),
                        new BigDecimal("0.00")),
                row(2L,
                        new BigDecimal("-12345.67"),
                        new BigDecimal("-271828.1828"),
                        new BigDecimal("-123456789012345678901234.56789012"),
                        new BigDecimal("-9876543210987654321098765432109876543210.12")),
                row(3L,
                        new BigDecimal("9999999.99"),
                        new BigDecimal("99999999999999.9999"),
                        new BigDecimal("999999999999999999999999999999.99999999"),
                        new BigDecimal(
                            "99999999999999999999999999999999999999999999999999999999999999999999999999.99")));

        withTable("xc_dec", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id   UInt32,"
                    + "  d32  Decimal(9, 2),"
                    + "  d64  Decimal(18, 4),"
                    + "  d128 Decimal(38, 8),"
                    + "  d256 Decimal(76, 2)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("decimals native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1,
                            new BigDecimal("0.00"),
                            new BigDecimal("0.0000"),
                            new BigDecimal("0.00000000"),
                            new BigDecimal("0.00")),
                    new Row(2,
                            new BigDecimal("-12345.67"),
                            new BigDecimal("-271828.1828"),
                            new BigDecimal("-123456789012345678901234.56789012"),
                            new BigDecimal("-9876543210987654321098765432109876543210.12")),
                    new Row(3,
                            new BigDecimal("9999999.99"),
                            new BigDecimal("99999999999999.9999"),
                            new BigDecimal("999999999999999999999999999999.99999999"),
                            new BigDecimal(
                                "99999999999999999999999999999999999999999999999999999999999999999999999999.99")));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("decimals official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Date (unsigned day count) and Date32 (signed day count) at their range edges. */
    @Test
    void dates() {
        record Row(int id, LocalDate d, LocalDate d32) {}
        String columns = "id, d, d32";
        List<Object[]> expected = rowsOf(
                row(1L, LocalDate.of(1970, 1, 1), LocalDate.of(1900, 1, 1)),
                row(2L, LocalDate.of(2024, 2, 29), LocalDate.of(1969, 12, 31)),
                row(3L, LocalDate.of(2149, 6, 6), LocalDate.of(2299, 12, 31)));

        withTable("xc_date", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  d   Date,"
                    + "  d32 Date32"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("dates native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, LocalDate.of(1970, 1, 1), LocalDate.of(1900, 1, 1)),
                    new Row(2, LocalDate.of(2024, 2, 29), LocalDate.of(1969, 12, 31)),
                    new Row(3, LocalDate.of(2149, 6, 6), LocalDate.of(2299, 12, 31)));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("dates official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id",
                            null, LocalDate.class, LocalDate.class));
        });
    }

    /**
     * DateTime with a non-UTC column timezone: the timezone is display
     * metadata only, so the same absolute {@link Instant} must round-trip
     * through both clients.
     */
    @Test
    void dateTimeWithTimezone() {
        record Row(int id, Instant dt) {}
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");
        Instant epoch = Instant.parse("1970-01-01T00:00:00Z");
        Instant billennium = Instant.parse("2001-09-09T01:46:40Z");
        Instant recent = Instant.parse("2024-06-30T23:59:59Z");
        List<Object[]> expected = rowsOf(
                row(1L, epoch),
                row(2L, billennium),
                row(3L, recent));

        withTable("xc_dt", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  dt DateTime('Asia/Tokyo')"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, dt", rowsOf(
                    row(1L, ZonedDateTime.ofInstant(epoch, tokyo)),
                    row(2L, ZonedDateTime.ofInstant(billennium, tokyo)),
                    row(3L, ZonedDateTime.ofInstant(recent, tokyo))));
            assertRowsMatch("DateTime(tz) native decode", expected,
                    decode(conn, "SELECT id, dt FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, epoch),
                    new Row(2, billennium),
                    new Row(3, recent));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("DateTime(tz) official read", expected,
                    officialSelect("SELECT id, dt FROM " + table + " ORDER BY id",
                            null, OffsetDateTime.class));
        });
    }

    /**
     * DateTime64 at precisions 3/6/9, mixing UTC, server-default and non-UTC
     * column timezones, with epoch, pre-epoch (negative ticks) and sub-second
     * edge values per precision.
     */
    @Test
    void dateTime64Precisions() {
        record Row(int id, Instant ms, Instant us, Instant ns) {}
        String columns = "id, ms, us, ns";
        ZoneId utc = ZoneId.of("UTC");
        ZoneId tokyo = ZoneId.of("Asia/Tokyo");

        Instant epoch = Instant.parse("1970-01-01T00:00:00Z");
        // Pre-epoch values exercise negative-tick floor division (in range for DateTime64).
        Instant preEpochMs = Instant.parse("1900-01-02T03:04:05.678Z");
        Instant preEpochUs = Instant.parse("1955-11-05T06:07:08.000009Z");
        Instant preEpochNs = Instant.parse("1969-12-31T23:59:59.999999999Z");
        // Sub-second edges at each precision.
        Instant edgeMs = Instant.parse("2024-01-01T00:00:00.001Z");
        Instant edgeUs = Instant.parse("2024-01-01T00:00:00.000001Z");
        Instant edgeNs = Instant.parse("2024-01-01T00:00:00.999999999Z");

        List<Object[]> expected = rowsOf(
                row(1L, epoch, epoch, epoch),
                row(2L, preEpochMs, preEpochUs, preEpochNs),
                row(3L, edgeMs, edgeUs, edgeNs));

        withTable("xc_dt64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  ms DateTime64(3, 'UTC'),"
                    + "  us DateTime64(6),"
                    + "  ns DateTime64(9, 'Asia/Tokyo')"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, rowsOf(
                    row(1L, ZonedDateTime.ofInstant(epoch, utc),
                            ZonedDateTime.ofInstant(epoch, utc),
                            ZonedDateTime.ofInstant(epoch, tokyo)),
                    row(2L, ZonedDateTime.ofInstant(preEpochMs, utc),
                            ZonedDateTime.ofInstant(preEpochUs, utc),
                            ZonedDateTime.ofInstant(preEpochNs, tokyo)),
                    row(3L, ZonedDateTime.ofInstant(edgeMs, utc),
                            ZonedDateTime.ofInstant(edgeUs, utc),
                            ZonedDateTime.ofInstant(edgeNs, tokyo))));
            assertRowsMatch("DateTime64 native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, epoch, epoch, epoch),
                    new Row(2, preEpochMs, preEpochUs, preEpochNs),
                    new Row(3, edgeMs, edgeUs, edgeNs));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("DateTime64 official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id",
                            null, OffsetDateTime.class, OffsetDateTime.class, OffsetDateTime.class));
        });
    }
}
