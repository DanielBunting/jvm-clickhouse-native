package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips the experimental ClickHouse {@code Time} and {@code Time64(p)}
 * types through a real server in both directions — DECODE (raw
 * {@code INSERT ... VALUES} with time-string literals, server encodes) and
 * ENCODE (bulk insert of a mapped record with a {@link Duration} field).
 *
 * <p>Both types are gated server-side by {@code SET enable_time_time64_type = 1};
 * the flag is SET on the same connection that runs the CREATE / INSERT / SELECT.
 *
 * <p>Representation (validated empirically by these tests): {@code Time} is a
 * signed Int32 second-count within a day, so the literal {@code '12:30:45'}
 * decodes to {@code Duration.ofSeconds(45045)} = 12h30m45s. {@code Time64(p)} is a
 * signed Int64 tick-count where one tick is {@code 10^(-p)} seconds, so
 * {@code '12:30:45.123'} at precision 3 decodes to 45045s + 123ms.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class TimeTypesIT extends TypeRoundTripBase {

    private static final String GATE = "SET enable_time_time64_type = 1";

    // -------------------------------------------------------------------- Time

    record TimeRow(int id, Duration t) {}

    /** Time: midnight, a normal value, and the max documented value (999:59:59). */
    @Test
    void timeRoundTrip() {
        Duration midnight = Duration.ZERO;
        Duration normal   = Duration.ofHours(12).plusMinutes(30).plusSeconds(45); // 45045s
        Duration nearMax  = Duration.ofHours(999).plusMinutes(59).plusSeconds(59);
        List<TimeRow> input = List.of(
                new TimeRow(1, midnight),
                new TimeRow(2, normal),
                new TimeRow(3, nearMax));

        // DECODE: literal INSERT, client decodes to Duration.
        withTable("time_dec", (conn, table) -> {
            conn.execute(GATE);
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t  Time"
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES"
                    + " (1, '00:00:00'), (2, '12:30:45'), (3, '999:59:59')");

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 Time rows");
            assertDuration(rows.get(0)[0], midnight);
            assertDuration(rows.get(1)[0], normal);
            assertDuration(rows.get(2)[0], nearMax);
        });

        // ENCODE + MAPPED-READ.
        withTable("time_enc", (conn, table) -> {
            conn.execute(GATE);
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t  Time"
                    + ") ENGINE = MergeTree() ORDER BY id");
            try (BulkInserter<TimeRow> inserter =
                    conn.createBulkInserter(table, TimeRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk Time rows");
            assertDuration(rows.get(0)[0], midnight);
            assertDuration(rows.get(1)[0], normal);
            assertDuration(rows.get(2)[0], nearMax);

            List<TimeRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, t FROM " + table + " ORDER BY id", TimeRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, "Mapped Time records must equal the inserted records");
        });
    }

    // ------------------------------------------------------------------ Time64

    record Time64Row(int id, Duration t) {}

    /** Time64(p) for p in {3, 9}: sub-second precision matching the column scale. */
    @Test
    void time64RoundTrip() {
        roundTripTime64(3, "12:30:45.123",
                Duration.ofHours(12).plusMinutes(30).plusSeconds(45).plusMillis(123));
        roundTripTime64(9, "12:30:45.123456789",
                Duration.ofHours(12).plusMinutes(30).plusSeconds(45).plusNanos(123456789));
    }

    private void roundTripTime64(int precision, String literal, Duration expected) {
        String chType = "Time64(" + precision + ")";

        // DECODE.
        withTable("time64p" + precision + "_dec", (conn, table) -> {
            conn.execute(GATE);
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, t) VALUES (1, '" + literal + "')");

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), chType + ": expected 1 row");
            assertDuration(rows.get(0)[0], expected);
        });

        // ENCODE + MAPPED-READ.
        withTable("time64p" + precision + "_enc", (conn, table) -> {
            conn.execute(GATE);
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  t " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");
            List<Time64Row> input = List.of(new Time64Row(1, expected));
            try (BulkInserter<Time64Row> inserter =
                    conn.createBulkInserter(table, Time64Row.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT t FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), chType + " encode: expected 1 row");
            assertDuration(rows.get(0)[0], expected);

            List<Time64Row> mapped;
            try (var stream = conn.query(
                    "SELECT id, t FROM " + table + " ORDER BY id", Time64Row.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, chType + ": mapped records must equal inserted records");
        });
    }

    // --------------------------------------------------------------- helpers

    private static void assertDuration(Object actual, Duration expected) {
        assertInstanceOf(Duration.class, actual,
                "Time/Time64 must decode to Duration, got "
                + (actual == null ? "null" : actual.getClass().getName()));
        assertEquals(expected, actual, "Duration value");
    }
}
