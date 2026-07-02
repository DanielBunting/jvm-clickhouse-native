package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.codec.DateCodec;
import io.github.danielbunting.clickhouse.types.codec.DateTimeCodec;
import io.github.danielbunting.clickhouse.types.codec.DecimalCodec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.TimeCodec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

/**
 * Server-free unit tests for {@link ChResultSet} temporal/decimal/byte accessors and
 * their {@code wasNull} / Calendar-overload behaviour.
 */
class ChResultSetTemporalTest {

    @Test
    void getDate_fromLocalDate() throws SQLException {
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("d", "Date", new DateCodec(), LocalDate.of(2026, 5, 30)));
        assertTrue(rs.next());
        assertEquals(Date.valueOf("2026-05-30"), rs.getDate(1));
    }

    @Test
    void getTimestamp_fromInstant() throws SQLException {
        Instant when = Instant.parse("2026-05-30T13:45:07Z");
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("t", "DateTime", new DateTimeCodec(null), when));
        assertTrue(rs.next());
        assertEquals(Timestamp.from(when), rs.getTimestamp(1));
    }

    @Test
    void calendarOverloadsDelegate() throws SQLException {
        Instant when = Instant.parse("2026-05-30T13:45:07Z");
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("t", "DateTime", new DateTimeCodec(null), when));
        assertTrue(rs.next());
        // The Calendar is currently ignored; the overloads return the same value.
        assertEquals(rs.getTimestamp(1), rs.getTimestamp(1, Calendar.getInstance()));
    }

    /**
     * {@code sql.Time} read surface (reference: client-v2 DataTypeUtilsTests
     * {@code testToLocalTimeWithCalendar} / {@code testToLocalTimeWithTimeZoneObject},
     * where a {@code sql.Time} converts to a {@link java.time.LocalTime} under an
     * explicit Calendar/TimeZone).
     *
     * <p>DEVIATION: this driver has no {@code sql.Time}/{@code LocalTime} read path at
     * all — every {@code getTime} overload (plain, by-label, and both Calendar forms,
     * regardless of the Calendar's zone) throws {@link SQLFeatureNotSupportedException},
     * even on a genuine {@code Time} column. The supported surface for a Time column is
     * {@code getObject}, which returns the codec's {@link Duration} box (a
     * zone-less time-of-day count, so the reference's zone-shifting conversions have
     * nothing to act on). The write side is covered by
     * {@code ChPreparedStatementBindingTest#calendarOverloadsRenderIdenticallyToPlainSetters}.
     */
    @Test
    void getTime_unsupportedOnTimeColumn_durationIsTheContract() throws SQLException {
        Duration tod = Duration.ofHours(12).plusMinutes(34).plusSeconds(56);
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("t", "Time", new TimeCodec(), tod));
        assertTrue(rs.next());

        // The supported read: the raw Duration box.
        assertEquals(tod, rs.getObject(1));

        // All four getTime overloads throw, with any Calendar zone.
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Calendar shanghai = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getTime(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getTime("t"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getTime(1, utc));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getTime(1, shanghai));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getTime("t", utc));
    }

    /**
     * Zone-carrying temporal getters (reference: jdbc-v1
     * ClickHouseResultSetTest#testDateTimeWithoutTimezone). The driver boxes
     * DateTime/DateTime64 as an absolute {@link Instant}; {@code getObject(col, type)}
     * additionally derives zoned/local views AT UTC — the zone never shifts the stored
     * instant (a {@code session_timezone} only affects how the server interprets
     * wall-clock literals). Callers wanting another zone call
     * {@code withZoneSameInstant} themselves. No Calendar semantics.
     */
    @Test
    void getObject_zonedTemporalTypes_deriveUtcViewsFromTheInstant() throws SQLException {
        Instant when = Instant.parse("2026-05-30T13:45:07Z");
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("t", "DateTime", new DateTimeCodec(null), when));
        assertTrue(rs.next());

        // The core surface: raw Instant, Instant passthrough, Timestamp coercion.
        assertEquals(when, rs.getObject(1));
        assertEquals(when, rs.getObject(1, Instant.class));
        assertEquals(Timestamp.from(when), rs.getObject(1, Timestamp.class));

        // Zoned/local views derived at UTC — same instant, explicit zone.
        assertEquals(when.atZone(java.time.ZoneOffset.UTC),
                rs.getObject(1, java.time.ZonedDateTime.class));
        assertEquals(when.atOffset(java.time.ZoneOffset.UTC),
                rs.getObject(1, java.time.OffsetDateTime.class));
        assertEquals(java.time.LocalDateTime.ofInstant(when, java.time.ZoneOffset.UTC),
                rs.getObject(1, java.time.LocalDateTime.class));
        assertEquals(when, rs.getObject(1, java.time.ZonedDateTime.class).toInstant(),
                "the zoned view preserves the absolute instant exactly");
    }

    @Test
    void getBigDecimalWithScale() throws SQLException {
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("d", "Decimal(10, 2)", new DecimalCodec(10, 2), new BigDecimal("12.50")));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("12.5"), rs.getBigDecimal(1, 1));
    }

    @Test
    void getBytes_fromStringAndFailure() throws SQLException {
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("s", "String", new StringColumnCodec(), "héllo"),
                RsFixtures.complexCol("n", "Int32", new Int32Codec(), 5));
        assertTrue(rs.next());
        assertArrayEquals("héllo".getBytes(StandardCharsets.UTF_8), rs.getBytes(1));
        // A non-string/non-bytes value cannot be read as bytes.
        assertThrows(SQLException.class, () -> rs.getBytes(2));
    }

    @Test
    void wasNull_afterObjectGettersOnNullCell() throws SQLException {
        StringColumnCodec codec = new StringColumnCodec();
        Object backing = codec.allocate(1);
        Column c = new Column("s", "Nullable(String)");
        c.codec(codec);
        c.values(backing);
        c.nulls(new boolean[] {true});
        c.rowCount(1);
        ChResultSet rs = RsFixtures.open(c);
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertTrue(rs.wasNull());
        assertNull(rs.getBigDecimal(1));
        assertTrue(rs.wasNull());
    }
}
