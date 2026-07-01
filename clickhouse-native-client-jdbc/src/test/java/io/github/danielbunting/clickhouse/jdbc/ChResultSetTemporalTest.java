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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Calendar;
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
