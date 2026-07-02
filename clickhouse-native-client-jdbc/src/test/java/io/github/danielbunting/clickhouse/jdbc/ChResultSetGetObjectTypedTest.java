package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.DateTimeCodec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Int64Codec;
import io.github.danielbunting.clickhouse.types.codec.Ipv6Codec;
import io.github.danielbunting.clickhouse.types.codec.UuidCodec;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Server-free unit tests for {@link ChResultSet#getObject(int, Class)}: the
 * {@code isInstance} passthrough for reference/composite types, the scalar coercions
 * via {@link JdbcValues}, null handling, and the type-map overload that ignores its map.
 */
class ChResultSetGetObjectTypedTest {

    @Test
    void passthrough_list_uuid_instant_inet() throws Exception {
        UUID uuid = UUID.randomUUID();
        Instant when = Instant.parse("2026-01-02T03:04:05Z");
        InetAddress ip = InetAddress.getByName("2001:db8::5");
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("a", "Array(Int32)", new ArrayColumnCodec(new Int32Codec()), List.of(1, 2)),
                RsFixtures.complexCol("u", "UUID", new UuidCodec(), uuid),
                RsFixtures.complexCol("t", "DateTime", new DateTimeCodec(null), when),
                RsFixtures.complexCol("ip", "IPv6", new Ipv6Codec(), ip));
        assertTrue(rs.next());
        assertEquals(List.of(1, 2), rs.getObject(1, List.class));
        assertEquals(uuid, rs.getObject(2, UUID.class));
        assertEquals(when, rs.getObject(3, Instant.class));
        assertEquals(ip, rs.getObject(4, InetAddress.class));
    }

    @Test
    void coercions_scalar() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("n", "Int64", new Int64Codec(), 42L));
        assertTrue(rs.next());
        assertEquals(Integer.valueOf(42), rs.getObject(1, Integer.class));
        assertEquals("42", rs.getObject(1, String.class));
        assertEquals(new BigDecimal(42), rs.getObject(1, BigDecimal.class));
        assertEquals(Boolean.TRUE, rs.getObject(1, Boolean.class));
    }

    @Test
    void coercion_outOfRangeNarrowing_throws() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("n", "Int64", new Int64Codec(), Long.MAX_VALUE));
        assertTrue(rs.next());
        assertThrows(SQLException.class, () -> rs.getObject(1, Integer.class));
    }

    /**
     * Zoned/local views of a DateTime column derive from the boxed {@link Instant}
     * at UTC: {@code getObject(..., ZonedDateTime/OffsetDateTime/LocalDateTime.class)}
     * all report the same absolute instant with a UTC zone/offset.
     */
    @Test
    void coercions_temporalViewsOfInstantAreUtc() throws Exception {
        Instant when = Instant.parse("2026-01-02T03:04:05Z");
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("t", "DateTime", new DateTimeCodec(null), when));
        assertTrue(rs.next());
        assertEquals(when.atZone(java.time.ZoneOffset.UTC),
                rs.getObject(1, java.time.ZonedDateTime.class));
        assertEquals(when.atOffset(java.time.ZoneOffset.UTC),
                rs.getObject(1, java.time.OffsetDateTime.class));
        assertEquals(java.time.LocalDateTime.of(2026, 1, 2, 3, 4, 5),
                rs.getObject(1, java.time.LocalDateTime.class));
    }

    /** The temporal view coercions only apply to Instant-boxed columns; a numeric column cannot masquerade as one. */
    @Test
    void coercions_temporalViewsOfNonTemporalColumnThrow() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("n", "Int64", new Int64Codec(), 42L));
        assertTrue(rs.next());
        assertThrows(SQLException.class, () -> rs.getObject(1, java.time.ZonedDateTime.class));
        assertThrows(SQLException.class, () -> rs.getObject(1, java.time.OffsetDateTime.class));
        assertThrows(SQLException.class, () -> rs.getObject(1, java.time.LocalDateTime.class));
    }

    @Test
    void unknownTargetType_throws() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("n", "Int64", new Int64Codec(), 42L));
        assertTrue(rs.next());
        assertThrows(SQLException.class, () -> rs.getObject(1, StringBuilder.class));
    }

    @Test
    void nullCell_returnsNullForAnyType() throws Exception {
        Int64Codec codec = new Int64Codec();
        Object backing = codec.allocate(1);
        Column c = new Column("n", "Nullable(Int64)");
        c.codec(codec);
        c.values(backing);
        c.nulls(new boolean[] {true});
        c.rowCount(1);
        ChResultSet rs = RsFixtures.open(c);
        assertTrue(rs.next());
        assertNull(rs.getObject(1, Integer.class));
        assertNull(rs.getObject(1, String.class));
    }

    @Test
    void getObjectWithTypeMap_ignoresMapAndReturnsRaw() throws Exception {
        List<Integer> list = List.of(7, 8);
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("a", "Array(Int32)", new ArrayColumnCodec(new Int32Codec()), list));
        assertTrue(rs.next());
        Object v = rs.getObject(1, Map.of("X", Integer.class));
        // The type map is ignored; the raw boxed value is returned.
        assertEquals(list, v);
    }
}
