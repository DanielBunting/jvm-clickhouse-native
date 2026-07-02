package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A thin, one-row-per-family matrix proving the JDBC read path wires each ClickHouse type
 * through to the right typed getter, {@code getObject} boxing and {@link ResultSetMetaData}.
 * Depth of codec fidelity lives in the core module's per-type ITs; this is a wiring check.
 */
@Tag("integration")
@Testcontainers
class JdbcTypeMatrixIT {

    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks></default></users></clickhouse>";

    private static final int NATIVE_PORT = 9000;

    @Container
    @SuppressWarnings("resource")
    private static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(ClickHouseImages.SERVER)
                    .withExposedPorts(NATIVE_PORT)
                    .withCopyToContainer(
                            Transferable.of(OPEN_DEFAULT_USER_XML),
                            "/etc/clickhouse-server/users.d/zz-open-default.xml")
                    .waitingFor(Wait.forListeningPort());

    private static Connection connect() throws SQLException {
        String url = "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
        return DriverManager.getConnection(url);
    }

    @Test
    void integers() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT toInt8(-8) a, toInt16(-16) b, toInt32(-32) c, toInt64(-64) d, "
                        + "toUInt64(18446744073709551615) e")) {
            assertTrue(rs.next());
            assertEquals((byte) -8, rs.getByte(1));
            assertEquals((short) -16, rs.getShort(2));
            assertEquals(-32, rs.getInt(3));
            assertEquals(-64L, rs.getLong(4));
            // UInt64 max wraps to -1 as a signed long (raw bits).
            assertEquals(-1L, rs.getLong(5));
            assertEquals("18446744073709551615", Long.toUnsignedString(rs.getLong(5)));
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.TINYINT, md.getColumnType(1));
            assertEquals(Types.BIGINT, md.getColumnType(4));
        }
    }

    @Test
    void floatDecimalStringEnum() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT toFloat64(1.5) f, toDecimal64(12.34, 2) d, 'hi' s, "
                        + "CAST('a', 'Enum8(''a''=1,''b''=2)') e")) {
            assertTrue(rs.next());
            assertEquals(1.5, rs.getDouble(1), 0.0);
            assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(2));
            assertEquals("hi", rs.getString(3));
            assertEquals("a", rs.getString(4));
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.DOUBLE, md.getColumnType(1));
            assertEquals(Types.DECIMAL, md.getColumnType(2));
            assertEquals(Types.VARCHAR, md.getColumnType(3));
        }
    }

    @Test
    void uuidAndTemporal() throws Exception {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT toUUID('" + uuid + "') u, toDate('2026-05-30') d, "
                        + "toDateTime('2026-05-30 13:45:07') t")) {
            assertTrue(rs.next());
            assertEquals(uuid, rs.getObject(1));
            assertEquals(java.sql.Date.valueOf("2026-05-30"), rs.getDate(2));
            assertInstanceOf(Instant.class, rs.getObject(3));
            assertEquals(Types.DATE, rs.getMetaData().getColumnType(2));
            assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(3));
        }
    }

    @Test
    void boolNullableLowCardinality() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT true b, CAST(NULL, 'Nullable(Int32)') n, "
                        + "CAST('x', 'LowCardinality(String)') lc")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            rs.getInt(2);
            assertTrue(rs.wasNull());
            assertEquals("x", rs.getString(3));
            assertEquals(ResultSetMetaData.columnNullable, rs.getMetaData().isNullable(2));
        }
    }

    @Test
    void arrayMapTuple() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT [1, 2, 3] a, map('k', 1) m, tuple(1, 'x') t")) {
            assertTrue(rs.next());
            assertArrayEquals(new Object[] {1, 2, 3}, (Object[]) rs.getArray(1).getArray());
            assertInstanceOf(Map.class, rs.getObject(2));
            assertEquals(Map.of("k", 1), rs.getObject(2));
            assertInstanceOf(List.class, rs.getObject(3));
            assertEquals(Types.ARRAY, rs.getMetaData().getColumnType(1));
        }
    }

    @Test
    void ipAddresses() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT toIPv4('192.168.0.1') v4, toIPv6('2001:db8::1') v6")) {
            assertTrue(rs.next());
            assertInstanceOf(InetAddress.class, rs.getObject(1));
            assertEquals(InetAddress.getByName("192.168.0.1"), rs.getObject(1));
            assertEquals(InetAddress.getByName("2001:db8::1"), rs.getObject(2));
        }
    }
}
