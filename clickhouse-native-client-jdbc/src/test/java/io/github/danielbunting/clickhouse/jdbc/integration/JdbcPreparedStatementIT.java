package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end {@link PreparedStatement} coverage against a real server: scalar/temporal/decimal
 * round-trips on both binding modes (default client-side interpolation and
 * {@code server_side_params=true}), plus multi-row {@code executeBatch}.
 */
@Tag("integration")
@Testcontainers
class JdbcPreparedStatementIT {

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

    private static String url() {
        return "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
    }

    private static Connection connect(boolean serverSideParams) throws SQLException {
        Properties props = new Properties();
        props.setProperty("server_side_params", Boolean.toString(serverSideParams));
        return DriverManager.getConnection(url(), props);
    }

    private void scalarRoundTrip(boolean serverSide) throws Exception {
        String table = "pstmt_scalar_" + (serverSide ? "srv" : "cli");
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int32, name String, price Decimal(10, 2), ts DateTime) ENGINE = Memory");
            }
            // The driver renders a Timestamp's wall clock and stores it into the (UTC)
            // DateTime column, so the round trip is compared as an Instant to stay
            // independent of the JVM's default time zone.
            LocalDateTime wall = LocalDateTime.of(2026, 5, 30, 13, 45, 7);
            Instant expected = wall.toInstant(ZoneOffset.UTC);
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, name, price, ts) VALUES (?, ?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "o'brien");
                ps.setBigDecimal(3, new BigDecimal("12.34"));
                ps.setTimestamp(4, Timestamp.valueOf(wall));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, name, price, ts FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("o'brien", rs.getString(2));
                assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(3));
                assertEquals(expected, rs.getObject(4));
            }
        }
    }

    @Test
    void scalarRoundTrip_clientSide() throws Exception {
        scalarRoundTrip(false);
    }

    @Test
    void scalarRoundTrip_serverSideParams() throws Exception {
        scalarRoundTrip(true);
    }

    @Test
    void executeBatchInsertsAllRows() throws Exception {
        try (Connection conn = connect(false)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS pstmt_batch");
                st.execute("CREATE TABLE pstmt_batch (id Int32, name String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO pstmt_batch (id, name) VALUES (?, ?)")) {
                for (int i = 1; i <= 10; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "n" + i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(10, counts.length);
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count(), sum(id) FROM pstmt_batch")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
                assertEquals(55, rs.getInt(2));
            }
        }
    }
}
