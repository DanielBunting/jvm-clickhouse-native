package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end smoke test for the JDBC driver against a real ClickHouse server started via
 * Testcontainers.
 *
 * <p>The stock {@code clickhouse/clickhouse-server} image restricts the {@code default} user to
 * localhost-only connections. Because Testcontainers reaches the server through a mapped port (a
 * non-loopback path from the container's perspective), we drop in a small users override that opens
 * the {@code default} user to all networks.
 */
@Tag("integration")
@Testcontainers
class JdbcSmokeIT {

    /** Users override that opens the default user to all networks (IPv4/IPv6). */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks></default></users></clickhouse>";

    /** Native-TCP protocol port. */
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

    @Test
    @DisplayName("DriverManager resolves the driver and SELECT 1 round-trips")
    void selectOne() throws Exception {
        String host = CLICKHOUSE.getHost();
        int mappedPort = CLICKHOUSE.getMappedPort(NATIVE_PORT);
        String url = "jdbc:chnative://" + host + ":" + mappedPort + "/default";

        // DriverManager should locate our driver for the URL (via ServiceLoader). No
        // Class.forName needed.
        Driver driver = DriverManager.getDriver(url);
        assertNotNull(driver, "DriverManager should find a driver for: " + url);

        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next(), "result set should have one row");
            assertEquals(1, rs.getInt(1));
            assertEquals(1L, rs.getLong(1));
            assertFalse(rs.next(), "result set should have exactly one row");
        }
    }
}
