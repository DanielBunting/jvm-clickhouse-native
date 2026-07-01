package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Confirms server and client errors surface as {@link SQLException} through the JDBC layer
 * (rather than leaking a raw core exception), and that using closed objects throws.
 */
@Tag("integration")
@Testcontainers
class JdbcErrorHandlingIT {

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
    void malformedSqlThrowsSqlException() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            assertThrows(SQLException.class, () -> st.executeQuery("SELECT FROM WHERE"));
        }
    }

    @Test
    void missingTableThrowsSqlException() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT * FROM no_such_table_xyz"));
        }
    }

    @Test
    void closedStatementThrows() throws Exception {
        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.close();
            assertThrows(SQLException.class, () -> st.executeQuery("SELECT 1"));
        }
    }

    @Test
    void closedResultSetThrows() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT 1");
            rs.close();
            assertThrows(SQLException.class, rs::next);
        }
    }

    @Test
    void unknownColumnLabelThrows() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1 AS a")) {
            assertTrue(rs.next());
            assertThrows(SQLException.class, () -> rs.getInt("nope"));
        }
    }
}
