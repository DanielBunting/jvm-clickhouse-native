package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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

    private static String url() {
        return "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(url());
    }

    /** First core {@link ServerException} in the cause chain, or {@code null}. */
    private static ServerException serverException(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ServerException) {
                return (ServerException) c;
            }
        }
        return null;
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

    /**
     * A read that outlives the configured socket timeout fails at the JDBC layer with the
     * {@link java.net.SocketTimeoutException} in the cause chain (ported from v1
     * {@code ClickHouseStatementTest#testSocketTimeout}; the {@code socket_timeout}
     * property maps to this driver's {@code socketTimeout} URL parameter, in seconds).
     *
     * <p>The server normally sends Progress packets every ~100ms while a query runs,
     * which keeps the socket alive, so the test stretches {@code interactive_delay}
     * beyond the sleep to make the connection genuinely idle. The thrown type is
     * asserted loosely ({@code Exception}) because mid-stream failures currently leak
     * core exceptions instead of {@code SQLException} — that defect is documented by
     * {@code JdbcStatementIT#knownBug_midStreamServerErrorSurfacesAsSqlException}.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void socketTimeoutSurfacesAsErrorWithSocketTimeoutCause() throws Exception {
        Connection conn = DriverManager.getConnection(
                url() + "?socketTimeout=1&settings.interactive_delay=10000000");
        try {
            Statement st = conn.createStatement();
            Exception e = assertThrows(Exception.class, () -> {
                try (ResultSet rs = st.executeQuery("SELECT sleep(2)")) {
                    rs.next();
                }
            });
            boolean timedOut = false;
            for (Throwable c = e; c != null; c = c.getCause()) {
                if (c instanceof java.net.SocketTimeoutException) {
                    timedOut = true;
                }
            }
            assertTrue(timedOut, "expected a SocketTimeoutException in the chain: " + e);
        } finally {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // The timed-out socket may fail the close handshake; irrelevant here.
            }
        }
    }

    /**
     * KNOWN BUG (failing on purpose — ported from jdbc-v2
     * {@code JDBCErrorHandlingTests#testServerErrorCodePropagatedToSQLException}):
     * {@link SQLException#getErrorCode()} must carry the server's error code
     * (here UNKNOWN_DATABASE = 81), per the JDBC vendor-code contract.
     *
     * <p>Actual: the code is 0. The core {@code ServerException} in the cause chain
     * DOES carry 81 (asserted first, proving this failure is exactly the propagation
     * bug and not a missing server error), but {@code ChStatement.wrap(RuntimeException)}
     * builds {@code new SQLException(e.getMessage(), e)}, discarding it.
     *
     * <p>HOW TO FIX: in {@code ChStatement.wrap} (src/main/java/.../jdbc/ChStatement.java),
     * when the cause chain contains a {@code ServerException se}, construct
     * {@code new SQLException(e.getMessage(), null, se.code(), e)} (and consider the same
     * for {@code ChPreparedStatement}/{@code ChConnection} wrap sites) so
     * {@code getErrorCode()} reports the ClickHouse error code.
     */
    @Test
    void knownBug_serverErrorCodePropagatedToSqlExceptionGetErrorCode() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            SQLException e = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT * FROM no_such_db_xyz.unknown_table"));
            ServerException server = serverException(e);
            assertNotNull(server, "sanity: the server error must be in the cause chain");
            assertEquals(81, server.code(),
                    "sanity: the cause carries UNKNOWN_DATABASE (81): " + e.getMessage());
            // The actual contract under test — currently 0 (bug).
            assertEquals(81, e.getErrorCode(),
                    "SQLException.getErrorCode() must propagate the server error code");
        }
    }
}
