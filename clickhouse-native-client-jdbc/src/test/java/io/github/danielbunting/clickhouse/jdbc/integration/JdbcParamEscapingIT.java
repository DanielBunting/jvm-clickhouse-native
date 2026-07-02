package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Live regression coverage, at the JDBC surface, for the query-parameter ESCAPED-form fix in
 * the core {@code QueryParameters}: under {@code server_side_params=true} the bound values
 * travel as server-side parameters whose text the server escape-parses, so before the fix a
 * raw backslash was <b>silently corrupted</b> ({@code "a\b"} arrived as {@code a} + backspace)
 * and a raw newline failed with BAD_QUERY_PARAMETER (457).
 *
 * <p>Assertions are <b>asymmetric</b> — values written through {@code setString} are read back
 * through a plain {@code Statement} (no parameters), so a symmetric encode/decode bug cannot
 * cancel out. Every case runs in BOTH binding modes, proving the two modes now denote
 * identical server-side values. The offline wire contract is pinned in the core module's
 * {@code QueryParameterEscapingTest}; the core-surface live proof is
 * {@code QueryParameterEscapingRoundTripIT}.
 */
@Tag("integration")
@Testcontainers
class JdbcParamEscapingIT {

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

    /** The adversarial payload set shared with the core-level regression IT. */
    private static final String[] PAYLOADS = {
            "a\\b",                    // raw backslash — arrived as a+backspace before the fix
            "c:\\path\\to\\file",      // repeated backslashes
            "trailing\\",              // backslash at end of text
            "line\nbreak",             // raw newline — was BAD_QUERY_PARAMETER (457)
            "tab\there",
            "cr\rreturn",
            "nul\u0000byte",           // embedded NUL travels as the \0 escape
            "it's",                    // quotes must still round trip
            "quote\"double",
            "\\N",                     // literal backslash-N must NOT collapse to SQL NULL
            "{p:String}",              // placeholder look-alike stays data
            "héllo 世界",
    };

    private static Connection connect(boolean serverSideParams) throws SQLException {
        Properties props = new Properties();
        props.setProperty("server_side_params", Boolean.toString(serverSideParams));
        String url = "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
        return DriverManager.getConnection(url, props);
    }

    /**
     * Writes every payload through {@code setString} in the given binding mode, then reads
     * each back byte-exactly through a parameterless {@code Statement}, and finally proves a
     * WHERE-equality parameter finds the right row.
     */
    private void adversarialStringsRoundTrip(boolean serverSideParams) throws Exception {
        String table = "jdbc_param_esc_" + (serverSideParams ? "srv" : "cli");
        try (Connection conn = connect(serverSideParams)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id UInt32, v String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                for (int i = 0; i < PAYLOADS.length; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, PAYLOADS[i]);
                    ps.executeUpdate();
                }
            }

            // Asymmetric read-back: plain SQL, no parameters anywhere on the read path.
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT v, length(v) FROM " + table + " ORDER BY id")) {
                for (String payload : PAYLOADS) {
                    assertTrue(rs.next());
                    assertEquals(payload, rs.getString(1),
                            "value must arrive byte-exact: " + escapeForMessage(payload));
                    assertEquals(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
                            rs.getInt(2), "server-side length: " + escapeForMessage(payload));
                }
                assertFalse(rs.next());
            }

            // And the reverse asymmetry: a parameter equality filter finds the literal row.
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM " + table + " WHERE v = ?")) {
                for (int i = 0; i < PAYLOADS.length; i++) {
                    ps.setString(1, PAYLOADS[i]);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "filter must match: " + escapeForMessage(PAYLOADS[i]));
                        assertEquals(i, rs.getInt(1));
                        assertFalse(rs.next());
                    }
                }
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @Test
    void adversarialStringsRoundTrip_serverSideParams() throws Exception {
        adversarialStringsRoundTrip(true);
    }

    @Test
    void adversarialStringsRoundTrip_clientSide() throws Exception {
        adversarialStringsRoundTrip(false);
    }

    /**
     * A bound value of the literal 2-char string {@code \N} must stay that string under
     * server-side parameters — before the escaping fix it collided with the wire NULL
     * sentinel. An actual {@code setNull} stays SQL NULL.
     */
    @Test
    void literalBackslashNIsNotNull_serverSideParams() throws Exception {
        String table = "jdbc_param_esc_nsent";
        try (Connection conn = connect(true)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id UInt32, v Nullable(String)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                ps.setInt(1, 0);
                ps.setString(2, "\\N");
                ps.executeUpdate();
                ps.setInt(1, 1);
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT v FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("\\N", rs.getString(1),
                        "the literal backslash-N string must not collapse to SQL NULL");
                assertFalse(rs.wasNull());
                assertTrue(rs.next());
                assertNull(rs.getString(1));
                assertTrue(rs.wasNull(), "only an actual null binding is SQL NULL");
                assertFalse(rs.next());
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    /** Renders control characters visibly for assertion messages. */
    private static String escapeForMessage(String s) {
        return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
                .replace("\t", "\\t").replace("\u0000", "\\0");
    }
}
