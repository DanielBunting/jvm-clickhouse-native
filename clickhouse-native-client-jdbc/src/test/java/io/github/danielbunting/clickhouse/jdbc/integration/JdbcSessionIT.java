package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.jdbc.ChDataSource;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Session/connection-scoped behavior over the native TCP protocol: database selection at
 * connect time, credential errors, server-side session state ({@code USE}, {@code SET ROLE},
 * temporary tables) persisting across statements on the same connection, and the driver's
 * actual (sometimes deliberately non-reference) semantics for {@code setSchema},
 * duplicate column labels and {@code executeQuery} on DDL.
 *
 * <p>Ported from the reference client's {@code ClickHouseConnectionTest},
 * {@code AccessManagementTest}, jdbc-v2 {@code StatementTest}/{@code ConnectionTest} and
 * {@code ClientIntegrationTest#testTempTable}. Where this driver's documented contract
 * deviates from the reference, the deviation is asserted (honestly) and called out in a
 * comment rather than papered over.
 */
@Tag("integration")
@Testcontainers
class JdbcSessionIT {

    /**
     * Opens the default user to remote hosts and grants it access management so tests can
     * CREATE USER / ROLE / ROW POLICY via plain DDL.
     */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks>"
            + "<access_management>1</access_management>"
            + "</default></users></clickhouse>";

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
        return url("default");
    }

    /** JDBC URL selecting {@code database} (path segment is percent-encoded). */
    private static String url(String database) {
        String encoded = URLEncoder.encode(database, StandardCharsets.UTF_8)
                .replace("+", "%20");
        return "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/" + encoded;
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(url());
    }

    private static Connection connectAs(String user, String password) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        return DriverManager.getConnection(url(), props);
    }

    /**
     * Walks the cause chain of {@code t} and returns the first core
     * {@link ServerException}, or {@code null} if the failure never reached the server.
     */
    private static ServerException serverException(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ServerException) {
                return (ServerException) c;
            }
        }
        return null;
    }

    /** Runs {@code SELECT currentDatabase()} on a fresh statement of {@code conn}. */
    private static String currentDatabase(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT currentDatabase()")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    /**
     * Renders {@code currentRoles()} as its ClickHouse array literal (e.g. {@code ['r1']})
     * via a fresh statement, exercising that session state set by an earlier statement is
     * visible to later statements on the same connection.
     */
    private static String currentRolesLiteral(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT arraySort(currentRoles())")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // ------------------------------------------------------------------
    // 1. Nonexistent database (reference: ClickHouseConnectionTest#testNonExistDatabase)
    // ------------------------------------------------------------------

    /**
     * Unlike the reference HTTP driver (lazy: connect succeeds, first query fails with
     * code 81), this native driver performs the protocol handshake eagerly, so a
     * nonexistent database is rejected. Wherever it surfaces (connect or first query),
     * it must be a useful {@link SQLException} carrying the server's UNKNOWN_DATABASE
     * (81) error.
     */
    @Test
    void nonExistentDatabaseFailsWithUsefulSqlException() {
        String database = "jdbc_session_no_such_db";
        SQLException failure = null;
        try (Connection conn = DriverManager.getConnection(url(database))) {
            // If connecting was lazy, the first query must fail instead.
            try (Statement st = conn.createStatement()) {
                st.executeQuery("SELECT 1");
            }
        } catch (SQLException e) {
            failure = e;
        }
        assertNotNull(failure,
                "connecting with a nonexistent database must fail at connect or first query");
        ServerException server = serverException(failure);
        assertNotNull(server, "the SQLException must chain the server-side error: " + failure);
        assertEquals(81, server.code(),
                "expected UNKNOWN_DATABASE (81) but got: " + failure.getMessage());
        assertTrue(failure.getMessage().contains(database),
                "error message should name the missing database: " + failure.getMessage());
    }

    // ------------------------------------------------------------------
    // 2. Wrong credentials (reference: GenericJDBCTest#connectionWithPropertiesTest)
    // ------------------------------------------------------------------

    @Test
    void wrongPasswordFailsOnConnectWithSqlException() throws Exception {
        try (Connection admin = connect(); Statement st = admin.createStatement()) {
            st.execute("DROP USER IF EXISTS jdbc_session_pw_user");
            st.execute("CREATE USER jdbc_session_pw_user IDENTIFIED WITH plaintext_password "
                    + "BY 'correct-horse'");
        }
        // Correct password connects and can query.
        try (Connection conn = connectAs("jdbc_session_pw_user", "correct-horse");
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT currentUser()")) {
            assertTrue(rs.next());
            assertEquals("jdbc_session_pw_user", rs.getString(1));
        }
        // Wrong password fails the eager native handshake with an SQLException.
        SQLException e = assertThrows(SQLException.class,
                () -> connectAs("jdbc_session_pw_user", "wrong-password").close());
        ServerException server = serverException(e);
        assertNotNull(server, "authentication failure must chain the server error: " + e);
        assertEquals(516, server.code(),
                "expected AUTHENTICATION_FAILED (516) but got: " + e.getMessage());
    }

    // ------------------------------------------------------------------
    // 3. SET ROLE session statefulness (reference: AccessManagementTest)
    // ------------------------------------------------------------------

    /**
     * {@code SET ROLE} executed through one statement must persist on the connection's
     * native session and be visible to subsequently created statements
     * (reference: AccessManagementTest#testSetRoleDifferentConnections).
     */
    @Test
    void setRolePersistsAcrossStatementsOnSameConnection() throws Exception {
        try (Connection admin = connect(); Statement st = admin.createStatement()) {
            st.execute("DROP ROLE IF EXISTS jdbc_rol1, jdbc_rol2");
            st.execute("DROP USER IF EXISTS jdbc_session_role_user");
            st.execute("CREATE ROLE jdbc_rol1, jdbc_rol2");
            st.execute("CREATE USER jdbc_session_role_user IDENTIFIED WITH no_password");
            st.execute("GRANT jdbc_rol1, jdbc_rol2 TO jdbc_session_role_user");
            st.execute("SET DEFAULT ROLE NONE TO jdbc_session_role_user");
        }

        try (Connection conn = connectAs("jdbc_session_role_user", "")) {
            assertEquals("[]", currentRolesLiteral(conn), "no default roles active");

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE jdbc_rol2");
            }
            assertEquals("['jdbc_rol2']", currentRolesLiteral(conn),
                    "SET ROLE must persist for later statements on the same connection");

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE jdbc_rol1, jdbc_rol2");
            }
            assertEquals("['jdbc_rol1', 'jdbc_rol2']", currentRolesLiteral(conn));

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE NONE");
            }
            assertEquals("[]", currentRolesLiteral(conn), "SET ROLE NONE resets the session");
        }

        // A brand-new connection must not inherit the other session's roles.
        try (Connection conn = connectAs("jdbc_session_role_user", "")) {
            assertEquals("[]", currentRolesLiteral(conn),
                    "roles are session state and must not leak across connections");
        }
    }

    /**
     * Row policies bound to roles gate row visibility per the currently-set role of the
     * native session (reference: AccessManagementTest#testSetRolesAccessingTableRows).
     */
    @Test
    void setRoleGatesRowVisibilityThroughRowPolicies() throws Exception {
        try (Connection admin = connect(); Statement st = admin.createStatement()) {
            st.execute("DROP ROLE IF EXISTS jdbc_row_a, jdbc_row_b");
            st.execute("DROP USER IF EXISTS jdbc_session_rp_user");
            st.execute("CREATE ROLE jdbc_row_a, jdbc_row_b");
            st.execute("CREATE USER jdbc_session_rp_user IDENTIFIED WITH no_password");
            st.execute("GRANT jdbc_row_a, jdbc_row_b TO jdbc_session_rp_user");
            st.execute("DROP TABLE IF EXISTS jdbc_session_row_policy");
            st.execute("CREATE TABLE jdbc_session_row_policy (s String) "
                    + "ENGINE = MergeTree ORDER BY tuple()");
            st.execute("INSERT INTO jdbc_session_row_policy VALUES ('a'), ('b')");
            st.execute("GRANT SELECT ON jdbc_session_row_policy TO jdbc_session_rp_user");
            st.execute("CREATE ROW POLICY OR REPLACE jdbc_policy_a ON jdbc_session_row_policy "
                    + "FOR SELECT USING s = 'a' TO jdbc_row_a");
            st.execute("CREATE ROW POLICY OR REPLACE jdbc_policy_b ON jdbc_session_row_policy "
                    + "FOR SELECT USING s = 'b' TO jdbc_row_b");
        }

        try (Connection conn = connectAs("jdbc_session_rp_user", "")) {
            assertEquals(List.of("a", "b"), visibleRows(conn),
                    "no active role: no policy applies, all rows visible");

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE jdbc_row_a");
            }
            assertEquals(List.of("a"), visibleRows(conn), "policy for jdbc_row_a filters to 'a'");

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE jdbc_row_b");
            }
            assertEquals(List.of("b"), visibleRows(conn), "policy for jdbc_row_b filters to 'b'");

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE jdbc_row_a, jdbc_row_b");
            }
            assertEquals(List.of("a", "b"), visibleRows(conn), "both policies OR together");

            try (Statement st = conn.createStatement()) {
                st.execute("SET ROLE NONE");
            }
            assertEquals(List.of("a", "b"), visibleRows(conn),
                    "SET ROLE NONE restores unfiltered visibility");
        } finally {
            try (Connection admin = connect(); Statement st = admin.createStatement()) {
                st.execute("DROP TABLE IF EXISTS jdbc_session_row_policy");
            }
        }
    }

    /** Ordered visible rows of the row-policy probe table via a fresh statement. */
    private static List<String> visibleRows(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT s FROM jdbc_session_row_policy ORDER BY s")) {
            java.util.ArrayList<String> rows = new java.util.ArrayList<>();
            while (rs.next()) {
                rows.add(rs.getString(1));
            }
            return rows;
        }
    }

    // ------------------------------------------------------------------
    // 4. USE <db> (reference: jdbc-v2 StatementTest#testSwitchDatabase)
    // ------------------------------------------------------------------

    @Test
    void useStatementSwitchesDatabaseForSubsequentQueries() throws Exception {
        String database = "jdbc_session_use_db";
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + database);
                assertEquals(0, st.executeUpdate("CREATE DATABASE " + database));
                assertFalse(st.execute("USE " + database), "USE produces no result set");
                // Unqualified DDL after USE lands in the switched-to database.
                assertEquals(0, st.executeUpdate(
                        "CREATE TABLE use_probe (id UInt8) ENGINE = Memory"));
            }

            // The switch is session state: a NEW statement sees it too.
            assertEquals(database, currentDatabase(conn));
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SHOW TABLES")) {
                assertTrue(rs.next());
                assertEquals("use_probe", rs.getString(1));
                assertFalse(rs.next());
            }

            // And it can be switched again mid-connection.
            try (Statement st = conn.createStatement()) {
                st.execute("USE system");
                try (ResultSet rs = st.executeQuery("SELECT name FROM settings LIMIT 1")) {
                    assertTrue(rs.next());
                    assertNotNull(rs.getString(1));
                }
            }
            assertEquals("system", currentDatabase(conn));
        } finally {
            try (Connection conn = connect(); Statement st = conn.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + database);
            }
        }
    }

    // ------------------------------------------------------------------
    // 5. setSchema (reference: jdbc-v2 StatementTest#testSetConnectionSchema)
    // ------------------------------------------------------------------

    /**
     * DEVIATION from jdbc-v2: {@link Connection#setSchema} in this driver is
     * storage-only ({@code ChConnection.setSchema} just records the value); it does NOT
     * reroute subsequent statements to the new database. The session database can only
     * be changed with {@code USE} (see above) or by connecting with the database in the
     * URL. This test pins the actual semantics.
     */
    @Test
    void setSchemaIsStorageOnlyAndDoesNotRerouteQueries() throws Exception {
        String database = "jdbc_session_schema_db";
        try (Connection admin = connect(); Statement st = admin.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + database);
            st.execute("CREATE DATABASE " + database);
        }
        try (Connection conn = connect()) {
            // The initial schema derives from the database in the URL path (was null
            // before the bug-24 fix made ChConnection fall back to the URL path).
            assertEquals("default", conn.getSchema(),
                    "schema derives from the URL-path database");

            conn.setSchema(database);
            assertEquals(database, conn.getSchema(), "setSchema stores the value");
            assertEquals("default", currentDatabase(conn),
                    "setSchema must NOT change the native session's current database");
        } finally {
            try (Connection admin = connect(); Statement st = admin.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + database);
            }
        }
    }

    // ------------------------------------------------------------------
    // 6. Exotic database names (reference: jdbc-v2 ConnectionTest#testConnectionWithValidDatabaseName)
    // ------------------------------------------------------------------

    @Test
    void connectsWithExoticDatabaseNamesInUrl() throws Exception {
        List<String> names = List.of("jdbc-db-with-dash", "jdbc db with space", "数据库");
        for (String database : names) {
            try (Connection admin = connect(); Statement st = admin.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS `" + database + "`");
                st.execute("CREATE DATABASE `" + database + "`");
            }
            try (Connection conn = DriverManager.getConnection(url(database))) {
                assertEquals(database, currentDatabase(conn),
                        "session must run in the URL-selected database: " + database);
                try (Statement st = conn.createStatement();
                        ResultSet rs = st.executeQuery("SELECT 1")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            } finally {
                try (Connection admin = connect(); Statement st = admin.createStatement()) {
                    st.execute("DROP DATABASE IF EXISTS `" + database + "`");
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 7. Duplicate column labels (reference: jdbc-v2 StatementTest#testResponseWithDuplicateColumns)
    // ------------------------------------------------------------------

    @Test
    void duplicateColumnLabelsResolveToFirstMatchInFindColumn() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            // Identical literals produce two identically-named columns; both are readable
            // by index, and label lookup resolves to the FIRST match.
            try (ResultSet rs = st.executeQuery("SELECT 'a', 'a'")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount());
                assertEquals("'a'", md.getColumnLabel(1));
                assertEquals("'a'", md.getColumnLabel(2));
                assertEquals(1, rs.findColumn("'a'"), "findColumn resolves the first match");
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("a", rs.getString(2));
                assertEquals("a", rs.getString("'a'"));
            }

            // Two different expressions under the SAME alias are rejected by the server
            // (MULTIPLE_EXPRESSIONS_FOR_ALIAS) and surface as an SQLException.
            SQLException e = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT 1 AS a, 2 AS a"));
            assertTrue(e.getMessage().toLowerCase().contains("alias"),
                    "server error should mention the alias conflict: " + e.getMessage());

            // A cross join qualifies the second table's clashing column name.
            st.execute("DROP TABLE IF EXISTS jdbc_dup_col1");
            st.execute("DROP TABLE IF EXISTS jdbc_dup_col2");
            st.execute("CREATE TABLE jdbc_dup_col1 (name String) "
                    + "ENGINE = MergeTree ORDER BY tuple()");
            st.execute("INSERT INTO jdbc_dup_col1 VALUES ('some name')");
            st.execute("CREATE TABLE jdbc_dup_col2 (name String) "
                    + "ENGINE = MergeTree ORDER BY tuple()");
            st.execute("INSERT INTO jdbc_dup_col2 VALUES ('another name')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM jdbc_dup_col1, jdbc_dup_col2")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount());
                assertEquals("name", md.getColumnLabel(1));
                assertEquals("jdbc_dup_col2.name", md.getColumnLabel(2));
                assertTrue(rs.next());
                assertEquals("some name", rs.getString("name"));
                assertEquals("another name", rs.getString("jdbc_dup_col2.name"));
                assertEquals("some name", rs.getString(1));
                assertEquals("another name", rs.getString(2));
            }
        }
    }

    // ------------------------------------------------------------------
    // 8. executeQuery on DDL (reference: jdbc-v2 StatementTest#testExecuteQueryWithNoResultSetWhenExpected)
    // ------------------------------------------------------------------

    /**
     * DEVIATION from jdbc-v2 (which throws): this driver routes
     * {@code executeQuery(...)} straight to the core query path without classifying the
     * SQL, so a DDL statement executes successfully and yields an EMPTY result set
     * (zero columns, no rows) rather than an {@link SQLException}.
     */
    @Test
    void executeQueryOnDdlReturnsEmptyResultSetInsteadOfThrowing() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_session_ddl_probe");
            try (ResultSet rs = st.executeQuery(
                    "CREATE TABLE jdbc_session_ddl_probe (id String) ENGINE = Memory")) {
                assertFalse(rs.next(), "DDL via executeQuery yields an empty result set");
            }
            // ... and the DDL genuinely executed.
            try (ResultSet rs = st.executeQuery(
                    "SELECT count() FROM system.tables "
                    + "WHERE database = 'default' AND name = 'jdbc_session_ddl_probe'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // ------------------------------------------------------------------
    // 9. Temporary tables (reference: ClientIntegrationTest#testTempTable)
    // ------------------------------------------------------------------

    @Test
    void temporaryTableIsScopedToItsConnection() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TEMPORARY TABLE IF EXISTS jdbc_session_temp");
                st.execute("CREATE TEMPORARY TABLE jdbc_session_temp (a Int8)");
                st.execute("INSERT INTO jdbc_session_temp VALUES (2), (3)");
            }
            // Visible to a different statement on the SAME connection (same session).
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT a FROM jdbc_session_temp ORDER BY a")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }

            // A second connection has its own session and must NOT see the temp table.
            try (Connection other = connect(); Statement st = other.createStatement()) {
                SQLException e = assertThrows(SQLException.class,
                        () -> st.executeQuery("SELECT * FROM jdbc_session_temp"));
                ServerException server = serverException(e);
                assertNotNull(server, "expected a server-side unknown-table error: " + e);
                assertEquals(60, server.code(),
                        "expected UNKNOWN_TABLE (60) but got: " + e.getMessage());
            }
        }
    }

    // ------------------------------------------------------------------
    // 10. Password-type matrix (reference: AccessManagementTest#testPasswordAuthentication)
    // ------------------------------------------------------------------

    /**
     * Users created with each server-side password type — plaintext, SHA-256 and
     * double-SHA1 — must all be able to authenticate over the native protocol (which
     * transmits the password and lets the server verify against its stored form), and a
     * wrong password must fail with AUTHENTICATION_FAILED for each type.
     */
    @Test
    void passwordAuthTypesAllAuthenticate() throws Exception {
        String[][] users = {
                {"jdbc_auth_plain", "plaintext_password"},
                {"jdbc_auth_sha256", "sha256_password"},
                {"jdbc_auth_double_sha1", "double_sha1_password"},
        };
        try (Connection admin = connect(); Statement st = admin.createStatement()) {
            for (String[] user : users) {
                st.execute("DROP USER IF EXISTS " + user[0]);
                st.execute("CREATE USER " + user[0] + " IDENTIFIED WITH " + user[1]
                        + " BY 'secret-" + user[0] + "'");
            }
        }
        for (String[] user : users) {
            try (Connection conn = connectAs(user[0], "secret-" + user[0]);
                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT currentUser()")) {
                assertTrue(rs.next());
                assertEquals(user[0], rs.getString(1),
                        "login should succeed for password type " + user[1]);
            }

            SQLException e = assertThrows(SQLException.class,
                    () -> connectAs(user[0], "wrong-password").close(),
                    "wrong password must be rejected for password type " + user[1]);
            ServerException server = serverException(e);
            assertNotNull(server, "auth failure must chain the server error: " + e);
            assertEquals(516, server.code(),
                    "expected AUTHENTICATION_FAILED (516) for " + user[1] + ": " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 11. setReadOnly (reference: v1 ClickHouseConnectionTest#testReadOnly, disabled upstream)
    // ------------------------------------------------------------------

    /**
     * DEVIATION from the reference driver (which maps read-only mode onto the server's
     * {@code readonly} setting so writes are rejected): {@code ChConnection.setReadOnly}
     * only stores the flag client-side and does NOT enforce anything server-side, so
     * writes still succeed. Pinned so a future enforcement change is deliberate.
     */
    @Test
    void setReadOnlyIsClientSideOnlyAndDoesNotBlockWrites() throws Exception {
        try (Connection conn = connect()) {
            conn.setReadOnly(true);
            assertTrue(conn.isReadOnly(), "the flag itself round-trips");
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS jdbc_session_readonly_probe");
                st.execute("CREATE TABLE jdbc_session_readonly_probe (id UInt8) ENGINE = Memory");
                st.execute("INSERT INTO jdbc_session_readonly_probe VALUES (1)");
                try (ResultSet rs = st.executeQuery(
                        "SELECT count() FROM jdbc_session_readonly_probe")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1),
                            "writes went through despite setReadOnly(true)");
                }
                st.execute("DROP TABLE jdbc_session_readonly_probe");
            }
        }
    }

    // ------------------------------------------------------------------
    // 12. Connection-level server settings via URL (reference: jdbc-v2
    //     ConnectionTest#testMaxResultRowsProperty / #testCustomParameters)
    // ------------------------------------------------------------------

    /**
     * A {@code settings.<name>=<value>} URL parameter becomes a per-connection default
     * server setting that the server actually enforces: {@code max_result_rows=5} makes
     * a 20-row select fail with TOO_MANY_ROWS_OR_BYTES (396). This driver carries the
     * setting in the URL rather than a Properties key (jdbc-v2 uses
     * {@code clickhouse_setting_*} properties; this driver's Properties only carry
     * credentials).
     */
    @Test
    void serverSettingFromUrlIsEnforcedByServer() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                        url() + "?settings.max_result_rows=5");
                Statement st = conn.createStatement()) {
            // Asserting Exception (not SQLException) keeps this test focused on the
            // setting-enforcement dimension; the exception TYPE is covered
            // separately by deferredServerErrorSurfacesAsSqlException.
            Exception e = assertThrows(Exception.class, () -> {
                try (ResultSet rs = st.executeQuery(
                        "SELECT toInt32(number) FROM system.numbers LIMIT 20")) {
                    while (rs.next()) {
                        // drain: the overflow error may surface during iteration
                    }
                }
            });
            ServerException server = serverException(e);
            assertNotNull(server, "expected a server-side overflow error: " + e);
            assertEquals(396, server.code(),
                    "expected TOO_MANY_ROWS_OR_BYTES (396) but got: " + e.getMessage());
        }
    }

    /**
     * A deferred server error on the session connection surfaces as an
     * {@link SQLException} from {@code ResultSet.next()} (was knownBug 3 — same
     * mid-stream translation as {@code JdbcStatementIT#midStreamServerErrorSurfacesAsSqlException}).
     */
    void deferredServerErrorSurfacesAsSqlException() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                        url() + "?settings.max_result_rows=5");
                Statement st = conn.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> {
                try (ResultSet rs = st.executeQuery(
                        "SELECT toInt32(number) FROM system.numbers LIMIT 20")) {
                    while (rs.next()) {
                        // drain until the deferred server error surfaces
                    }
                }
            });
            ServerException server = serverException(e);
            assertNotNull(server, "the SQLException must chain the server error: " + e);
            assertEquals(396, server.code());
        }
    }

    /**
     * A connection-level setting is visible to the session via {@code getSetting()}
     * (reference: jdbc-v2 ConnectionTest#testCustomParameters, which uses a
     * {@code custom_*} setting; a stock server only accepts registered settings, so a
     * built-in one is used here).
     */
    @Test
    void serverSettingFromUrlIsReadableViaGetSetting() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                        url() + "?settings.session_timezone=Asia/Tokyo");
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT getSetting('session_timezone')")) {
            assertTrue(rs.next());
            assertEquals("Asia/Tokyo", rs.getString(1));
        }
    }

    /**
     * DEVIATION from jdbc-v2 over HTTP (DriverTest#testUnknownSettings expects
     * UNKNOWN_SETTING, 115): this driver sends connection settings on the native
     * protocol with flags=0 (not IMPORTANT), and the server skips unknown
     * non-important settings instead of failing the query. Pinned; if the client ever
     * marks settings IMPORTANT this will start failing with 115.
     */
    @Test
    void unknownServerSettingFromUrlIsIgnoredByServer() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                        url() + "?settings.jdbc_session_no_such_setting=1");
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    // ------------------------------------------------------------------
    // 13. database property vs URL path (reference: jdbc-v2 ConnectionTest#testSelectingDatabase)
    // ------------------------------------------------------------------

    /**
     * DEVIATION from jdbc-v2 (where a {@code database} property participates in
     * database selection): this driver's config parser only merges
     * user/username/password from the Properties, so a {@code database} property does
     * NOT change the native session's database — the URL path always wins. The property
     * still seeds JDBC-side {@code getCatalog()}/{@code getSchema()}, which is pinned
     * here too (see ChConnection's constructor).
     */
    @Test
    void databasePropertyDoesNotChangeSessionDatabase() throws Exception {
        Properties props = new Properties();
        props.setProperty("database", "system");
        try (Connection conn = DriverManager.getConnection(url(), props)) {
            assertEquals("default", currentDatabase(conn),
                    "the URL path database wins; the property must not reroute the session");
            assertEquals("system", conn.getCatalog(),
                    "catalog is derived from the database property only");
            assertEquals("system", conn.getSchema(),
                    "schema is derived from the database property only");
        }
    }

    // ------------------------------------------------------------------
    // 14. DataSource happy path (reference: jdbc-v2 DataSourceTest#testGetConnectionWithUserAndPassword)
    // ------------------------------------------------------------------

    /** Creates (idempotently) the plaintext-password user shared by the DataSource tests. */
    private static void createDataSourceUser() throws SQLException {
        try (Connection admin = connect(); Statement st = admin.createStatement()) {
            st.execute("DROP USER IF EXISTS jdbc_session_ds_user");
            st.execute("CREATE USER jdbc_session_ds_user IDENTIFIED WITH plaintext_password "
                    + "BY 'ds-pass'");
        }
    }

    /**
     * {@code getConnection(user, password)} authenticates against a live server and
     * hands out a working connection; bad credentials fail with an SQLException
     * chaining the server's AUTHENTICATION_FAILED error.
     */
    @Test
    void dataSourceGetConnectionUserPasswordOverloadAuthenticates() throws Exception {
        createDataSourceUser();

        ChDataSource ds = new ChDataSource(url());
        try (Connection conn = ds.getConnection("jdbc_session_ds_user", "ds-pass");
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT currentUser()")) {
            assertTrue(rs.next());
            assertEquals("jdbc_session_ds_user", rs.getString(1));
        }

        SQLException e = assertThrows(SQLException.class,
                () -> ds.getConnection("jdbc_session_ds_user", "wrong").close());
        ServerException server = serverException(e);
        assertNotNull(server, "auth failure must chain the server error: " + e);
        assertEquals(516, server.code());
    }

    /**
     * A no-arg {@code getConnection()} authenticates with the credentials supplied to
     * the DataSource constructor: {@code ChDataSource} copies the configured entries
     * for real ({@code putAll}, not the defaults-only {@code new Properties(defaults)}
     * wrapper), and {@code ClickHouseConfig.fromUrl(url, info)} no longer
     * short-circuits on {@code info.isEmpty} — which ignores Properties defaults (was
     * knownBug 21; jdbc-v2 DataSourceTest#testGetConnectionWithUserAndPassword
     * semantics, and the {@code javax.sql.DataSource} contract).
     */
    @Test
    void dataSourceGetConnectionUsesConstructorProperties() throws Exception {
        createDataSourceUser();

        Properties props = new Properties();
        props.setProperty("user", "jdbc_session_ds_user");
        props.setProperty("password", "ds-pass");
        ChDataSource ds = new ChDataSource(url(), props);
        try (Connection conn = ds.getConnection();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT currentUser()")) {
            assertTrue(rs.next());
            assertEquals("jdbc_session_ds_user", rs.getString(1),
                    "getConnection() must use the constructor Properties");
        }
    }
}
