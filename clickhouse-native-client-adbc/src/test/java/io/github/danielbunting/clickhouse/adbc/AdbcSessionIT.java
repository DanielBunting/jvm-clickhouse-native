package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ServerException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Session/connection-scoped behavior over the native protocol at the ADBC surface, ported from
 * the JDBC module's {@code JdbcSessionIT}: database selection at connect, credential errors with
 * server codes, server-side session state ({@code USE}, {@code SET ROLE}, temporary tables, row
 * policies) persisting across statements of one connection but never leaking across connections,
 * password auth types, and URI server settings. One ADBC connection = one native session.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcSessionIT extends AdbcIntegrationTest {

    // ---- database selection & credentials -------------------------------------------------------

    @Test
    @DisplayName("connecting to a nonexistent database fails with UNKNOWN_DATABASE (81) naming it")
    void nonExistentDatabaseFailsUsefully(BufferAllocator allocator) throws Exception {
        Map<String, Object> params = connectParams();
        params.put(AdbcParams.PARAM_DATABASE, "adbc_session_no_such_db");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(params);
        try {
            AdbcException ex = assertThrows(AdbcException.class, () -> database.connect().close());
            // The connect-failover wrapper's own message names only the endpoint; the database
            // name is carried by the chained ServerException, whose code proves the scenario.
            assertEquals(81, serverCode(ex), "expected UNKNOWN_DATABASE: " + ex.getMessage());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a wrong password fails the eager handshake with AUTHENTICATION_FAILED (516)")
    void wrongPasswordFailsOnConnect(BufferAllocator allocator) throws Exception {
        AdbcDatabase adminDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection admin = adminDb.connect()) {
            execDdl(admin, "DROP USER IF EXISTS adbc_session_pw_user");
            execDdl(admin, "CREATE USER adbc_session_pw_user IDENTIFIED WITH plaintext_password"
                    + " BY 'correct-horse'");
        } finally {
            adminDb.close();
        }

        // The right password authenticates and the session reports the user.
        AdbcDatabase okDb = new ChAdbcDriver(allocator).open(withCredentials("adbc_session_pw_user", "correct-horse"));
        try (AdbcConnection conn = okDb.connect()) {
            assertEquals(List.of("adbc_session_pw_user"), readStrings(conn, "SELECT currentUser()"));
        } finally {
            okDb.close();
        }

        AdbcDatabase badDb = new ChAdbcDriver(allocator).open(withCredentials("adbc_session_pw_user", "wrong-password"));
        try {
            AdbcException ex = assertThrows(AdbcException.class, () -> badDb.connect().close());
            assertEquals(516, serverCode(ex), "expected AUTHENTICATION_FAILED: " + ex.getMessage());
        } finally {
            badDb.close();
        }
    }

    @Test
    @DisplayName("plaintext, sha256 and double-sha1 password auth types all authenticate")
    void passwordAuthTypesAllAuthenticate(BufferAllocator allocator) throws Exception {
        String[][] users = {
                {"adbc_auth_plain", "plaintext_password"},
                {"adbc_auth_sha256", "sha256_password"},
                {"adbc_auth_dsha1", "double_sha1_password"},
        };
        AdbcDatabase adminDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection admin = adminDb.connect()) {
            for (String[] user : users) {
                execDdl(admin, "DROP USER IF EXISTS " + user[0]);
                execDdl(admin, "CREATE USER " + user[0] + " IDENTIFIED WITH " + user[1] + " BY 'pw-" + user[0] + "'");
            }
        } finally {
            adminDb.close();
        }
        for (String[] user : users) {
            AdbcDatabase db = new ChAdbcDriver(allocator).open(withCredentials(user[0], "pw-" + user[0]));
            try (AdbcConnection conn = db.connect()) {
                assertEquals(List.of(user[0]), readStrings(conn, "SELECT currentUser()"),
                        user[1] + " must authenticate");
            } finally {
                db.close();
            }
        }
    }

    @Test
    @DisplayName("exotic database names (spaces, dashes, unicode) connect via the discrete parameter")
    void exoticDatabaseNamesConnect(BufferAllocator allocator) throws Exception {
        String database = "adbc db-mit ünïcode";
        AdbcDatabase adminDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection admin = adminDb.connect()) {
            execDdl(admin, "CREATE DATABASE `" + database + "`");
            try {
                Map<String, Object> params = connectParams();
                params.put(AdbcParams.PARAM_DATABASE, database);
                AdbcDatabase db = new ChAdbcDriver(allocator).open(params);
                try (AdbcConnection conn = db.connect()) {
                    assertEquals(List.of(database), readStrings(conn, "SELECT currentDatabase()"));
                } finally {
                    db.close();
                }
            } finally {
                execDdl(admin, "DROP DATABASE IF EXISTS `" + database + "`");
            }
        } finally {
            adminDb.close();
        }
    }

    // ---- server-side session state ----------------------------------------------------------------

    @Test
    @DisplayName("SET ROLE persists for later statements on the same connection, never across connections")
    void setRolePersistsAcrossStatements(BufferAllocator allocator) throws Exception {
        AdbcDatabase adminDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection admin = adminDb.connect()) {
            execDdl(admin, "DROP ROLE IF EXISTS adbc_rol1, adbc_rol2");
            execDdl(admin, "DROP USER IF EXISTS adbc_session_role_user");
            execDdl(admin, "CREATE ROLE adbc_rol1, adbc_rol2");
            execDdl(admin, "CREATE USER adbc_session_role_user IDENTIFIED WITH no_password");
            execDdl(admin, "GRANT adbc_rol1, adbc_rol2 TO adbc_session_role_user");
            execDdl(admin, "SET DEFAULT ROLE NONE TO adbc_session_role_user");
        } finally {
            adminDb.close();
        }

        AdbcDatabase db = new ChAdbcDriver(allocator).open(withCredentials("adbc_session_role_user", ""));
        try (AdbcConnection conn = db.connect()) {
            assertEquals(List.of("[]"), currentRoles(conn), "no default roles active");
            execDdl(conn, "SET ROLE adbc_rol2");
            assertEquals(List.of("['adbc_rol2']"), currentRoles(conn),
                    "SET ROLE must persist for later statements on the same connection");
            execDdl(conn, "SET ROLE adbc_rol1, adbc_rol2");
            assertEquals(List.of("['adbc_rol1','adbc_rol2']"), currentRoles(conn));
            execDdl(conn, "SET ROLE NONE");
            assertEquals(List.of("[]"), currentRoles(conn), "SET ROLE NONE resets the session");
        } finally {
            db.close();
        }

        AdbcDatabase fresh = new ChAdbcDriver(allocator).open(withCredentials("adbc_session_role_user", ""));
        try (AdbcConnection conn = fresh.connect()) {
            assertEquals(List.of("[]"), currentRoles(conn),
                    "roles are session state and must not leak across connections");
        } finally {
            fresh.close();
        }
    }

    @Test
    @DisplayName("row policies bound to roles gate row visibility per the session's active role")
    void setRoleGatesRowVisibilityThroughRowPolicies(BufferAllocator allocator) throws Exception {
        AdbcDatabase adminDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection admin = adminDb.connect()) {
            execDdl(admin, "DROP ROLE IF EXISTS adbc_row_a, adbc_row_b");
            execDdl(admin, "DROP USER IF EXISTS adbc_session_rp_user");
            execDdl(admin, "CREATE ROLE adbc_row_a, adbc_row_b");
            execDdl(admin, "CREATE USER adbc_session_rp_user IDENTIFIED WITH no_password");
            execDdl(admin, "GRANT adbc_row_a, adbc_row_b TO adbc_session_rp_user");
            execDdl(admin, "DROP TABLE IF EXISTS adbc_session_row_policy");
            execDdl(admin, "CREATE TABLE adbc_session_row_policy (s String) "
                    + "ENGINE = MergeTree ORDER BY tuple()");
            execDdl(admin, "INSERT INTO adbc_session_row_policy VALUES ('a'), ('b')");
            execDdl(admin, "GRANT SELECT ON adbc_session_row_policy TO adbc_session_rp_user");
            execDdl(admin, "CREATE ROW POLICY OR REPLACE adbc_policy_a ON adbc_session_row_policy "
                    + "FOR SELECT USING s = 'a' TO adbc_row_a");
            execDdl(admin, "CREATE ROW POLICY OR REPLACE adbc_policy_b ON adbc_session_row_policy "
                    + "FOR SELECT USING s = 'b' TO adbc_row_b");

            AdbcDatabase db = new ChAdbcDriver(allocator).open(withCredentials("adbc_session_rp_user", ""));
            try (AdbcConnection conn = db.connect()) {
                String probe = "SELECT s FROM adbc_session_row_policy ORDER BY s";
                assertEquals(List.of("a", "b"), readStrings(conn, probe),
                        "no active role: no policy applies, all rows visible");
                execDdl(conn, "SET ROLE adbc_row_a");
                assertEquals(List.of("a"), readStrings(conn, probe));
                execDdl(conn, "SET ROLE adbc_row_b");
                assertEquals(List.of("b"), readStrings(conn, probe));
                execDdl(conn, "SET ROLE adbc_row_a, adbc_row_b");
                assertEquals(List.of("a", "b"), readStrings(conn, probe), "both policies OR together");
            } finally {
                db.close();
                execDdl(admin, "DROP TABLE IF EXISTS adbc_session_row_policy");
            }
        } finally {
            adminDb.close();
        }
    }

    @Test
    @DisplayName("USE switches the session database for subsequent statements")
    void useStatementSwitchesDatabase(BufferAllocator allocator) throws Exception {
        String database = "adbc_session_use_db";
        AdbcDatabase db = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = db.connect()) {
            execDdl(conn, "DROP DATABASE IF EXISTS " + database);
            execDdl(conn, "CREATE DATABASE " + database);
            try {
                execDdl(conn, "USE " + database);
                assertEquals(List.of(database), readStrings(conn, "SELECT currentDatabase()"),
                        "the switch is session state visible to a NEW statement");
                // Unqualified DDL after USE lands in the switched-to database.
                execDdl(conn, "CREATE TABLE use_probe (id UInt8) ENGINE = Memory");
                assertEquals(List.of("use_probe"), readStrings(conn,
                        "SELECT name FROM system.tables WHERE database = '" + database + "'"));
            } finally {
                execDdl(conn, "DROP DATABASE IF EXISTS " + database);
            }
        } finally {
            db.close();
        }
    }

    @Test
    @DisplayName("a temporary table is scoped to its own connection")
    void temporaryTableIsScopedToItsConnection(BufferAllocator allocator) throws Exception {
        AdbcDatabase db = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection owner = db.connect(); AdbcConnection other = db.connect()) {
            execDdl(owner, "CREATE TEMPORARY TABLE adbc_temp_probe (n Int64)");
            execDdl(owner, "INSERT INTO adbc_temp_probe VALUES (42)");
            assertEquals(List.of("42"), readStrings(owner, "SELECT toString(n) FROM adbc_temp_probe"),
                    "the owning session sees its temp table");

            AdbcException ex = assertThrows(AdbcException.class, () -> {
                try (AdbcStatement statement = other.createStatement()) {
                    statement.setSqlQuery("SELECT n FROM adbc_temp_probe");
                    statement.executeQuery().close();
                }
            });
            assertEquals(60, serverCode(ex),
                    "another session must not see it (UNKNOWN_TABLE): " + ex.getMessage());
        } finally {
            db.close();
        }
    }

    // ---- URI server settings ------------------------------------------------------------------------

    @Test
    @DisplayName("a settings.* URI parameter lands as session state the server reports and enforces")
    void serverSettingFromUriIsAppliedToTheSession(BufferAllocator allocator) throws Exception {
        Map<String, Object> params = new java.util.HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default?settings.max_result_rows=5"
                + "&settings.result_overflow_mode=throw");
        AdbcDatabase db = new ChAdbcDriver(allocator).open(params);
        try (AdbcConnection conn = db.connect()) {
            assertEquals(List.of("5"), readStrings(conn,
                    "SELECT value FROM system.settings WHERE name = 'max_result_rows'"),
                    "the session must report the URI-supplied setting");

            // The overflow abort may arrive at executeQuery time (AdbcException) or mid-stream
            // (the reader's pinned raw-core-exception divergence); the server code proves it
            // either way.
            Exception ex = assertThrows(Exception.class, () -> {
                try (AdbcStatement statement = conn.createStatement()) {
                    statement.setSqlQuery("SELECT number FROM numbers(100)");
                    try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                        ArrowReader reader = result.getReader();
                        while (reader.loadNextBatch()) {
                            // drain: the overflow abort may arrive mid-stream
                        }
                    }
                }
            });
            assertEquals(396, serverCode(ex),
                    "expected TOO_MANY_ROWS_OR_BYTES (396): " + ex.getMessage());
        } finally {
            db.close();
        }
    }

    @Test
    @DisplayName("DIVERGENCE: executeQuery on DDL executes it and yields an empty result")
    void executeQueryOnDdlYieldsEmptyResult(BufferAllocator allocator) throws Exception {
        // Mirrors the JDBC driver's pinned deviation (executeQuery on DDL returns an empty
        // ResultSet): ADBC streams the DDL through the query path, which the server answers
        // with no data blocks — and the DDL takes effect.
        String table = uniqueTable("adbc_ddl_via_query");
        AdbcDatabase db = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = db.connect()) {
            try (AdbcStatement statement = conn.createStatement()) {
                statement.setSqlQuery("CREATE TABLE " + table + " (n Int64) ENGINE = Memory");
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    assertFalse(result.getReader().loadNextBatch(), "DDL produces no batches");
                }
            }
            try {
                assertEquals(List.of("0"), readStrings(conn, "SELECT toString(count()) FROM " + table),
                        "the DDL must have taken effect");
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            db.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------------

    private static Map<String, Object> withCredentials(String user, String password) {
        Map<String, Object> params = connectParams();
        AdbcDriver.PARAM_USERNAME.set(params, user);
        AdbcDriver.PARAM_PASSWORD.set(params, password);
        return params;
    }

    /** First core {@link ServerException} code in the cause chain, or -1. */
    private static int serverCode(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ServerException se) {
                return se.code();
            }
        }
        return -1;
    }

    private static List<String> currentRoles(AdbcConnection conn) throws Exception {
        return readStrings(conn, "SELECT toString(arraySort(currentRoles()))");
    }

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }

    /** Drains a one-VarChar-column query into strings. */
    private static List<String> readStrings(AdbcConnection adbc, String sql) throws Exception {
        List<String> out = new ArrayList<>();
        try (AdbcStatement statement = adbc.createStatement()) {
            statement.setSqlQuery(sql);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    VarCharVector vector = (VarCharVector) root.getVector(0);
                    for (int r = 0; r < root.getRowCount(); r++) {
                        out.add(vector.isNull(r) ? null : String.valueOf(vector.getObject(r)));
                    }
                }
            }
        }
        assertNotNull(out);
        return out;
    }
}
