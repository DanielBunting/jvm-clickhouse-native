package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Authentication and database-selection semantics over the native protocol, ported from
 * the reference suites (client-v1 ClientIntegrationTest#testNonExistDb, client-v2
 * ClientTests#testPasswordAuthentication). The JDBC-level analogues live in
 * {@code JdbcSessionIT}; these pin the same behavior on the raw native connection.
 */
@Tag("integration")
class AuthAndDatabaseSemanticsIT extends TypeRoundTripBase {

    /** Unwraps to the ServerException in the cause chain and returns its code, or -1. */
    private static int rootServerCode(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ServerException se) {
                return se.code();
            }
        }
        return -1;
    }

    /**
     * Querying a database that does not exist fails with a clear server error (code 81,
     * UNKNOWN_DATABASE) at connect time — the native handshake carries the database — and
     * succeeds once the database is created (reference: testNonExistDb's create-then-retry
     * shape).
     */
    @Test
    void nonExistentDatabaseFailsThenWorksAfterCreate() {
        String db = "authdb_missing_" + System.nanoTime();
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .database(db)
                .build();

        // Connect-time failures are wrapped in ConnectionException; the server's
        // UNKNOWN_DATABASE (81) rejection is the root cause.
        Exception ex = assertThrows(
                io.github.danielbunting.clickhouse.ClickHouseException.class,
                () -> ClickHouseConnection.open(cfg).close(),
                "connecting to a missing database must fail");
        assertEquals(81, rootServerCode(ex), "UNKNOWN_DATABASE (81), got: " + ex.getMessage());

        try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
            admin.execute("CREATE DATABASE " + db);
            try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
                assertEquals(db, String.valueOf(
                                decode(conn, "SELECT currentDatabase()").get(0)[0]),
                        "after CREATE DATABASE the same config connects into it");
            } finally {
                admin.execute("DROP DATABASE IF EXISTS " + db);
            }
        }
    }

    /**
     * A password containing quotes, spaces, symbols, and non-ASCII characters
     * authenticates (reference: client-v2 ClientTests#testPasswordAuthentication —
     * special-character password round trip; the reference's header-vs-URL transport
     * split is HTTP-only and has no counterpart here).
     */
    @Test
    void specialCharacterPasswordAuthenticates() {
        String user = "auth_user_" + System.nanoTime();
        String password = "^1A~$dE#3s p@ceüñ中";
        try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
            admin.execute("CREATE USER " + user
                    + " IDENTIFIED WITH sha256_password BY '"
                    + password.replace("\\", "\\\\").replace("'", "\\'") + "'");
            admin.execute("GRANT SELECT ON *.* TO " + user);
            try {
                ClickHouseConfig cfg = ClickHouseConfig.builder()
                        .host(clickHouseHost())
                        .port(clickHousePort())
                        .username(user)
                        .password(password)
                        .build();
                try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
                    assertEquals(user, String.valueOf(
                                    decode(conn, "SELECT currentUser()").get(0)[0]),
                            "authenticated as the special-character-password user");
                }

                // The wrong password is still rejected for the same user.
                ClickHouseConfig bad = ClickHouseConfig.builder()
                        .host(clickHouseHost())
                        .port(clickHousePort())
                        .username(user)
                        .password(password + "x")
                        .build();
                Exception ex = assertThrows(
                        io.github.danielbunting.clickhouse.ClickHouseException.class,
                        () -> ClickHouseConnection.open(bad).close());
                assertEquals(516, rootServerCode(ex),
                        "AUTHENTICATION_FAILED (516), got: " + ex.getMessage());
            } finally {
                admin.execute("DROP USER IF EXISTS " + user);
            }
        }
    }
}
