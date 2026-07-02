package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChConnection} that run entirely against an in-memory
 * fake core connection ({@link FakeCore}) — no ClickHouse server is required.
 */
class ChConnectionTest {

    private ChConnection newConnection(FakeCore core) {
        Properties info = new Properties();
        info.setProperty("database", "default");
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", info);
    }

    // ------------------------------------------------------------------
    // Construction / accessors
    // ------------------------------------------------------------------

    @Test
    void coreGetterReturnsWrappedConnection() {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        assertSame(core, conn.core());
    }

    @Test
    void catalogAndSchemaDefaultToDatabaseProperty() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertEquals("default", conn.getCatalog());
        assertEquals("default", conn.getSchema());
    }

    /**
     * A connection opened with a database in the JDBC URL path but no
     * {@code database} property still reports that database as its catalog and
     * schema: the constructor falls back to the URL path when the property is absent
     * (was knownBug 24; jdbc-v2 derives them from the parsed URL).
     */
    @Test
    void catalogAndSchemaFallBackToDatabaseFromUrlPath() throws SQLException {
        ChConnection conn = new ChConnection(
                new FakeCore(), "jdbc:chnative://localhost:9000/mydb", new Properties());
        assertEquals("mydb", conn.getCatalog(), "catalog must come from the URL path");
        assertEquals("mydb", conn.getSchema(), "schema must come from the URL path");
    }

    // ------------------------------------------------------------------
    // Statement factory
    // ------------------------------------------------------------------

    @Test
    void createStatementReturnsChStatementBoundToThisConnection() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        Statement stmt = conn.createStatement();
        assertTrue(stmt instanceof ChStatement);
        assertSame(conn, stmt.getConnection());
    }

    @Test
    void createStatementForwardReadOnlyIsAccepted() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        Statement stmt = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertTrue(stmt instanceof ChStatement);
    }

    @Test
    void createStatementScrollableThrows() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () ->
                conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    void createStatementUpdatableThrows() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () ->
                conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
    }

    @Test
    void prepareStatementScrollableOrUpdatableThrows() {
        // Same forward-only/read-only restriction as createStatement, exercised on the
        // prepareStatement overloads (reference: jdbc-v2
        // ConnectionTest#testCreateUnsupportedStatements).
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(
                "SELECT 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(
                "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareStatement(
                "SELECT 1", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Test
    void prepareStatementForwardReadOnlyWithHoldabilityIsAccepted() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertTrue(conn.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)
                instanceof ChPreparedStatement);
    }

    @Test
    void prepareStatementGeneratedKeysOverloadsAreLenient() throws SQLException {
        // DEVIATION from jdbc-v2 (ConnectionTest#testCreateUnsupportedStatements throws
        // SQLFeatureNotSupportedException): this driver leniently ignores the
        // generated-keys request — ClickHouse has no generated keys, and the returned
        // statement simply reports none. Pinned so a future change is deliberate.
        ChConnection conn = newConnection(new FakeCore());
        assertTrue(conn.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS)
                instanceof ChPreparedStatement);
        assertTrue(conn.prepareStatement("SELECT 1", new int[] {1})
                instanceof ChPreparedStatement);
        assertTrue(conn.prepareStatement("SELECT 1", new String[] {"1"})
                instanceof ChPreparedStatement);
    }

    // ------------------------------------------------------------------
    // Auto-commit defaults / transaction no-ops
    // ------------------------------------------------------------------

    @Test
    void autoCommitDefaultsToTrue() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertTrue(conn.getAutoCommit());
    }

    @Test
    void autoCommitCanBeToggled() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
        conn.setAutoCommit(true);
        assertTrue(conn.getAutoCommit());
    }

    @Test
    void commitAndRollbackAreNoOps() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.commit();
        conn.rollback();
        // No exception and no interaction with the core is the expected behaviour.
    }

    @Test
    void transactionIsolationIsNone() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
    }

    @Test
    void setTransactionIsolationAcceptsOnlyNone() throws SQLException {
        // Reference: jdbc-v2 ConnectionTest#setTransactionIsolationTest — only
        // TRANSACTION_NONE is settable; every real isolation level is rejected.
        ChConnection conn = newConnection(new FakeCore());
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
        assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
        int[] unsupported = {
                Connection.TRANSACTION_READ_UNCOMMITTED,
                Connection.TRANSACTION_READ_COMMITTED,
                Connection.TRANSACTION_REPEATABLE_READ,
                Connection.TRANSACTION_SERIALIZABLE,
        };
        for (int level : unsupported) {
            assertThrows(SQLFeatureNotSupportedException.class,
                    () -> conn.setTransactionIsolation(level));
        }
        // A failed set must not have disturbed the stored level.
        assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Test
    void closeDelegatesToCoreAndMarksClosed() throws SQLException {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
        assertEquals(1, core.closeCount);
    }

    @Test
    void closeIsIdempotent() throws SQLException {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        conn.close();
        conn.close();
        assertEquals(1, core.closeCount);
    }

    @Test
    void usingClosedConnectionThrows() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.close();
        assertThrows(SQLException.class, conn::createStatement);
    }

    @Test
    void isValidRunsSelectOne() {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        assertTrue(conn.isValid(1));
        assertEquals("SELECT 1", core.lastScalarSql);
    }

    @Test
    void isValidReturnsFalseWhenClosed() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.close();
        assertFalse(conn.isValid(1));
    }

    @Test
    void isValidNegativeTimeoutReturnsFalse() {
        // DEVIATION from jdbc-v2 (ConnectionTest#isValidTest throws SQLException for a
        // negative timeout): ChConnection.isValid is documented as lenient and reports
        // the connection invalid instead. Pinned here; no core probe must be issued.
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        assertFalse(conn.isValid(-1));
        assertNull(core.lastScalarSql, "a negative timeout must not reach the server");
    }

    @Test
    void isValidZeroTimeoutMeansNoLimitAndProbes() {
        // JDBC spec: 0 means "no timeout limit", so the probe still runs
        // (reference: jdbc-v2 ConnectionTest#isValidTest asserts isValid(0) is true).
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        assertTrue(conn.isValid(0));
        assertEquals("SELECT 1", core.lastScalarSql);
    }

    @Test
    void isValidReturnsFalseWhenCoreThrows() {
        FakeCore core = new FakeCore();
        core.scalarThrows = true;
        ChConnection conn = newConnection(core);
        assertFalse(conn.isValid(1));
    }

    // ------------------------------------------------------------------
    // Misc supported behaviour
    // ------------------------------------------------------------------

    @Test
    void readOnlyRoundTrips() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertFalse(conn.isReadOnly());
        conn.setReadOnly(true);
        assertTrue(conn.isReadOnly());
    }

    @Test
    void warningsAreNull() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertNull(conn.getWarnings());
        conn.clearWarnings();
    }

    @Test
    void unwrapToConnectionReturnsSelf() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertSame(conn, conn.unwrap(Connection.class));
        assertTrue(conn.isWrapperFor(Connection.class));
    }

    @Test
    void prepareCallIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareCall("CALL x()"));
    }

    @Test
    void prepareCallAllOverloadsUnsupported() {
        // Reference: jdbc-v2 ConnectionTest#testCreateUnsupportedStatements exercises
        // every prepareCall overload.
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareCall(
                "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.prepareCall(
                "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    @Test
    void nullInfoDoesNotThrow() throws SQLException {
        ChConnection conn = new ChConnection(new FakeCore(), "jdbc:chnative://h:9000/", null);
        assertTrue(conn.getAutoCommit());
        assertNull(conn.getCatalog());
    }

    // ------------------------------------------------------------------
    // nativeSQL
    // ------------------------------------------------------------------

    @Test
    void nativeSQLIsIdentityPassthrough() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        // The native protocol speaks plain SQL, so JDBC escape sequences are
        // deliberately passed through untranslated.
        String escaped = "SELECT {ts '2024-01-02 02:01:01'} as v1, {d '2024-01-02'} as v2";
        assertEquals(escaped, conn.nativeSQL(escaped));
        assertEquals("SELECT 1 as t", conn.nativeSQL("SELECT 1 as t"));
    }

    @Test
    void nativeSQLNullReturnsNull() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertNull(conn.nativeSQL(null));
    }

    @Test
    void nativeSQLOnClosedConnectionThrows() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.close();
        assertThrows(SQLException.class, () -> conn.nativeSQL("SELECT 1"));
    }

    // ------------------------------------------------------------------
    // Catalog / schema
    // ------------------------------------------------------------------

    @Test
    void setCatalogStoresValue() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        // ClickHouse models a "database" as the JDBC catalog; the value is stored.
        conn.setCatalog("catalog-name");
        assertEquals("catalog-name", conn.getCatalog());
    }

    @Test
    void setSchemaStoresValue() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setSchema("schema-name");
        assertEquals("schema-name", conn.getSchema());
    }

    // ------------------------------------------------------------------
    // Holdability
    // ------------------------------------------------------------------

    @Test
    void holdabilityDefaultsToCloseCursorsAtCommit() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
    }

    @Test
    void setHoldabilityAcceptsBothStandardValues() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
    }

    @Test
    void setHoldabilityInvalidValueThrows() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLException.class, () -> conn.setHoldability(-1));
    }

    // ------------------------------------------------------------------
    // Savepoints (unsupported)
    // ------------------------------------------------------------------

    @Test
    void setSavepointIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, conn::setSavepoint);
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.setSavepoint("savepoint-name"));
    }

    @Test
    void releaseSavepointIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.releaseSavepoint(null));
    }

    @Test
    void rollbackToSavepointIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.rollback(null));
    }

    // ------------------------------------------------------------------
    // LOB / SQLXML factories (unsupported)
    // ------------------------------------------------------------------

    @Test
    void createClobIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, conn::createClob);
    }

    @Test
    void createBlobIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, conn::createBlob);
    }

    @Test
    void createNClobIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, conn::createNClob);
    }

    @Test
    void createSQLXMLIsUnsupported() {
        ChConnection conn = newConnection(new FakeCore());
        assertThrows(SQLFeatureNotSupportedException.class, conn::createSQLXML);
    }

    // ------------------------------------------------------------------
    // Client info
    // ------------------------------------------------------------------

    @Test
    void clientInfoDefaultsToEmpty() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        assertTrue(conn.getClientInfo().isEmpty());
        assertNull(conn.getClientInfo("ApplicationName"));
    }

    @Test
    void setClientInfoStringRoundTrips() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setClientInfo("ApplicationName", "test 2");
        assertEquals("test 2", conn.getClientInfo("ApplicationName"));
        assertNull(conn.getClientInfo("unknown"));
    }

    @Test
    void setClientInfoNullValueRemovesKey() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setClientInfo("ApplicationName", "test");
        conn.setClientInfo("ApplicationName", null);
        assertNull(conn.getClientInfo("ApplicationName"));
    }

    @Test
    void setClientInfoPropertiesReplacesAllEntries() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setClientInfo("ApplicationName", "test");

        Properties replacement = new Properties();
        replacement.setProperty("ClientUser", "alice");
        conn.setClientInfo(replacement);
        assertEquals("alice", conn.getClientInfo("ClientUser"));
        assertNull(conn.getClientInfo("ApplicationName"));

        // An empty Properties clears everything; a null one resets to empty.
        conn.setClientInfo(new Properties());
        assertNull(conn.getClientInfo("ClientUser"));
        conn.setClientInfo((Properties) null);
        assertTrue(conn.getClientInfo().isEmpty());
    }

    @Test
    void getClientInfoReturnsDefensiveCopy() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.setClientInfo("ApplicationName", "app");
        Properties snapshot = conn.getClientInfo();
        snapshot.setProperty("ApplicationName", "mutated");
        assertEquals("app", conn.getClientInfo("ApplicationName"));
    }

    /**
     * {@code setClientInfo} on a closed connection throws
     * {@link SQLClientInfoException} for both overloads, per the JDBC spec (was
     * knownBug 25; jdbc-v2 ConnectionTest#setGetClientInfoTest asserts both).
     */
    @Test
    void setClientInfoOnClosedConnectionThrows() throws SQLException {
        ChConnection conn = newConnection(new FakeCore());
        conn.close();
        assertThrows(SQLClientInfoException.class,
                () -> conn.setClientInfo("ApplicationName", "app"),
                "setClientInfo(String,String) on a closed connection must throw");
        assertThrows(SQLClientInfoException.class,
                () -> conn.setClientInfo(new Properties()),
                "setClientInfo(Properties) on a closed connection must throw");
    }

    // ------------------------------------------------------------------
    // Abort
    // ------------------------------------------------------------------

    @Test
    void abortClosesConnection() throws SQLException {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        conn.abort(Executors.newSingleThreadExecutor());
        assertTrue(conn.isClosed());
        assertEquals(1, core.closeCount);
    }

    @Test
    void abortOnClosedConnectionIsNoOp() throws SQLException {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        conn.close();
        conn.abort(Executors.newSingleThreadExecutor());
        assertEquals(1, core.closeCount);
    }

    /**
     * {@code abort(null)} rejects the null executor with {@link SQLException} before
     * closing anything, per the JDBC spec (was knownBug 23; jdbc-v2
     * ConnectionTest#abortTest).
     */
    @Test
    void abortWithNullExecutorThrows() {
        FakeCore core = new FakeCore();
        ChConnection conn = newConnection(core);
        assertThrows(SQLException.class, () -> conn.abort(null),
                "abort(null) must reject the null executor");
    }

    // ------------------------------------------------------------------
    // Minimal in-memory fake of the core connection.
    // ------------------------------------------------------------------

    /**
     * Tiny test double implementing the core
     * {@link io.github.danielbunting.clickhouse.ClickHouseConnection} interface.
     * Only the methods exercised by {@link ChConnection} do anything useful;
     * the rest are inert.
     */
    private static final class FakeCore implements ClickHouseConnection {
        int closeCount;
        String lastScalarSql;
        boolean scalarThrows;

        @Override
        public long executeScalar(String sql) {
            this.lastScalarSql = sql;
            if (scalarThrows) {
                throw new RuntimeException("boom");
            }
            return 1L;
        }

        @Override
        public void execute(String sql) {
            // no-op
        }

        @Override
        public QueryResult query(String sql) {
            throw new UnsupportedOperationException("not needed for these tests");
        }

        @Override
        public <T> Stream<T> query(String sql, Class<T> type) {
            throw new UnsupportedOperationException("not needed for these tests");
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException("not needed for these tests");
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String sql) {
            throw new UnsupportedOperationException("not needed for these tests");
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
