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
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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
    void nullInfoDoesNotThrow() throws SQLException {
        ChConnection conn = new ChConnection(new FakeCore(), "jdbc:chnative://h:9000/", null);
        assertTrue(conn.getAutoCommit());
        assertNull(conn.getCatalog());
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
