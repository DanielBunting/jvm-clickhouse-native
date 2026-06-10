package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChDatabaseMetaData}.
 *
 * <p>All tests run entirely in-process against a {@link FakeCore} — no ClickHouse
 * server is required.
 */
class ChDatabaseMetaDataTest {

    private static final String JDBC_URL = "jdbc:chnative://localhost:9000/testdb";

    private ChConnection conn;
    private ChDatabaseMetaData meta;

    @BeforeEach
    void setUp() {
        Properties info = new Properties();
        info.setProperty("database", "testdb");
        conn = new ChConnection(new FakeCore(), JDBC_URL, info);
        meta = new ChDatabaseMetaData(conn);
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    @Test
    void constructorRejectsNull() {
        assertThrows(NullPointerException.class, () -> new ChDatabaseMetaData(null));
    }

    // -----------------------------------------------------------------------
    // Product / driver identity
    // -----------------------------------------------------------------------

    @Test
    void databaseProductNameIsClickHouse() {
        assertEquals("ClickHouse", meta.getDatabaseProductName());
    }

    @Test
    void driverNameIsCorrect() {
        assertEquals("clickhouse-native-client-jdbc", meta.getDriverName());
    }

    @Test
    void driverVersionIsNonEmpty() {
        assertNotNull(meta.getDriverVersion());
        assertFalse(meta.getDriverVersion().isEmpty());
    }

    @Test
    void driverMajorVersionMatchesVersionString() {
        String[] parts = meta.getDriverVersion().split("\\.", 2);
        assertEquals(meta.getDriverMajorVersion(), Integer.parseInt(parts[0]));
    }

    @Test
    void jdbcMajorVersionIsFour() {
        assertEquals(4, meta.getJDBCMajorVersion());
    }

    @Test
    void jdbcCompliantReturnsFalse() throws SQLException {
        // DatabaseMetaData does not define jdbcCompliant(); we verify via the driver.
        ClickHouseDriver driver = new ClickHouseDriver();
        assertFalse(driver.jdbcCompliant());
    }

    // -----------------------------------------------------------------------
    // Transaction support
    // -----------------------------------------------------------------------

    @Test
    void supportsTransactionsIsFalse() {
        assertFalse(meta.supportsTransactions());
    }

    @Test
    void defaultTransactionIsolationIsNone() {
        assertEquals(Connection.TRANSACTION_NONE, meta.getDefaultTransactionIsolation());
    }

    @Test
    void supportsTransactionIsolationNone() {
        assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
    }

    @Test
    void doesNotSupportOtherTransactionIsolationLevels() {
        assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
    }

    // -----------------------------------------------------------------------
    // Batch updates
    // -----------------------------------------------------------------------

    @Test
    void supportsBatchUpdatesIsTrue() {
        assertTrue(meta.supportsBatchUpdates());
    }

    // -----------------------------------------------------------------------
    // Identifier quoting
    // -----------------------------------------------------------------------

    @Test
    void identifierQuoteStringIsDoubleQuote() {
        assertEquals("\"", meta.getIdentifierQuoteString());
    }

    // -----------------------------------------------------------------------
    // Result set types
    // -----------------------------------------------------------------------

    @Test
    void supportsForwardOnlyResultSet() {
        assertTrue(meta.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
    }

    @Test
    void doesNotSupportScrollableResultSets() {
        assertFalse(meta.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(meta.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
    }

    @Test
    void supportsForwardOnlyReadOnlyConcurrency() {
        assertTrue(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    void doesNotSupportUpdatableConcurrency() {
        assertFalse(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
    }

    // -----------------------------------------------------------------------
    // Read-only / isReadOnly
    // -----------------------------------------------------------------------

    @Test
    void isReadOnlyIsFalse() {
        assertFalse(meta.isReadOnly());
    }

    // -----------------------------------------------------------------------
    // SQL keywords
    // -----------------------------------------------------------------------

    @Test
    void getSQLKeywordsReturnsEmptyString() {
        assertEquals("", meta.getSQLKeywords());
    }

    // -----------------------------------------------------------------------
    // URL / connection
    // -----------------------------------------------------------------------

    @Test
    void getUrlReturnsOriginalJdbcUrl() {
        assertEquals(JDBC_URL, meta.getURL());
    }

    @Test
    void getConnectionReturnsSameConnection() {
        assertSame(conn, meta.getConnection());
    }

    // -----------------------------------------------------------------------
    // Catalog / schema queries — all must throw SQLFeatureNotSupportedException
    // -----------------------------------------------------------------------

    // NOTE: getTables/getColumns/getSchemas/getCatalogs/getTypeInfo/getPrimaryKeys are now
    // IMPLEMENTED (backed by system.* queries) — see feat/jdbc-meta. Their behaviour requires
    // a live server, so the happy-path assertions live in the JDBC integration tests; the old
    // "throws SQLFeatureNotSupportedException" unit tests were removed because they asserted
    // the previous (unsupported) behaviour. getProcedures/getIndexInfo remain unsupported below.

    @Test
    void getProceduresThrowsFeatureNotSupported() {
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> meta.getProcedures(null, null, null));
    }

    @Test
    void getIndexInfoThrowsFeatureNotSupported() {
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> meta.getIndexInfo(null, null, "t", false, false));
    }

    // -----------------------------------------------------------------------
    // Unwrap
    // -----------------------------------------------------------------------

    @Test
    void unwrapToDatabaseMetaDataReturnsSelf() throws SQLException {
        assertSame(meta, meta.unwrap(DatabaseMetaData.class));
    }

    @Test
    void unwrapToUnknownTypeThrows() {
        assertThrows(SQLException.class, () -> meta.unwrap(String.class));
    }

    @Test
    void isWrapperForDatabaseMetaDataIsTrue() {
        assertTrue(meta.isWrapperFor(DatabaseMetaData.class));
    }

    @Test
    void isWrapperForUnknownTypeIsFalse() {
        assertFalse(meta.isWrapperFor(String.class));
    }

    // -----------------------------------------------------------------------
    // Misc capability flags sampled for completeness
    // -----------------------------------------------------------------------

    @Test
    void supportsGroupByIsTrue() {
        assertTrue(meta.supportsGroupBy());
    }

    @Test
    void supportsUnionAllIsTrue() {
        assertTrue(meta.supportsUnionAll());
    }

    @Test
    void supportsStoredProceduresIsFalse() {
        assertFalse(meta.supportsStoredProcedures());
    }

    @Test
    void supportsSavepointsIsFalse() {
        assertFalse(meta.supportsSavepoints());
    }

    // -----------------------------------------------------------------------
    // Minimal in-memory fake of the core connection
    // -----------------------------------------------------------------------

    /**
     * Test double for {@link ClickHouseConnection}.
     * Only implements the subset of methods needed by {@link ChConnection}'s
     * own constructor and helpers; all other methods throw.
     */
    private static final class FakeCore implements ClickHouseConnection {

        @Override
        public long executeScalar(String sql) {
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
            // no-op
        }
    }
}
