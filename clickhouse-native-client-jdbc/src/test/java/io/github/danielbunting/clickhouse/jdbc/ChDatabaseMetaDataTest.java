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
    // Broad capability-flag sweep (ported from jdbc-v2 DatabaseMetaDataTest
    // #testSupportFlags). Where this driver deliberately deviates from the
    // reference the actual value is pinned with a comment.
    // -----------------------------------------------------------------------

    @Test
    void identifierStorageFlags() {
        assertTrue(meta.supportsMixedCaseIdentifiers());
        assertFalse(meta.storesUpperCaseIdentifiers());
        // DEVIATION: reference reports storesLowerCaseIdentifiers()=true /
        // storesMixedCaseIdentifiers()=false; ClickHouse identifiers are in fact
        // case-sensitive, so this driver reports mixed-case storage.
        assertFalse(meta.storesLowerCaseIdentifiers());
        assertTrue(meta.storesMixedCaseIdentifiers());
        // DEVIATION: reference reports supportsMixedCaseQuotedIdentifiers()=false.
        assertTrue(meta.supportsMixedCaseQuotedIdentifiers());
        assertFalse(meta.storesUpperCaseQuotedIdentifiers());
        assertFalse(meta.storesLowerCaseQuotedIdentifiers());
        assertTrue(meta.storesMixedCaseQuotedIdentifiers());
        assertEquals("\\", meta.getSearchStringEscape());
        assertEquals("", meta.getExtraNameCharacters());
    }

    @Test
    void nullSortingAndSelectability() {
        assertFalse(meta.allProceduresAreCallable());
        assertTrue(meta.allTablesAreSelectable());
        assertFalse(meta.nullsAreSortedHigh());
        assertTrue(meta.nullsAreSortedLow());
        assertFalse(meta.nullsAreSortedAtStart());
        // DEVIATION: reference reports nullsAreSortedAtEnd()=true.
        assertFalse(meta.nullsAreSortedAtEnd());
        assertFalse(meta.usesLocalFiles());
        assertFalse(meta.usesLocalFilePerTable());
        assertTrue(meta.nullPlusNonNullIsNull());
    }

    @Test
    void sqlGrammarAndJoinFlags() {
        assertTrue(meta.supportsAlterTableWithAddColumn());
        assertTrue(meta.supportsAlterTableWithDropColumn());
        assertTrue(meta.supportsColumnAliasing());
        assertFalse(meta.supportsConvert());
        assertFalse(meta.supportsConvert(java.sql.Types.INTEGER, java.sql.Types.VARCHAR));
        assertTrue(meta.supportsTableCorrelationNames());
        assertFalse(meta.supportsDifferentTableCorrelationNames());
        assertTrue(meta.supportsExpressionsInOrderBy());
        assertTrue(meta.supportsOrderByUnrelated());
        assertTrue(meta.supportsGroupByUnrelated());
        assertTrue(meta.supportsGroupByBeyondSelect());
        // DEVIATION: reference reports supportsLikeEscapeClause()=true.
        assertFalse(meta.supportsLikeEscapeClause());
        assertTrue(meta.supportsNonNullableColumns());
        assertTrue(meta.supportsMinimumSQLGrammar());
        // DEVIATION: reference claims core/extended grammar support.
        assertFalse(meta.supportsCoreSQLGrammar());
        assertFalse(meta.supportsExtendedSQLGrammar());
        assertFalse(meta.supportsANSI92EntryLevelSQL());
        assertFalse(meta.supportsANSI92IntermediateSQL());
        assertFalse(meta.supportsANSI92FullSQL());
        assertFalse(meta.supportsIntegrityEnhancementFacility());
        assertTrue(meta.supportsOuterJoins());
        assertTrue(meta.supportsFullOuterJoins());
        assertTrue(meta.supportsLimitedOuterJoins());
        assertTrue(meta.supportsUnion());
        assertFalse(meta.supportsMultipleResultSets());
        assertFalse(meta.supportsMultipleOpenResults());
        assertFalse(meta.supportsMultipleTransactions());
    }

    @Test
    void subqueryFlags() {
        assertTrue(meta.supportsSubqueriesInComparisons());
        // DEVIATION: reference reports supportsSubqueriesInExists()=false; ClickHouse
        // does support EXISTS subqueries and this driver says so.
        assertTrue(meta.supportsSubqueriesInExists());
        assertTrue(meta.supportsSubqueriesInIns());
        // DEVIATION: reference reports supportsSubqueriesInQuantifieds()=true.
        assertFalse(meta.supportsSubqueriesInQuantifieds());
        assertTrue(meta.supportsCorrelatedSubqueries());
    }

    /**
     * A ClickHouse database maps to the JDBC <em>catalog</em> in this driver (there is
     * no schema layer), so catalogs are supported in DML/DDL and schemas are not. The
     * reference driver's configurable databaseTerm/schema_term property is N/A here.
     */
    @Test
    void schemaAndCatalogSupportFlags() {
        assertEquals("schema", meta.getSchemaTerm());
        assertEquals("database", meta.getCatalogTerm());
        assertEquals("procedure", meta.getProcedureTerm());
        assertTrue(meta.isCatalogAtStart());
        assertEquals(".", meta.getCatalogSeparator());
        assertFalse(meta.supportsSchemasInDataManipulation());
        assertFalse(meta.supportsSchemasInProcedureCalls());
        assertFalse(meta.supportsSchemasInTableDefinitions());
        assertFalse(meta.supportsSchemasInIndexDefinitions());
        assertFalse(meta.supportsSchemasInPrivilegeDefinitions());
        assertTrue(meta.supportsCatalogsInDataManipulation());
        assertFalse(meta.supportsCatalogsInProcedureCalls());
        assertTrue(meta.supportsCatalogsInTableDefinitions());
        assertFalse(meta.supportsCatalogsInIndexDefinitions());
        assertFalse(meta.supportsCatalogsInPrivilegeDefinitions());
    }

    @Test
    void updateAndCursorFlags() {
        assertFalse(meta.supportsPositionedDelete());
        assertFalse(meta.supportsPositionedUpdate());
        assertFalse(meta.supportsSelectForUpdate());
        assertFalse(meta.supportsOpenCursorsAcrossCommit());
        assertFalse(meta.supportsOpenCursorsAcrossRollback());
        // DEVIATION: reference reports false for both; statements here are
        // independent of the (non-existent) transaction boundary.
        assertTrue(meta.supportsOpenStatementsAcrossCommit());
        assertTrue(meta.supportsOpenStatementsAcrossRollback());
        assertFalse(meta.supportsStoredFunctionsUsingCallSyntax());
        assertFalse(meta.autoCommitFailureClosesAllResultSets());
        assertFalse(meta.dataDefinitionCausesTransactionCommit());
        assertFalse(meta.dataDefinitionIgnoredInTransactions());
        assertFalse(meta.supportsDataManipulationTransactionsOnly());
        assertFalse(meta.supportsDataDefinitionAndDataManipulationTransactions());
    }

    /**
     * All limits are 0 ("unknown / no fixed limit"). DEVIATION: the reference reports
     * fixed values for some (maxConnections=150, maxColumnsInTable=1000, ...).
     */
    @Test
    void allLimitsReportZero() {
        assertEquals(0, meta.getMaxBinaryLiteralLength());
        assertEquals(0, meta.getMaxCharLiteralLength());
        assertEquals(0, meta.getMaxColumnNameLength());
        assertEquals(0, meta.getMaxColumnsInGroupBy());
        assertEquals(0, meta.getMaxColumnsInIndex());
        assertEquals(0, meta.getMaxColumnsInOrderBy());
        assertEquals(0, meta.getMaxColumnsInSelect());
        assertEquals(0, meta.getMaxColumnsInTable());
        assertEquals(0, meta.getMaxConnections());
        assertEquals(0, meta.getMaxCursorNameLength());
        assertEquals(0, meta.getMaxIndexLength());
        assertEquals(0, meta.getMaxSchemaNameLength());
        assertEquals(0, meta.getMaxProcedureNameLength());
        assertEquals(0, meta.getMaxCatalogNameLength());
        assertEquals(0, meta.getMaxRowSize());
        assertFalse(meta.doesMaxRowSizeIncludeBlobs());
        assertEquals(0, meta.getMaxStatementLength());
        assertEquals(0, meta.getMaxStatements());
        assertEquals(0, meta.getMaxTableNameLength());
        assertEquals(0, meta.getMaxTablesInSelect());
        assertEquals(0, meta.getMaxUserNameLength());
        assertEquals(0L, meta.getMaxLogicalLobSize());
    }

    @Test
    void holdabilityStateTypeAndRowId() {
        assertTrue(meta.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertFalse(meta.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, meta.getResultSetHoldability());
        assertEquals(DatabaseMetaData.sqlStateSQL, meta.getSQLStateType());
        assertEquals(java.sql.RowIdLifetime.ROWID_UNSUPPORTED, meta.getRowIdLifetime());
        assertFalse(meta.locatorsUpdateCopy());
        assertEquals(2, meta.getJDBCMinorVersion());
    }

    @Test
    void visibilityAndDetectionFlagsAllFalse() {
        int[] types = {ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.TYPE_SCROLL_SENSITIVE};
        for (int type : types) {
            assertFalse(meta.ownUpdatesAreVisible(type));
            assertFalse(meta.ownDeletesAreVisible(type));
            assertFalse(meta.ownInsertsAreVisible(type));
            assertFalse(meta.othersUpdatesAreVisible(type));
            assertFalse(meta.othersDeletesAreVisible(type));
            assertFalse(meta.othersInsertsAreVisible(type));
            assertFalse(meta.updatesAreDetected(type));
            assertFalse(meta.deletesAreDetected(type));
            assertFalse(meta.insertsAreDetected(type));
        }
    }

    @Test
    void generatedKeysAndMiscFlags() {
        assertFalse(meta.supportsGetGeneratedKeys());
        assertFalse(meta.generatedKeyAlwaysReturned());
        assertFalse(meta.supportsNamedParameters());
        assertFalse(meta.supportsRefCursors());
        assertFalse(meta.supportsStatementPooling());
        assertFalse(meta.supportsSharding());
    }

    /**
     * DEVIATION: the reference advertises long comma-separated function inventories;
     * this driver honestly returns empty strings (query {@code system.functions}
     * instead).
     */
    @Test
    void functionListsAreEmpty() {
        assertEquals("", meta.getNumericFunctions());
        assertEquals("", meta.getStringFunctions());
        assertEquals("", meta.getSystemFunctions());
        assertEquals("", meta.getTimeDateFunctions());
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
