package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

/**
 * A {@link DatabaseMetaData} implementation that describes the capabilities of this
 * ClickHouse JDBC driver and the connected server.
 *
 * <h2>Design goals</h2>
 * <p>This is an honest, stub-quality implementation. Where ClickHouse differs from
 * traditional RDBMS semantics the values returned reflect the actual capabilities of
 * the engine rather than aspirationally claiming full JDBC compliance. Specifically:
 * <ul>
 *   <li>Transactions are not supported ({@link #supportsTransactions()} = {@code false}).
 *   <li>The driver is not JDBC-compliant ({@link #jdbcCompliant()} on the driver returns
 *       {@code false}).
 *   <li>Heavy catalog-query methods ({@link #getTables}, {@link #getColumns}, etc.) that
 *       would require round-trips to the ClickHouse system tables throw
 *       {@link SQLFeatureNotSupportedException} rather than return empty result sets.
 *       Callers that need structural metadata should query {@code system.tables},
 *       {@code system.columns}, etc. directly via a regular statement.
 * </ul>
 *
 * <h2>Thread safety</h2>
 * <p>Instances are not thread-safe, mirroring the contract of the parent
 * {@link ChConnection}.
 */
public final class ChDatabaseMetaData implements DatabaseMetaData {

    private final ChConnection conn;

    /** Cached result of {@code SELECT version()}, lazily fetched once per instance. */
    private String serverVersion;
    private boolean serverVersionFetched;

    /**
     * Creates metadata for the given JDBC connection.
     *
     * @param conn the owning {@link ChConnection}; must not be {@code null}
     */
    public ChDatabaseMetaData(ChConnection conn) {
        if (conn == null) {
            throw new NullPointerException("conn must not be null");
        }
        this.conn = conn;
    }

    /**
     * Returns the underlying core connection used to run {@code system.*} queries.
     */
    private ClickHouseConnection core() {
        return conn.core();
    }

    // -----------------------------------------------------------------------
    // Driver / product identity
    // -----------------------------------------------------------------------

    @Override
    public String getDatabaseProductName() {
        return "ClickHouse";
    }

    /**
     * Returns the server version string from {@code SELECT version()} (e.g.
     * {@code "25.6.1.100"}), fetched once and cached on this instance. If the query
     * fails (e.g. the connection is unusable) an empty string is returned rather than
     * propagating the error, matching the lenient contract of {@link DatabaseMetaData}.
     *
     * @return the server version string, or {@code ""} if it could not be determined
     */
    @Override
    public String getDatabaseProductVersion() {
        String v = serverVersion();
        return v == null ? "" : v;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return versionPart(0);
    }

    @Override
    public int getDatabaseMinorVersion() {
        return versionPart(1);
    }

    /**
     * Lazily runs {@code SELECT version()} once and caches the raw string. Swallows
     * any error (returning {@code null}) so the boolean/string metadata accessors stay
     * non-throwing.
     */
    private String serverVersion() {
        if (!serverVersionFetched) {
            serverVersionFetched = true;
            try (QueryResult r = core().query("SELECT version()")) {
                serverVersion = firstString(r);
            } catch (RuntimeException e) {
                serverVersion = null;
            }
        }
        return serverVersion;
    }

    /**
     * Parses the dotted server version and returns the zero-based component, or 0 when
     * absent/unparseable. Component 0 is the major, 1 the minor (ClickHouse uses a
     * {@code YY.M.patch.build} scheme, so "major" is the calendar year).
     */
    private int versionPart(int index) {
        String v = serverVersion();
        if (v == null || v.isEmpty()) {
            return 0;
        }
        String[] parts = v.split("\\.");
        if (index >= parts.length) {
            return 0;
        }
        try {
            return Integer.parseInt(parts[index].trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public String getDriverName() {
        return "clickhouse-native-client-jdbc";
    }

    @Override
    public String getDriverVersion() {
        return getMajorVersion() + "." + getMinorVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return getMinorVersion();
    }

    /** Returns the JDBC major version supported by this driver: 4. */
    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    /** Returns the JDBC minor version: 2 (Java 8 baseline). */
    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    private int getMajorVersion() {
        return 1;
    }

    private int getMinorVersion() {
        return 0;
    }

    // -----------------------------------------------------------------------
    // Connection / URL
    // -----------------------------------------------------------------------

    @Override
    public String getURL() {
        return conn.url();
    }

    @Override
    public String getUserName() throws SQLException {
        return conn.getClientInfo("user");
    }

    @Override
    public Connection getConnection() {
        return conn;
    }

    // -----------------------------------------------------------------------
    // General capabilities
    // -----------------------------------------------------------------------

    @Override
    public boolean isReadOnly() {
        return false;
    }

    /**
     * ClickHouse does not support ANSI/SQL transactions; always returns {@code false}.
     */
    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    // -----------------------------------------------------------------------
    // SQL / identifier rules
    // -----------------------------------------------------------------------

    /**
     * Returns {@code "\""} — ClickHouse uses double-quotes to delimit identifiers.
     */
    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    /**
     * Returns an empty string; ClickHouse shares almost all ANSI SQL keywords so
     * advertising extras would be misleading.
     */
    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    // -----------------------------------------------------------------------
    // SQL feature support
    // -----------------------------------------------------------------------

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    // -----------------------------------------------------------------------
    // Limits
    // -----------------------------------------------------------------------

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    // -----------------------------------------------------------------------
    // Result set types
    // -----------------------------------------------------------------------

    /**
     * Only {@link ResultSet#TYPE_FORWARD_ONLY} is supported; all other types return {@code false}.
     *
     * @param type the result set type constant
     * @return {@code true} only for {@link ResultSet#TYPE_FORWARD_ONLY}
     */
    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY
                && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    // -----------------------------------------------------------------------
    // Misc boolean features
    // -----------------------------------------------------------------------

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsRefCursors() {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    // -----------------------------------------------------------------------
    // Catalog / schema / table queries — unsupported in this stub
    // -----------------------------------------------------------------------

    /**
     * Not supported in this stub implementation. Applications that need a list of
     * procedures should query {@code system.functions} directly.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedures is not supported; "
                + "query system.functions directly");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
            String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedureColumns is not supported");
    }

    /**
     * Lists tables/views from {@code system.tables}, shaped to the JDBC contract.
     *
     * <p>The result has the standard columns {@code TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
     * TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME,
     * REF_GENERATION}, ordered by {@code TABLE_TYPE, TABLE_CAT, TABLE_NAME} per the spec.
     * ClickHouse "database" is exposed as the JDBC catalog; there is no schema layer so
     * {@code TABLE_SCHEM} is always {@code NULL}. {@code TABLE_TYPE} is {@code 'VIEW'} for
     * view engines ({@code View}/{@code MaterializedView}/{@code LiveView}) and
     * {@code 'TABLE'} otherwise.
     *
     * @param catalog          database to restrict to, or {@code null}/{@code ""} for all
     * @param schemaPattern    ignored (ClickHouse has no schemas)
     * @param tableNamePattern a SQL LIKE pattern, or {@code null} for all
     * @param types            JDBC table types to include (e.g. {@code TABLE}, {@code VIEW}),
     *                         or {@code null} for all
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
            String[] types) throws SQLException {
        StringBuilder sql = new StringBuilder(
                "SELECT database AS TABLE_CAT, "
                + "CAST(NULL AS Nullable(String)) AS TABLE_SCHEM, "
                + "name AS TABLE_NAME, "
                + "if(engine LIKE '%View', 'VIEW', 'TABLE') AS TABLE_TYPE, "
                + "CAST(comment AS String) AS REMARKS, "
                + "CAST(NULL AS Nullable(String)) AS TYPE_CAT, "
                + "CAST(NULL AS Nullable(String)) AS TYPE_SCHEM, "
                + "CAST(NULL AS Nullable(String)) AS TYPE_NAME, "
                + "CAST(NULL AS Nullable(String)) AS SELF_REFERENCING_COL_NAME, "
                + "CAST(NULL AS Nullable(String)) AS REF_GENERATION "
                + "FROM system.tables WHERE 1=1");
        appendCatalogFilter(sql, "database", catalog);
        appendLikeFilter(sql, "name", tableNamePattern);
        if (types != null && types.length > 0) {
            sql.append(" AND if(engine LIKE '%View', 'VIEW', 'TABLE') IN (")
               .append(inList(types)).append(')');
        }
        sql.append(" ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_NAME");
        return runMeta(sql.toString());
    }

    /**
     * Returns an empty result with the spec columns {@code TABLE_SCHEM, TABLE_CATALOG}.
     * ClickHouse has no schema layer, so this is always empty (but valid, not throwing).
     */
    @Override
    public ResultSet getSchemas() throws SQLException {
        return runMeta(
                "SELECT CAST(NULL AS Nullable(String)) AS TABLE_SCHEM, "
                + "CAST(NULL AS Nullable(String)) AS TABLE_CATALOG WHERE 1=0");
    }

    /**
     * Returns an empty schema result (ClickHouse has no schema layer). The
     * {@code catalog}/{@code schemaPattern} arguments are ignored.
     */
    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    /**
     * Lists ClickHouse databases as JDBC catalogs (a CH "database" is the JDBC catalog,
     * as confirmed by {@link ChConnection#getCatalog()}). The single column is
     * {@code TABLE_CAT}, ordered ascending per the spec.
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        return runMeta("SELECT name AS TABLE_CAT FROM system.databases ORDER BY TABLE_CAT");
    }

    /**
     * Returns the table types this driver reports: {@code TABLE} and {@code VIEW}.
     * Single column {@code TABLE_TYPE}, ordered ascending.
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        return runMeta(
                "SELECT 'TABLE' AS TABLE_TYPE UNION ALL SELECT 'VIEW' ORDER BY TABLE_TYPE");
    }

    /**
     * Lists columns from {@code system.columns}, shaped to the JDBC contract.
     *
     * <p>Columns: {@code TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
     * TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE,
     * REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH,
     * ORDINAL_POSITION, IS_NULLABLE, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
     * SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN}, ordered by
     * {@code TABLE_CAT, TABLE_NAME, ORDINAL_POSITION} per the spec.
     *
     * <p>{@code DATA_TYPE} is resolved client-side from the raw ClickHouse type string via
     * {@link JdbcValues#sqlType(String)} (the server-side {@code system.columns.type} is the
     * source of truth); a {@code Nullable(...)} wrapper drives {@code NULLABLE}/{@code IS_NULLABLE}.
     *
     * @param catalog           database to restrict to, or {@code null}/{@code ""} for all
     * @param schemaPattern     ignored (ClickHouse has no schemas)
     * @param tableNamePattern  a SQL LIKE pattern on table name, or {@code null} for all
     * @param columnNamePattern a SQL LIKE pattern on column name, or {@code null} for all
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        // DATA_TYPE / NULLABLE / IS_NULLABLE depend on the raw CH type string, which is
        // hard to map purely in SQL; compute them client-side via a multiValueMatch-free
        // helper. We select the raw bits and re-shape in a wrapping ChResultSet-style view.
        // Simpler: select all needed columns including the raw type, then derive the JDBC
        // shape with CH expressions that mirror JdbcValues.sqlType for the common cases,
        // and fold the precise mapping in the SQL using a CASE over type families.
        StringBuilder sql = new StringBuilder(
                "SELECT database AS TABLE_CAT, "
                + "CAST(NULL AS Nullable(String)) AS TABLE_SCHEM, "
                + "table AS TABLE_NAME, "
                + "name AS COLUMN_NAME, "
                + dataTypeCaseExpr() + " AS DATA_TYPE, "
                + "type AS TYPE_NAME, "
                + "toInt32(0) AS COLUMN_SIZE, "
                + "CAST(NULL AS Nullable(Int32)) AS BUFFER_LENGTH, "
                + "numeric_scale AS DECIMAL_DIGITS, "
                + "toInt32(10) AS NUM_PREC_RADIX, "
                + "if(startsWith(type, 'Nullable('), 1, 0) AS NULLABLE, "
                + "CAST(comment AS String) AS REMARKS, "
                + "CAST(default_expression AS Nullable(String)) AS COLUMN_DEF, "
                + "CAST(NULL AS Nullable(Int32)) AS SQL_DATA_TYPE, "
                + "CAST(NULL AS Nullable(Int32)) AS SQL_DATETIME_SUB, "
                + "CAST(NULL AS Nullable(Int32)) AS CHAR_OCTET_LENGTH, "
                + "toInt32(position) AS ORDINAL_POSITION, "
                + "if(startsWith(type, 'Nullable('), 'YES', 'NO') AS IS_NULLABLE, "
                + "CAST(NULL AS Nullable(String)) AS SCOPE_CATALOG, "
                + "CAST(NULL AS Nullable(String)) AS SCOPE_SCHEMA, "
                + "CAST(NULL AS Nullable(String)) AS SCOPE_TABLE, "
                + "CAST(NULL AS Nullable(Int16)) AS SOURCE_DATA_TYPE, "
                + "'NO' AS IS_AUTOINCREMENT, "
                + "'NO' AS IS_GENERATEDCOLUMN "
                + "FROM system.columns WHERE 1=1");
        appendCatalogFilter(sql, "database", catalog);
        appendLikeFilter(sql, "table", tableNamePattern);
        appendLikeFilter(sql, "name", columnNamePattern);
        sql.append(" ORDER BY TABLE_CAT, TABLE_NAME, ORDINAL_POSITION");
        return runMeta(sql.toString());
    }

    /**
     * Builds a server-side {@code multiIf} that mirrors {@link JdbcValues#sqlType(String)}
     * for the common ClickHouse type families, returning the {@link java.sql.Types} int.
     * Operates on the un-wrapped element type so {@code Nullable(T)}/{@code LowCardinality(T)}
     * resolve to the inner type's JDBC code.
     */
    private static String dataTypeCaseExpr() {
        // Strip Nullable(...) and LowCardinality(...) wrappers (one level is enough for
        // the families CH stores in system.columns) before matching.
        String base = "replaceRegexpOne("
                + "replaceRegexpOne(type, '^Nullable\\\\((.*)\\\\)$', '\\\\1'), "
                + "'^LowCardinality\\\\((.*)\\\\)$', '\\\\1')";
        return "toInt32(multiIf("
                + "startsWith(" + base + ", 'Array('), " + Types.ARRAY + ", "
                + "startsWith(" + base + ", 'FixedString'), " + Types.VARCHAR + ", "
                + "startsWith(" + base + ", 'Decimal'), " + Types.DECIMAL + ", "
                + "startsWith(" + base + ", 'DateTime'), " + Types.TIMESTAMP + ", "
                + "startsWith(" + base + ", 'Enum'), " + Types.VARCHAR + ", "
                + base + " IN ('UInt8','Int16','UInt16'), " + Types.SMALLINT + ", "
                + base + " = 'Int8', " + Types.TINYINT + ", "
                + base + " = 'Int32', " + Types.INTEGER + ", "
                + base + " IN ('UInt32','Int64','UInt64'), " + Types.BIGINT + ", "
                + base + " IN ('Int128','UInt128','Int256','UInt256'), " + Types.DECIMAL + ", "
                + base + " = 'Float32', " + Types.REAL + ", "
                + base + " = 'Float64', " + Types.DOUBLE + ", "
                + base + " = 'String', " + Types.VARCHAR + ", "
                + base + " IN ('Date','Date32'), " + Types.DATE + ", "
                + Types.OTHER + "))";
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
            String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getColumnPrivileges is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTablePrivileges is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
            boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBestRowIdentifier is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getVersionColumns is not supported");
    }

    /**
     * Returns an empty, spec-shaped result. ClickHouse has no enforced primary-key
     * constraints (the {@code ORDER BY}/sorting key is a storage detail, not a unique
     * key), so reporting none is the honest, tool-safe answer. Columns:
     * {@code TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME}.
     */
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        return runMeta(
                "SELECT CAST(NULL AS Nullable(String)) AS TABLE_CAT, "
                + "CAST(NULL AS Nullable(String)) AS TABLE_SCHEM, "
                + "CAST(NULL AS Nullable(String)) AS TABLE_NAME, "
                + "CAST(NULL AS Nullable(String)) AS COLUMN_NAME, "
                + "CAST(NULL AS Nullable(Int16)) AS KEY_SEQ, "
                + "CAST(NULL AS Nullable(String)) AS PK_NAME WHERE 1=0");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getImportedKeys is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getExportedKeys is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
            String parentTable, String foreignCatalog, String foreignSchema,
            String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCrossReference is not supported");
    }

    /**
     * Returns a static description of the major ClickHouse data types, shaped to the JDBC
     * contract (18 columns: {@code TYPE_NAME, DATA_TYPE, PRECISION, LITERAL_PREFIX,
     * LITERAL_SUFFIX, CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE,
     * UNSIGNED_ATTRIBUTE, FIXED_PREC_SCALE, AUTO_INCREMENT, LOCAL_TYPE_NAME, MINIMUM_SCALE,
     * MAXIMUM_SCALE, SQL_DATA_TYPE, SQL_DATETIME_SUB, NUM_PREC_RADIX}), ordered by
     * {@code DATA_TYPE} per the spec.
     *
     * <p>The result is produced as a {@code SELECT ... UNION ALL ...} of literal rows run
     * through the core connection, so it carries proper native column typing and flows
     * through the normal {@link ChResultSet} path with no bespoke ResultSet implementation.
     * Every type is reported {@code typeNullable} (CH wraps any type in {@code Nullable})
     * and {@code typeSearchable}; numeric types use radix 10.
     */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        StringBuilder sql = new StringBuilder();
        // name, java.sql.Types, precision, unsigned, radix
        Object[][] rows = {
            {"Int8", Types.TINYINT, 3, 0, 10},
            {"UInt8", Types.SMALLINT, 3, 1, 10},
            {"Int16", Types.SMALLINT, 5, 0, 10},
            {"UInt16", Types.INTEGER, 5, 1, 10},
            {"Int32", Types.INTEGER, 10, 0, 10},
            {"UInt32", Types.BIGINT, 10, 1, 10},
            {"Int64", Types.BIGINT, 19, 0, 10},
            {"UInt64", Types.NUMERIC, 20, 1, 10},
            {"Float32", Types.REAL, 7, 0, 10},
            {"Float64", Types.DOUBLE, 15, 0, 10},
            {"Decimal", Types.DECIMAL, 76, 0, 10},
            {"String", Types.VARCHAR, 0, 0, 0},
            {"FixedString", Types.VARCHAR, 0, 0, 0},
            {"UUID", Types.OTHER, 36, 0, 0},
            {"Date", Types.DATE, 10, 0, 0},
            {"Date32", Types.DATE, 10, 0, 0},
            {"DateTime", Types.TIMESTAMP, 19, 0, 0},
            {"DateTime64", Types.TIMESTAMP, 29, 0, 0},
            {"Enum8", Types.VARCHAR, 0, 0, 0},
            {"Enum16", Types.VARCHAR, 0, 0, 0},
            {"Array", Types.ARRAY, 0, 0, 0},
        };
        for (int i = 0; i < rows.length; i++) {
            Object[] r = rows[i];
            String name = (String) r[0];
            int dataType = (Integer) r[1];
            int precision = (Integer) r[2];
            int unsigned = (Integer) r[3];
            int radix = (Integer) r[4];
            boolean caseSensitive = dataType == Types.VARCHAR || dataType == Types.OTHER;
            if (i == 0) {
                sql.append("SELECT ");
            } else {
                sql.append(" UNION ALL SELECT ");
            }
            sql.append(quote(name)).append(" AS TYPE_NAME, ")
               .append("toInt32(").append(dataType).append(") AS DATA_TYPE, ")
               .append("toInt32(").append(precision).append(") AS PRECISION, ")
               .append("CAST(NULL AS Nullable(String)) AS LITERAL_PREFIX, ")
               .append("CAST(NULL AS Nullable(String)) AS LITERAL_SUFFIX, ")
               .append("CAST(NULL AS Nullable(String)) AS CREATE_PARAMS, ")
               .append("toInt16(").append(DatabaseMetaData.typeNullable).append(") AS NULLABLE, ")
               .append("toUInt8(").append(caseSensitive ? 1 : 0).append(") AS CASE_SENSITIVE, ")
               .append("toInt16(").append(DatabaseMetaData.typeSearchable).append(") AS SEARCHABLE, ")
               .append("toUInt8(").append(unsigned).append(") AS UNSIGNED_ATTRIBUTE, ")
               .append("toUInt8(0) AS FIXED_PREC_SCALE, ")
               .append("toUInt8(0) AS AUTO_INCREMENT, ")
               .append("CAST(NULL AS Nullable(String)) AS LOCAL_TYPE_NAME, ")
               .append("toInt16(0) AS MINIMUM_SCALE, ")
               .append("toInt16(0) AS MAXIMUM_SCALE, ")
               .append("CAST(NULL AS Nullable(Int32)) AS SQL_DATA_TYPE, ")
               .append("CAST(NULL AS Nullable(Int32)) AS SQL_DATETIME_SUB, ")
               .append("toInt32(").append(radix).append(") AS NUM_PREC_RADIX");
        }
        sql.append(" ORDER BY DATA_TYPE");
        return runMeta(sql.toString());
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
            boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException("getIndexInfo is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
            int[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUDTs is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTypes is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTables is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAttributes is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfoProperties is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getFunctions is not supported; "
                + "query system.functions directly");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
            String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFunctionColumns is not supported");
    }

    /**
     * Not supported in this stub implementation.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getPseudoColumns is not supported");
    }

    // -----------------------------------------------------------------------
    // RowId
    // -----------------------------------------------------------------------

    /**
     * ClickHouse has no RowId concept; returns {@link RowIdLifetime#ROWID_UNSUPPORTED}.
     */
    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    // -----------------------------------------------------------------------
    // Metadata-query helpers (package-visible bits are static for unit testing)
    // -----------------------------------------------------------------------

    /**
     * Runs a metadata-shaping SQL query through the core connection and wraps the lazy
     * result in a forward-only {@link ChResultSet}.
     *
     * @param sql the fully-formed SQL (already escaped) producing the JDBC-shaped columns
     * @return a {@link ResultSet} over the result
     * @throws SQLException if the query fails
     */
    private ResultSet runMeta(String sql) throws SQLException {
        try {
            QueryResult result = core().query(sql);
            return new ChResultSet(result);
        } catch (RuntimeException e) {
            throw new SQLException("Failed to run metadata query: " + sql, e);
        }
    }

    /** Reads the first column of the first row of a result as a string, or {@code null}. */
    private static String firstString(QueryResult result) {
        var blocks = result.blocks();
        while (blocks.hasNext()) {
            var block = blocks.next();
            if (block == null || block.isEmpty()) {
                continue;
            }
            Object v = block.column(0).value(0);
            return v == null ? null : String.valueOf(v);
        }
        return null;
    }

    /**
     * Appends {@code AND <column> = '<catalog>'} when {@code catalog} is a non-empty
     * filter. A {@code null} or empty catalog means "all databases" (the JDBC convention).
     */
    static void appendCatalogFilter(StringBuilder sql, String column, String catalog) {
        if (catalog != null && !catalog.isEmpty()) {
            sql.append(" AND ").append(column).append(" = ").append(quote(catalog));
        }
    }

    /**
     * Appends an {@code AND <column> LIKE '<pattern>'} clause for a JDBC name pattern, or
     * nothing when {@code pattern} is {@code null} (meaning "no filter"). The JDBC LIKE
     * wildcards {@code %} and {@code _} pass through to ClickHouse's {@code LIKE} verbatim;
     * the value is single-quote-escaped so it cannot break out of the literal. A pattern of
     * {@code "%"} is treated as "match all" and skipped for efficiency.
     */
    static void appendLikeFilter(StringBuilder sql, String column, String pattern) {
        if (pattern == null || pattern.equals("%")) {
            return;
        }
        sql.append(" AND ").append(column).append(" LIKE ").append(quote(pattern));
    }

    /** Builds a comma-separated list of quoted literals for an {@code IN (...)} clause. */
    static String inList(String[] values) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(quote(values[i] == null ? "" : values[i]));
        }
        return b.toString();
    }

    /**
     * Renders a Java string as a single-quoted ClickHouse string literal, escaping
     * backslashes and single quotes (ClickHouse uses C-style backslash escaping in string
     * literals). A {@code null} becomes the SQL literal {@code NULL}.
     */
    static String quote(String value) {
        if (value == null) {
            return "NULL";
        }
        StringBuilder b = new StringBuilder(value.length() + 2);
        b.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' || c == '\'') {
                b.append('\\');
            }
            b.append(c);
        }
        b.append('\'');
        return b.toString();
    }

    // -----------------------------------------------------------------------
    // Wrapper
    // -----------------------------------------------------------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsSharding() {
        return false;
    }

    @Override
    public long getMaxLogicalLobSize() {
        return 0;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
