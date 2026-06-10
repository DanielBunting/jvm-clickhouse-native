package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A {@link java.sql.Connection} that wraps a native-TCP
 * {@link io.github.danielbunting.clickhouse.ClickHouseConnection}.
 *
 * <p>This is a thin adapter: every supported JDBC call delegates to the core
 * client. Statement and metadata objects are created lazily through the agreed
 * sibling classes {@link ChStatement}, {@link ChPreparedStatement}, and
 * {@link ChDatabaseMetaData}.
 *
 * <h2>Transaction semantics</h2>
 * ClickHouse has only limited, experimental transaction support and is not a
 * general-purpose transactional store. This driver therefore reports
 * auto-commit as {@code true} by default and treats {@link #commit()} and
 * {@link #rollback()} as best-effort no-ops: there is no client-side buffering
 * of statements, so each statement is independently sent to the server. Calling
 * {@code commit()}/{@code rollback()} while in auto-commit mode simply does
 * nothing rather than raising an error, which keeps naive JDBC tooling working.
 *
 * <p>Instances are not thread-safe, mirroring the single-caller contract of the
 * underlying core connection.
 */
public final class ChConnection implements Connection {

    private final ClickHouseConnection core;
    private final String url;
    private final Properties info;

    private boolean closed;
    private boolean autoCommit = true;
    private boolean readOnly;
    private String catalog;
    private String schema;
    private int transactionIsolation = Connection.TRANSACTION_NONE;
    private int holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    private Properties clientInfo = new Properties();

    /**
     * Wraps an already-opened core connection.
     *
     * @param core the live native ClickHouse connection; must not be {@code null}
     * @param url  the original JDBC URL the driver was given (for diagnostics)
     * @param info the connection properties supplied to the driver; may be {@code null}
     */
    public ChConnection(ClickHouseConnection core, String url, Properties info) {
        if (core == null) {
            throw new NullPointerException("core connection must not be null");
        }
        this.core = core;
        this.url = url;
        this.info = info != null ? info : new Properties();
        // Derive the default catalog/schema from the "database" property, if present.
        String db = this.info.getProperty("database");
        this.catalog = db;
        this.schema = db;
    }

    /**
     * Returns the underlying core connection. Package-private so sibling JDBC
     * classes (statements, metadata) can issue native queries.
     *
     * @return the wrapped {@link io.github.danielbunting.clickhouse.ClickHouseConnection}
     */
    ClickHouseConnection core() {
        return core;
    }

    /** Returns the original JDBC URL this connection was opened with. */
    String url() {
        return url;
    }

    /**
     * Whether prepared statements should bind via <em>server-side</em> query parameters
     * ({@code {name:Type}} placeholders sent on the wire) instead of the default
     * client-side literal interpolation. Controlled by the connection property
     * {@code server_side_params=true}; defaults to {@code false} so the proven
     * interpolation path stays the default.
     *
     * @return {@code true} to use server-side parameter binding
     */
    boolean useServerSideParams() {
        return Boolean.parseBoolean(info.getProperty("server_side_params", "false"));
    }

    // ------------------------------------------------------------------
    // Statement factories
    // ------------------------------------------------------------------

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new ChStatement(this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        requireForwardReadOnly(resultSetType, resultSetConcurrency);
        return new ChStatement(this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkOpen();
        requireForwardReadOnly(resultSetType, resultSetConcurrency);
        return new ChStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new ChPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkOpen();
        requireForwardReadOnly(resultSetType, resultSetConcurrency);
        return new ChPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkOpen();
        requireForwardReadOnly(resultSetType, resultSetConcurrency);
        return new ChPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        return new ChPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();
        return new ChPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        return new ChPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("ClickHouse does not support stored procedures");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("ClickHouse does not support stored procedures");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("ClickHouse does not support stored procedures");
    }

    // ------------------------------------------------------------------
    // Metadata
    // ------------------------------------------------------------------

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return new ChDatabaseMetaData(this);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        // The native protocol speaks plain SQL; no JDBC escape translation is needed.
        return sql;
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            core.close();
        } catch (RuntimeException e) {
            throw new SQLException("Failed to close ClickHouse connection", e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isValid(int timeout) {
        if (timeout < 0) {
            // Spec: negative timeout is illegal; be lenient and report invalid.
            return false;
        }
        if (closed) {
            return false;
        }
        try {
            core.executeScalar("SELECT 1");
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // ------------------------------------------------------------------
    // Transactions (limited; see class Javadoc)
    // ------------------------------------------------------------------

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        // Best-effort no-op: statements are sent eagerly, so there is nothing to
        // flush. ClickHouse offers only limited transaction support.
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        // Best-effort no-op; ClickHouse cannot roll back already-executed statements.
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException(
                    "ClickHouse only supports TRANSACTION_NONE");
        }
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return transactionIsolation;
    }

    // ------------------------------------------------------------------
    // Read-only / catalog / schema
    // ------------------------------------------------------------------

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        // ClickHouse models a "database" as the JDBC catalog.
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return catalog;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return schema;
    }

    // ------------------------------------------------------------------
    // Warnings
    // ------------------------------------------------------------------

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    // ------------------------------------------------------------------
    // Holdability
    // ------------------------------------------------------------------

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT
                && holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException("Invalid holdability: " + holdability);
        }
        this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return holdability;
    }

    // ------------------------------------------------------------------
    // Type map
    // ------------------------------------------------------------------

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported");
    }

    // ------------------------------------------------------------------
    // Client info
    // ------------------------------------------------------------------

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (value == null) {
            clientInfo.remove(name);
        } else {
            clientInfo.setProperty(name, value);
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.clientInfo = properties != null ? properties : new Properties();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkOpen();
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
        return (Properties) clientInfo.clone();
    }

    // ------------------------------------------------------------------
    // Network timeout / abort
    // ------------------------------------------------------------------

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout is not supported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout is not supported");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        // Closing is sufficient to release resources for this thin client.
        close();
    }

    // ------------------------------------------------------------------
    // LOB / structured factories (unsupported)
    // ------------------------------------------------------------------

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOBs are not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOBs are not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOBs are not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf is not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct is not supported");
    }

    // ------------------------------------------------------------------
    // Wrapper
    // ------------------------------------------------------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        if (iface.isInstance(core)) {
            return iface.cast(core);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || iface.isInstance(core);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    /**
     * Validates that the requested {@link ResultSet} type/concurrency is the only
     * combination this forward-only, read-only driver can honour.
     */
    private void requireForwardReadOnly(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException(
                    "Only TYPE_FORWARD_ONLY result sets are supported");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException(
                    "Only CONCUR_READ_ONLY result sets are supported");
        }
    }
}
