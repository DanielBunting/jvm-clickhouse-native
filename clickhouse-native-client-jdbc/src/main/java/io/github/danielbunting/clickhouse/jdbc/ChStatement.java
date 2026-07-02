package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.ClickHouseException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * A thin {@link java.sql.Statement} over the core
 * {@link io.github.danielbunting.clickhouse.ClickHouseConnection}.
 *
 * <p>Queries (SELECT/SHOW/DESC/WITH) are routed to {@code core().query(...)} and
 * wrapped in a {@link ChResultSet}; everything else is routed to
 * {@code core().execute(...)}. The vast majority of {@code java.sql.Statement}
 * surface that ClickHouse cannot meaningfully support throws
 * {@link SQLFeatureNotSupportedException}.
 *
 * <p>Not thread-safe; mirrors the single-threaded core connection.
 */
public class ChStatement implements Statement {

    /** Owning JDBC connection; also the source of the core connection. */
    protected final ChConnection conn;

    /** The most recently produced result set, exposed via {@link #getResultSet()}. */
    protected ChResultSet currentResultSet;

    /** Update count of the most recent {@code executeUpdate}/non-query {@code execute}. */
    protected int currentUpdateCount = -1;

    private final List<String> batch = new ArrayList<>();
    private int maxRows;
    private int fetchSize;
    private int queryTimeoutSeconds;
    private boolean escapeProcessing = true;
    private boolean closed;

    /**
     * Creates a statement bound to the given JDBC connection.
     *
     * @param conn the owning {@link ChConnection}; must not be {@code null}
     */
    public ChStatement(ChConnection conn) {
        this.conn = conn;
    }

    /**
     * Returns {@code true} when the (trimmed) SQL is a result-producing statement —
     * one beginning with {@code SELECT}, {@code SHOW}, {@code DESC}/{@code DESCRIBE},
     * {@code EXISTS}, or a {@code WITH} CTE. Used by {@link #execute(String)} to
     * decide between {@link #executeQuery(String)} and {@link #executeUpdate(String)}.
     *
     * @param sql the SQL to classify (may be {@code null})
     * @return whether the statement yields a {@link ResultSet}
     */
    protected static boolean producesResultSet(String sql) {
        if (sql == null) {
            return false;
        }
        String trimmed = sql.stripLeading();
        // Skip a leading line comment / block comment is out of scope for v1.
        String upper = trimmed.toUpperCase();
        return upper.startsWith("SELECT")
                || upper.startsWith("SHOW")
                || upper.startsWith("DESC")
                || upper.startsWith("EXISTS")
                || upper.startsWith("WITH");
    }

    /**
     * Wraps a core failure as a {@link SQLException} preserving the cause.
     *
     * @param e the core exception
     * @return a {@link SQLException} to throw
     */
    protected static SQLException wrap(RuntimeException e) {
        return new SQLException(e.getMessage(), e);
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    /**
     * Per-query server settings derived from statement state: a positive
     * {@link #setQueryTimeout} travels as {@code max_execution_time}, so the SERVER
     * aborts the query (TIMEOUT_EXCEEDED, code 159) — surfaced as an
     * {@link java.sql.SQLTimeoutException} by {@code wrap}.
     *
     * @return the settings map, or {@code null} when no statement setting applies
     */
    private java.util.Map<String, String> perQuerySettings() {
        if (queryTimeoutSeconds > 0) {
            return java.util.Map.of("max_execution_time", String.valueOf(queryTimeoutSeconds));
        }
        return null;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        try {
            currentUpdateCount = -1;
            java.util.Map<String, String> settings = perQuerySettings();
            currentResultSet = new ChResultSet(settings == null
                    ? conn.core().query(sql)
                    : conn.core().query(sql, settings));
            return currentResultSet;
        } catch (ClickHouseException e) {
            throw wrap(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        try {
            java.util.Map<String, String> settings = perQuerySettings();
            if (settings == null) {
                conn.core().execute(sql);
            } else {
                conn.core().execute(sql, settings);
            }
            currentResultSet = null;
            currentUpdateCount = 0;
            return 0;
        } catch (ClickHouseException e) {
            throw wrap(e);
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        if (producesResultSet(sql)) {
            executeQuery(sql);
            return true;
        }
        executeUpdate(sql);
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkOpen();
        // A single statement never produces multiple results here.
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        currentUpdateCount = -1;
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkOpen();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        int[] results = new int[batch.size()];
        try {
            for (int i = 0; i < batch.size(); i++) {
                conn.core().execute(batch.get(i));
                results[i] = SUCCESS_NO_INFO;
            }
        } catch (ClickHouseException e) {
            throw wrap(e);
        } finally {
            batch.clear();
        }
        return results;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return conn;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    // ---- stored, mostly-advisory knobs -------------------------------------

    @Override
    public int getMaxRows() throws SQLException {
        checkOpen();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkOpen();
        if (max < 0) {
            throw new SQLException("maxRows must be >= 0");
        }
        this.maxRows = max;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("fetchSize must be >= 0");
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkOpen();
        return queryTimeoutSeconds;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkOpen();
        this.queryTimeoutSeconds = seconds;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkOpen();
        this.escapeProcessing = enable;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // ---- warnings: ClickHouse native client surfaces none ------------------

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors are not supported");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkOpen();
        // Unbounded; ignore.
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkOpen();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkOpen();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkOpen();
        return false;
    }

    // ---- generated keys / explicit autogen overloads: unsupported ----------

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public void cancel() throws SQLException {
        // Cross-thread: sends a Cancel packet to the server for the in-flight query.
        conn.core().cancel();
    }

    // ---- Wrapper -----------------------------------------------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
