package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryParameters;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

/**
 * A {@link java.sql.PreparedStatement} with two binding strategies, chosen per
 * connection by the {@code server_side_params} property:
 *
 * <ul>
 *   <li><b>Client-side interpolation (default).</b> Each {@code ?} placeholder outside
 *       a string literal is replaced with a safely-quoted SQL literal built from the
 *       bound parameter, and the resulting SQL is handed to the inherited
 *       {@link ChStatement} execution paths.</li>
 *   <li><b>Server-side parameters</b> ({@code server_side_params=true}). Each positional
 *       {@code ?} is rewritten to a typed placeholder {@code {_pN:String}} and the bound
 *       values travel separately on the Query packet, so the <em>server</em> casts each
 *       value from text to the placeholder type. This removes client-side string
 *       splicing entirely — no SQL-injection or quoting-fidelity hazard.
 *       <p><b>Limitation:</b> without per-column type inference every placeholder is
 *       declared {@code :String} and relies on ClickHouse's implicit cast from the
 *       textual form to the target column/expression type (which CH performs for most
 *       scalar types). Contexts that need an exact non-String parameter type, and the
 *       batch INSERT collapse, are not covered by this path and fall back to
 *       interpolation.</li>
 * </ul>
 *
 * <p>Batches accumulate full parameter rows. {@link #executeBatch()} collapses an
 * {@code INSERT ... VALUES (?...)} into a single multi-row
 * {@code INSERT ... VALUES (...),(...)} statement (interpolation path only).
 */
public class ChPreparedStatement extends ChStatement implements PreparedStatement {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Generated parameter-name prefix for the server-side rewrite: {@code _p1, _p2, …}. */
    static final String PARAM_NAME_PREFIX = "_p";

    private final String sql;
    private final int parameterCount;

    /**
     * Whether to bind via server-side query parameters instead of client-side
     * interpolation (per the {@code server_side_params} connection property).
     */
    private final boolean serverSideParams;

    /**
     * The SQL with {@code ?} rewritten to {@code {_pN:String}} placeholders, computed
     * once for the server-side path; {@code null} when interpolation is used.
     */
    private final String serverSideSql;

    /** Current parameter bindings, 1-based logically (index 0 unused). */
    private Object[] params;

    /** Accumulated parameter rows for {@link #addBatch()}/{@link #executeBatch()}. */
    private final List<Object[]> batchRows = new ArrayList<>();

    /**
     * Creates a prepared statement for the given parameterized SQL.
     *
     * @param conn the owning JDBC connection
     * @param sql  SQL text containing {@code ?} placeholders
     */
    public ChPreparedStatement(ChConnection conn, String sql) {
        super(conn);
        this.sql = sql;
        this.parameterCount = countPlaceholders(sql);
        this.params = new Object[parameterCount + 1];
        this.serverSideParams = conn.useServerSideParams();
        this.serverSideSql = this.serverSideParams ? rewriteToNamedParams(sql) : null;
    }

    // ---- placeholder counting / substitution -------------------------------

    /**
     * Counts {@code ?} placeholders that fall outside single-quoted string
     * literals. A doubled quote ({@code ''}) inside a literal is treated as an
     * escaped quote rather than a literal boundary.
     *
     * @param sql the SQL text
     * @return number of bindable placeholders
     */
    static int countPlaceholders(String sql) {
        int count = 0;
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++; // skip escaped quote
                } else {
                    inString = !inString;
                }
            } else if (c == '?' && !inString) {
                count++;
            }
        }
        return count;
    }

    /**
     * Substitutes the given parameter values (1-based, index 0 unused) into the
     * template, replacing each {@code ?} outside a string literal with a quoted
     * literal from {@link #toLiteral(Object)}.
     *
     * @param template the parameterized SQL
     * @param values   bindings; {@code values[1..n]} correspond to placeholders 1..n
     * @return the fully substituted SQL
     * @throws SQLException if a placeholder has no bound value
     */
    static String substitute(String template, Object[] values) throws SQLException {
        StringBuilder out = new StringBuilder(template.length() + 16);
        boolean inString = false;
        int param = 1;
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '\'') {
                out.append(c);
                if (inString && i + 1 < template.length() && template.charAt(i + 1) == '\'') {
                    out.append('\'');
                    i++;
                } else {
                    inString = !inString;
                }
            } else if (c == '?' && !inString) {
                if (param >= values.length) {
                    throw new SQLException("Missing value for parameter " + param);
                }
                out.append(toLiteral(values[param]));
                param++;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /**
     * Renders a single bound value as a SQL literal: {@code NULL} for null,
     * single-quoted with embedded quotes/backslashes escaped for textual values,
     * bare numerals for numbers and booleans (0/1), and {@code 'YYYY-MM-DD HH:MM:SS'}
     * for temporal values.
     *
     * @param value the bound value (may be {@code null})
     * @return the SQL literal text
     */
    static String toLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean b) {
            return b ? "1" : "0";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof byte[] bytes) {
            return quote(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        }
        if (value instanceof Timestamp ts) {
            LocalDateTime ldt = ts.toLocalDateTime();
            return quote(TIMESTAMP_FORMAT.format(ldt));
        }
        if (value instanceof Instant instant) {
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
            return quote(TIMESTAMP_FORMAT.format(ldt));
        }
        if (value instanceof Date date) {
            return quote(date.toLocalDate().toString());
        }
        if (value instanceof LocalDate ld) {
            return quote(ld.toString());
        }
        if (value instanceof Time time) {
            return quote(time.toString());
        }
        if (value instanceof UUID) {
            return quote(value.toString());
        }
        // Fallback: treat as string.
        return quote(value.toString());
    }

    /**
     * Single-quotes a value and escapes backslashes and embedded single quotes,
     * matching ClickHouse string-literal escaping.
     *
     * @param s the raw text
     * @return a quoted, escaped SQL string literal
     */
    static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '\'') {
                sb.append('\\');
            }
            sb.append(c);
        }
        sb.append('\'');
        return sb.toString();
    }

    /**
     * Rewrites positional {@code ?} placeholders (outside string literals) into named,
     * typed server-side placeholders {@code {_p1:String}}, {@code {_p2:String}}, …,
     * preserving everything else verbatim. The names line up with the values built by
     * {@link #buildServerSideParams(Object[])}.
     *
     * <p>Every placeholder is declared {@code :String}; the server casts the textual
     * value to the column/expression type at bind time (see the class limitation note).
     *
     * @param template the SQL with {@code ?} placeholders
     * @return the SQL with named typed placeholders
     */
    static String rewriteToNamedParams(String template) {
        StringBuilder out = new StringBuilder(template.length() + 16);
        boolean inString = false;
        int param = 1;
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '\'') {
                out.append(c);
                if (inString && i + 1 < template.length() && template.charAt(i + 1) == '\'') {
                    out.append('\'');
                    i++;
                } else {
                    inString = !inString;
                }
            } else if (c == '?' && !inString) {
                out.append('{').append(PARAM_NAME_PREFIX).append(param).append(":String}");
                param++;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /**
     * Builds the {@link QueryParameters} for the current bindings, mapping positional
     * parameter {@code n} to the generated name {@code _pN} and converting each value to
     * its ClickHouse textual form (a null value binds SQL NULL via the {@code \N}
     * sentinel).
     *
     * @param values bindings; {@code values[1..n]} correspond to placeholders 1..n
     * @return the server-side parameter set
     * @throws SQLException if a placeholder has no bound value
     */
    private QueryParameters buildServerSideParams(Object[] values) throws SQLException {
        QueryParameters.Builder b = QueryParameters.builder();
        for (int n = 1; n <= parameterCount; n++) {
            if (n >= values.length) {
                throw new SQLException("Missing value for parameter " + n);
            }
            b.bind(PARAM_NAME_PREFIX + n, values[n]);
        }
        return b.build();
    }

    /** Builds the effective SQL for the current single-row parameter set. */
    private String effectiveSql() throws SQLException {
        return substitute(sql, params);
    }

    private void setParam(int parameterIndex, Object value) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameterCount) {
            throw new SQLException(
                    "Parameter index " + parameterIndex + " out of range [1, " + parameterCount + "]");
        }
        params[parameterIndex] = value;
    }

    // ---- execution ----------------------------------------------------------

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (serverSideParams) {
            try {
                QueryParameters qp = buildServerSideParams(params);
                currentUpdateCount = -1;
                currentResultSet = new ChResultSet(conn.core().query(serverSideSql, qp));
                return currentResultSet;
            } catch (ClickHouseException e) {
                throw wrap(e);
            }
        }
        return super.executeQuery(effectiveSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (serverSideParams) {
            try {
                QueryParameters qp = buildServerSideParams(params);
                conn.core().execute(serverSideSql, qp);
                currentResultSet = null;
                currentUpdateCount = 0;
                return 0;
            } catch (ClickHouseException e) {
                throw wrap(e);
            }
        }
        return super.executeUpdate(effectiveSql());
    }

    @Override
    public boolean execute() throws SQLException {
        if (serverSideParams) {
            if (producesResultSet(serverSideSql)) {
                executeQuery();
                return true;
            }
            executeUpdate();
            return false;
        }
        return super.execute(effectiveSql());
    }

    @Override
    public void clearParameters() throws SQLException {
        params = new Object[parameterCount + 1];
    }

    @Override
    public void addBatch() throws SQLException {
        batchRows.add(params.clone());
        clearParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int rowCount = batchRows.size();
        if (rowCount == 0) {
            return new int[0];
        }
        // TODO: future optimization — route batch inserts through the core
        // BulkInserter for allocation-lean column-major sends instead of building
        // a textual multi-row INSERT.
        String trimmedUpper = sql.stripLeading().toUpperCase();
        try {
            if (serverSideParams) {
                // Server-side path: run each row as its own parameterized statement.
                // The multi-row INSERT collapse is interpolation-only (it depends on
                // splicing tuples into the VALUES clause).
                for (Object[] row : batchRows) {
                    conn.core().execute(serverSideSql, buildServerSideParams(row));
                }
            } else if (trimmedUpper.startsWith("INSERT")) {
                conn.core().execute(buildMultiRowInsert());
            } else {
                // Non-INSERT batches: execute each substituted statement in turn.
                for (Object[] row : batchRows) {
                    conn.core().execute(substitute(sql, row));
                }
            }
        } catch (ClickHouseException e) {
            throw wrap(e);
        } finally {
            batchRows.clear();
        }
        int[] results = new int[rowCount];
        java.util.Arrays.fill(results, SUCCESS_NO_INFO);
        return results;
    }

    /**
     * Builds a single {@code INSERT ... VALUES (...),(...)} statement from the
     * accumulated batch rows by reusing the leading {@code INSERT ... VALUES}
     * clause of the template and emitting one quoted tuple per row.
     *
     * @return the multi-row INSERT SQL
     * @throws SQLException if the template lacks a {@code VALUES} clause
     */
    private String buildMultiRowInsert() throws SQLException {
        int valuesIdx = indexOfValues(sql);
        if (valuesIdx < 0) {
            throw new SQLException("Batched INSERT must contain a VALUES clause");
        }
        // Everything up to and including the VALUES keyword is the prefix.
        int afterValues = valuesIdx + "VALUES".length();
        String prefix = sql.substring(0, afterValues);
        String tupleTemplate = sql.substring(afterValues).trim(); // e.g. "(?, ?, ?)"

        StringBuilder out = new StringBuilder();
        out.append(prefix).append(' ');
        for (int r = 0; r < batchRows.size(); r++) {
            if (r > 0) {
                out.append(',');
            }
            out.append(substitute(tupleTemplate, batchRows.get(r)));
        }
        return out.toString();
    }

    /** Finds the index of the {@code VALUES} keyword (case-insensitive) outside string literals. */
    private static int indexOfValues(String sql) {
        boolean inString = false;
        for (int i = 0; i + 6 <= sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inString = !inString;
            } else if (!inString && (c == 'V' || c == 'v')) {
                if (sql.regionMatches(true, i, "VALUES", 0, 6)) {
                    return i;
                }
            }
        }
        return -1;
    }

    // ---- setters ------------------------------------------------------------

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(parameterIndex, null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParam(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        setParam(parameterIndex, x);
    }

    // ---- metadata -----------------------------------------------------------

    @Override
    public ParameterMetaData getParameterMetaData() {
        return new ParameterMetaData() {
            @Override
            public int getParameterCount() {
                return parameterCount;
            }

            @Override
            public int isNullable(int param) {
                return parameterNullableUnknown;
            }

            @Override
            public boolean isSigned(int param) {
                return true;
            }

            @Override
            public int getPrecision(int param) {
                return 0;
            }

            @Override
            public int getScale(int param) {
                return 0;
            }

            @Override
            public int getParameterType(int param) {
                return java.sql.Types.OTHER;
            }

            @Override
            public String getParameterTypeName(int param) {
                return "String";
            }

            @Override
            public String getParameterClassName(int param) {
                return Object.class.getName();
            }

            @Override
            public int getParameterMode(int param) {
                return parameterModeIn;
            }

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
        };
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // ClickHouse native client cannot describe the result without executing.
        throw new SQLFeatureNotSupportedException(
                "Result-set metadata before execution is not supported");
    }

    // ---- unsupported stream / advanced setters ------------------------------

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw unsupported("setAsciiStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw unsupported("setAsciiStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw unsupported("setAsciiStream");
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw unsupported("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw unsupported("setBinaryStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw unsupported("setBinaryStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw unsupported("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        throw unsupported("setCharacterStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw unsupported("setCharacterStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw unsupported("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        throw unsupported("setNCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw unsupported("setNCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw unsupported("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw unsupported("setBlob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw unsupported("setBlob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw unsupported("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw unsupported("setClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw unsupported("setClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw unsupported("setClob");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw unsupported("setNClob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw unsupported("setNClob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw unsupported("setNClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw unsupported("setArray");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setParam(parameterIndex, value);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw unsupported("setRowId");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw unsupported("setSQLXML");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParam(parameterIndex, x == null ? null : x.toString());
    }

    private static SQLFeatureNotSupportedException unsupported(String op) {
        return new SQLFeatureNotSupportedException(op + " is not supported");
    }
}
