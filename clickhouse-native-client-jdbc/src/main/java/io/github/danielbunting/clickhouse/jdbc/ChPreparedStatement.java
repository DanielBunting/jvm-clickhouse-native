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

    /**
     * Sub-second variant used when a temporal binding carries a non-zero fractional
     * part, so {@code DateTime64} precision survives client-side interpolation. Nine
     * fractional digits (nanoseconds) match the codec's maximum {@code DateTime64(9)};
     * ClickHouse truncates the extra digits for lower-precision columns.
     */
    private static final DateTimeFormatter TIMESTAMP_FRAC_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    /** Generated parameter-name prefix for the server-side rewrite: {@code _p1, _p2, …}. */
    static final String PARAM_NAME_PREFIX = "_p";

    private final String sql;

    /**
     * The bindable {@code ?} offsets of {@link #sql}, computed once at construction
     * ({@link #placeholderPositions}) and shared by every operation on the immutable
     * template — substitution, the server-side rewrite, and parameter counting.
     * ({@code buildMultiRowInsert}'s tuple template is a different string and keeps
     * its own scan.)
     */
    private final List<Integer> placeholderOffsets;

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
        this.placeholderOffsets = placeholderPositions(sql);
        this.parameterCount = placeholderOffsets.size();
        this.params = new Object[parameterCount + 1];
        this.serverSideParams = conn.useServerSideParams();
        this.serverSideSql =
                this.serverSideParams ? rewriteToNamedParams(sql, placeholderOffsets, null) : null;
    }

    // ---- placeholder scanning / counting / substitution ---------------------

    /**
     * Scans the SQL and returns the offsets of every {@code ?} that is a real,
     * bindable JDBC placeholder. This is the single scanner shared by
     * {@link #countPlaceholders}, {@link #substitute} and
     * {@link #rewriteToNamedParams}, and it understands:
     *
     * <ul>
     *   <li>single-quoted string literals, with both the doubled-quote ({@code ''})
     *       and backslash ({@code \'}, {@code \\}) escapes;</li>
     *   <li>backtick- and double-quoted identifiers, with the doubled-character
     *       escapes ({@code ``}, {@code ""}) and backslash escapes;</li>
     *   <li>{@code --}, {@code #} and {@code #!} line comments and nesting
     *       {@code /* *}{@code /} block comments (comment text is plain text);</li>
     *   <li>the {@code ?::type} cast adjacency (the {@code ::} is not a ternary
     *       colon);</li>
     *   <li>the {@code :} inside a braced {@code {name:Type}} server-side parameter
     *       (the name/Type separator, never a ternary colon); and</li>
     *   <li>the ClickHouse ternary operator {@code cond ? a : b}, disambiguated in a
     *       single left-to-right pass: each ternary-eligible {@code :} pairs with the
     *       NEAREST preceding unpaired {@code ?} at the same nesting depth, and a
     *       {@code ?} whose expression ends (at a same-depth {@code ,}/{@code ;}, the
     *       enclosing bracket's close, or end of text) without being paired is a
     *       placeholder.</li>
     * </ul>
     *
     * @param sql the SQL text
     * @return the offsets of the bindable {@code ?} characters, in order
     */
    static List<Integer> placeholderPositions(String sql) {
        int n = sql.length();
        // Offsets of every real-text '?', in order; ternary '?'s are struck out
        // (by index into this list) as their pairing ':' arrives.
        List<Integer> questionMarks = new ArrayList<>();
        java.util.BitSet ternary = new java.util.BitSet();
        // Bracket-opener stack; its length is the current nesting depth.
        StringBuilder openers = new StringBuilder();
        // Per depth, the indexes (into questionMarks) of '?'s still eligible to be a
        // ternary condition at that depth, in scan order.
        List<java.util.ArrayDeque<Integer>> pending = new ArrayList<>();
        pending.add(new java.util.ArrayDeque<>());
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            int depth = openers.length();
            if (c == '\'' || c == '`' || c == '"') {
                i = skipQuoted(sql, i);
            } else if (c == '#' || (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-')) {
                i = skipLineComment(sql, i);
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i = skipBlockComment(sql, i);
            } else if (c == '(' || c == '[' || c == '{') {
                openers.append(c);
                if (pending.size() <= openers.length()) {
                    pending.add(new java.util.ArrayDeque<>());
                }
                i++;
            } else if (c == ')' || c == ']' || c == '}') {
                if (depth > 0) {
                    // The bracket's expression ends: its still-unpaired '?'s are
                    // placeholders for good.
                    pending.get(depth).clear();
                    openers.setLength(depth - 1);
                }
                i++;
            } else if (c == ',' || c == ';') {
                // An expression boundary at this depth: its unpaired '?'s stay
                // placeholders.
                pending.get(depth).clear();
                i++;
            } else if (c == '?') {
                pending.get(depth).addLast(questionMarks.size());
                questionMarks.add(i);
                i++;
            } else if (c == ':') {
                if (i + 1 < n && sql.charAt(i + 1) == ':') {
                    i += 2; // '::' cast operator — never a ternary colon
                } else {
                    // Inside braces the ':' is a {name:Type} separator, not a ternary
                    // colon; elsewhere it claims the nearest preceding unpaired '?'
                    // at this depth (if any) as a ternary condition.
                    boolean insideBraces = depth > 0 && openers.charAt(depth - 1) == '{';
                    if (!insideBraces && !pending.get(depth).isEmpty()) {
                        ternary.set(pending.get(depth).removeLast());
                    }
                    i++;
                }
            } else {
                i++;
            }
        }
        List<Integer> positions = new ArrayList<>(questionMarks.size());
        for (int q = 0; q < questionMarks.size(); q++) {
            if (!ternary.get(q)) {
                positions.add(questionMarks.get(q));
            }
        }
        return positions;
    }

    /**
     * Skips a quoted region (string literal or quoted identifier) starting at the
     * opening quote; returns the index just past the closing quote. A backslash
     * consumes the following character; a doubled quote stays inside the region.
     * An unterminated region consumes the rest of the text.
     */
    private static int skipQuoted(String sql, int start) {
        char q = sql.charAt(start);
        int n = sql.length();
        int i = start + 1;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\\' && i + 1 < n) {
                i += 2;
            } else if (c == q) {
                if (i + 1 < n && sql.charAt(i + 1) == q) {
                    i += 2;
                } else {
                    return i + 1;
                }
            } else {
                i++;
            }
        }
        return n;
    }

    /** Skips a {@code --}/{@code #}/{@code #!} line comment through its newline. */
    private static int skipLineComment(String sql, int start) {
        int nl = sql.indexOf('\n', start);
        return nl < 0 ? sql.length() : nl + 1;
    }

    /** Skips a (nesting) block comment; an unterminated one consumes the rest. */
    private static int skipBlockComment(String sql, int start) {
        int n = sql.length();
        int depth = 1;
        int i = start + 2;
        while (i < n && depth > 0) {
            if (sql.charAt(i) == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                depth++;
                i += 2;
            } else if (sql.charAt(i) == '*' && i + 1 < n && sql.charAt(i + 1) == '/') {
                depth--;
                i += 2;
            } else {
                i++;
            }
        }
        return i;
    }

    /**
     * Counts the bindable {@code ?} placeholders in the SQL (see
     * {@link #placeholderPositions} for exactly what the scan understands).
     *
     * @param sql the SQL text
     * @return number of bindable placeholders
     */
    static int countPlaceholders(String sql) {
        return placeholderPositions(sql).size();
    }

    /**
     * Substitutes the given parameter values (1-based, index 0 unused) into the
     * template, replacing each bindable {@code ?} with a quoted literal from
     * {@link #toLiteral(Object)}. Literals, identifiers, comments and ternary
     * question marks pass through verbatim.
     *
     * @param template the parameterized SQL
     * @param values   bindings; {@code values[1..n]} correspond to placeholders 1..n
     * @return the fully substituted SQL
     * @throws SQLException if a placeholder has no bound value
     */
    static String substitute(String template, Object[] values) throws SQLException {
        return substitute(template, placeholderPositions(template), values);
    }

    /**
     * Variant of {@link #substitute(String, Object[])} taking the template's
     * precomputed placeholder offsets, so callers holding an immutable template (the
     * prepared statement itself) scan it only once.
     */
    private static String substitute(String template, List<Integer> positions, Object[] values)
            throws SQLException {
        StringBuilder out = new StringBuilder(template.length() + 16);
        int prev = 0;
        int param = 1;
        for (int pos : positions) {
            out.append(template, prev, pos);
            if (param >= values.length) {
                throw new SQLException("Missing value for parameter " + param);
            }
            out.append(toLiteral(values[param]));
            param++;
            prev = pos + 1;
        }
        out.append(template, prev, template.length());
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
            return quote(formatDateTime(ts.toLocalDateTime()));
        }
        if (value instanceof Instant instant) {
            return quote(formatDateTime(LocalDateTime.ofInstant(instant, ZoneOffset.UTC)));
        }
        // Zone-carrying temporals render as their UTC wall clock, matching the Instant
        // branch above (the zone shifts the instant, then drops out of the literal).
        if (value instanceof java.time.ZonedDateTime zdt) {
            return quote(formatDateTime(LocalDateTime.ofInstant(zdt.toInstant(), ZoneOffset.UTC)));
        }
        if (value instanceof java.time.OffsetDateTime odt) {
            return quote(formatDateTime(LocalDateTime.ofInstant(odt.toInstant(), ZoneOffset.UTC)));
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
        // IPv4/IPv6: the canonical numeric address (InetAddress.toString would prepend
        // the hostname). ClickHouse casts the quoted string to IPv4/IPv6.
        if (value instanceof java.net.InetAddress inet) {
            return quote(inet.getHostAddress());
        }
        // Collections and arrays (except byte[], handled above) render as a ClickHouse
        // array literal, e.g. ['a','b'] / [1,2] / [] (see issue clickhouse-java#2329).
        if (value instanceof java.util.Collection || value.getClass().isArray()) {
            return arrayLiteral(value);
        }
        // Maps render as a ClickHouse map literal, e.g. {'a': 1, 'b': 2}, recursing
        // through toLiteral for keys and values (mirrors the Collection branch above).
        if (value instanceof java.util.Map<?, ?> map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (java.util.Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(toLiteral(e.getKey())).append(": ").append(toLiteral(e.getValue()));
            }
            return sb.append('}').toString();
        }
        // Fallback: treat as string.
        return quote(value.toString());
    }

    /**
     * Formats a temporal value as a ClickHouse date-time literal, appending a
     * nine-digit (nanosecond) fractional part only when the value carries sub-second
     * precision.
     *
     * @param ldt the local date-time
     * @return {@code yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]}
     */
    static String formatDateTime(LocalDateTime ldt) {
        return (ldt.getNano() != 0 ? TIMESTAMP_FRAC_FORMAT : TIMESTAMP_FORMAT).format(ldt);
    }

    /**
     * Renders a {@link java.util.Collection} or Java array (not {@code byte[]}) as a
     * ClickHouse array literal, recursing through {@link #toLiteral(Object)} for each
     * element (so strings are quoted, numbers bare, nested arrays bracketed).
     *
     * @param value a {@code Collection} or array value
     * @return the array literal, e.g. {@code ['a','b']}
     */
    static String arrayLiteral(Object value) {
        StringBuilder sb = new StringBuilder("[");
        if (value instanceof java.util.Collection<?> c) {
            int i = 0;
            for (Object e : c) {
                if (i++ > 0) {
                    sb.append(',');
                }
                sb.append(toLiteral(e));
            }
        } else {
            int n = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(toLiteral(java.lang.reflect.Array.get(value, i)));
            }
        }
        return sb.append(']').toString();
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
     * Rewrites bindable {@code ?} placeholders (see {@link #placeholderPositions})
     * into named, typed server-side placeholders {@code {_p1:String}},
     * {@code {_p2:String}}, …, preserving everything else verbatim. The names line up
     * with the values built by {@link #buildServerSideParams(Object[])}.
     *
     * <p>Every placeholder is declared {@code :String}; the server casts the textual
     * value to the column/expression type at bind time (see the class limitation note).
     *
     * @param template the SQL with {@code ?} placeholders
     * @return the SQL with named typed placeholders
     */
    static String rewriteToNamedParams(String template) {
        return rewriteToNamedParams(template, null);
    }

    /**
     * Bindings-aware variant of {@link #rewriteToNamedParams(String)}: a parameter
     * whose bound value is {@code null} (or that is unbound) is declared
     * {@code {_pN:Nullable(String)}} so the server accepts the {@code \N} null
     * sentinel; non-null parameters keep the plain {@code :String} declaration.
     *
     * @param template the SQL with {@code ?} placeholders
     * @param values   current bindings ({@code values[1..n]}), or {@code null} to
     *                 declare every placeholder {@code :String}
     * @return the SQL with named typed placeholders
     */
    static String rewriteToNamedParams(String template, Object[] values) {
        return rewriteToNamedParams(template, placeholderPositions(template), values);
    }

    /**
     * Variant of {@link #rewriteToNamedParams(String, Object[])} taking the template's
     * precomputed placeholder offsets (see {@link #placeholderOffsets}).
     */
    private static String rewriteToNamedParams(
            String template, List<Integer> positions, Object[] values) {
        StringBuilder out = new StringBuilder(template.length() + 16);
        int prev = 0;
        int param = 1;
        for (int pos : positions) {
            out.append(template, prev, pos);
            boolean nullable = values != null && (param >= values.length || values[param] == null);
            out.append('{').append(PARAM_NAME_PREFIX).append(param)
                    .append(nullable ? ":Nullable(String)}" : ":String}");
            param++;
            prev = pos + 1;
        }
        out.append(template, prev, template.length());
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
        return substitute(sql, placeholderOffsets, params);
    }

    private void setParam(int parameterIndex, Object value) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameterCount) {
            throw new SQLException(
                    "Parameter index " + parameterIndex + " out of range [1, " + parameterCount + "]");
        }
        params[parameterIndex] = value;
    }

    /**
     * The effective server-side SQL for one row of bindings: the cached rewrite when
     * every parameter is bound non-null, or a bindings-aware rewrite declaring the
     * null-bound placeholders {@code Nullable(String)} (so the {@code \N} sentinel is
     * accepted by the server).
     */
    private String serverSideSqlFor(Object[] values) {
        for (int n = 1; n <= parameterCount; n++) {
            if (n >= values.length || values[n] == null) {
                return rewriteToNamedParams(sql, placeholderOffsets, values);
            }
        }
        return serverSideSql;
    }

    // ---- execution ----------------------------------------------------------

    /**
     * Statement-derived per-query settings for the server-side path (e.g.
     * {@code setQueryTimeout} as {@code max_execution_time}), as the non-null map the
     * combined core overloads expect — an empty map applies no settings.
     */
    private java.util.Map<String, String> serverSideSettings() {
        java.util.Map<String, String> settings = perQuerySettings();
        return settings == null ? java.util.Map.of() : settings;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (serverSideParams) {
            closeSupersededResultSet();
            try {
                QueryParameters qp = buildServerSideParams(params);
                currentUpdateCount = -1;
                currentResultSet = new ChResultSet(
                        conn.core().query(serverSideSqlFor(params), qp, serverSideSettings()),
                        this);
                return currentResultSet;
            } catch (ClickHouseException e) {
                throw wrap(e);
            }
        }
        return executeQueryInternal(effectiveSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (serverSideParams) {
            closeSupersededResultSet();
            try {
                QueryParameters qp = buildServerSideParams(params);
                conn.core().execute(serverSideSqlFor(params), qp, serverSideSettings());
                currentUpdateCount = 0;
                return 0;
            } catch (ClickHouseException e) {
                throw wrap(e);
            }
        }
        return executeUpdateInternal(effectiveSql());
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
        return executeInternal(effectiveSql());
    }

    // ---- inherited String-arg overloads: forbidden on a PreparedStatement ----

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw stringArgNotAllowed("executeQuery");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw stringArgNotAllowed("executeUpdate");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw stringArgNotAllowed("execute");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw stringArgNotAllowed("addBatch");
    }

    private static SQLException stringArgNotAllowed(String method) {
        return new SQLException(
                method + "(String) cannot be called on a PreparedStatement");
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
                    conn.core().execute(serverSideSqlFor(row), buildServerSideParams(row),
                            serverSideSettings());
                }
            } else if (trimmedUpper.startsWith("INSERT")) {
                conn.core().execute(buildMultiRowInsert());
            } else {
                // Non-INSERT batches: execute each substituted statement in turn.
                for (Object[] row : batchRows) {
                    conn.core().execute(substitute(sql, placeholderOffsets, row));
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

    /**
     * Finds the index of the {@code VALUES} keyword (case-insensitive) outside string
     * literals, requiring a word boundary on both sides so identifiers like
     * {@code values_t} are not mistaken for the keyword.
     */
    private static int indexOfValues(String sql) {
        boolean inString = false;
        for (int i = 0; i + 6 <= sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inString = !inString;
            } else if (!inString && (c == 'V' || c == 'v')) {
                if (sql.regionMatches(true, i, "VALUES", 0, 6)
                        && (i == 0 || !isIdentifierChar(sql.charAt(i - 1)))
                        && (i + 6 == sql.length() || !isIdentifierChar(sql.charAt(i + 6)))) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** True for characters that can continue an (optionally quoted) identifier. */
    private static boolean isIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '`' || c == '"';
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
            /** Per the JDBC contract, every per-parameter accessor validates its index. */
            private void checkIndex(int param) throws SQLException {
                if (param < 1 || param > parameterCount) {
                    throw new SQLException("Parameter index " + param
                            + " out of range [1, " + parameterCount + "]");
                }
            }

            @Override
            public int getParameterCount() {
                return parameterCount;
            }

            @Override
            public int isNullable(int param) throws SQLException {
                checkIndex(param);
                return parameterNullableUnknown;
            }

            @Override
            public boolean isSigned(int param) throws SQLException {
                checkIndex(param);
                return true;
            }

            @Override
            public int getPrecision(int param) throws SQLException {
                checkIndex(param);
                return 0;
            }

            @Override
            public int getScale(int param) throws SQLException {
                checkIndex(param);
                return 0;
            }

            @Override
            public int getParameterType(int param) throws SQLException {
                checkIndex(param);
                return java.sql.Types.OTHER;
            }

            @Override
            public String getParameterTypeName(int param) throws SQLException {
                checkIndex(param);
                return "String";
            }

            @Override
            public String getParameterClassName(int param) throws SQLException {
                checkIndex(param);
                return Object.class.getName();
            }

            @Override
            public int getParameterMode(int param) throws SQLException {
                checkIndex(param);
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
