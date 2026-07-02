package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChPreparedStatement}: placeholder substitution, literal
 * quoting/escaping, and multi-row batch INSERT building. No server is involved;
 * the core connection is a hand-written fake that records the SQL it is asked to
 * execute.
 */
class ChPreparedStatementTest {

    /** A core {@link ClickHouseConnection} that records executed SQL and does nothing else. */
    private static final class RecordingCore implements ClickHouseConnection {
        final List<String> executed = new ArrayList<>();

        @Override
        public long executeScalar(String sql) {
            return 0;
        }

        @Override
        public void execute(String sql) {
            executed.add(sql);
        }

        @Override
        public QueryResult query(String sql) {
            executed.add(sql);
            return new QueryResult() {
                @Override
                public List<String> columnNames() {
                    return List.of();
                }

                @Override
                public List<String> columnTypes() {
                    return List.of();
                }

                @Override
                public java.util.Iterator<io.github.danielbunting.clickhouse.protocol.Block> blocks() {
                    return java.util.Collections.emptyIterator();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public <T> Stream<T> query(String sql, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static ChConnection conn(RecordingCore core) {
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", new Properties());
    }

    // ---- static helpers -----------------------------------------------------

    @Test
    void quotesAndEscapesStrings() {
        assertEquals("'hello'", ChPreparedStatement.quote("hello"));
        assertEquals("'O\\'Brien'", ChPreparedStatement.quote("O'Brien"));
        assertEquals("'a\\\\b'", ChPreparedStatement.quote("a\\b"));
    }

    @Test
    void rendersLiteralsByType() {
        assertEquals("NULL", ChPreparedStatement.toLiteral(null));
        assertEquals("42", ChPreparedStatement.toLiteral(42));
        assertEquals("42", ChPreparedStatement.toLiteral(42L));
        assertEquals("3.14", ChPreparedStatement.toLiteral(3.14d));
        assertEquals("1", ChPreparedStatement.toLiteral(Boolean.TRUE));
        assertEquals("0", ChPreparedStatement.toLiteral(Boolean.FALSE));
        assertEquals("12.50", ChPreparedStatement.toLiteral(new BigDecimal("12.50")));
        assertEquals("'it\\'s'", ChPreparedStatement.toLiteral("it's"));
    }

    @Test
    void rendersTimestampLiteral() {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 5, 30, 13, 45, 7));
        assertEquals("'2026-05-30 13:45:07'", ChPreparedStatement.toLiteral(ts));
    }

    @Test
    void countsPlaceholdersOutsideStringLiterals() {
        assertEquals(2, ChPreparedStatement.countPlaceholders("SELECT ? WHERE a = ?"));
        // The '?' inside the literal must not count.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT ? WHERE s = 'is it?'"));
        // Escaped quote inside a literal.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT 'o''?' , ?"));
    }

    @Test
    void substitutesPlaceholdersAndLeavesLiteralQuestionMarks() throws SQLException {
        Object[] values = {null, "x", 5};
        String result = ChPreparedStatement.substitute(
                "SELECT ? FROM t WHERE note = 'really?' AND n = ?", values);
        assertEquals("SELECT 'x' FROM t WHERE note = 'really?' AND n = 5", result);
    }

    // ---- comment- and identifier-aware placeholder scanning (ported from
    // clickhouse-java ClickHouseSqlParserFacadeTest#testComments and jdbc-v2
    // BaseSqlParserFacadeTest INSERT_WITH_COMMENTS) ----------------------------

    /**
     * KNOWN BUG (fails until fixed): the placeholder scanner only understands
     * single-quoted string literals, so a {@code ?} inside a {@code --}/{@code #}/
     * {@code #!} line comment or a (nesting) block comment is counted as a bindable
     * parameter.
     *
     * <p>Expected (clickhouse-java reference parsers, v1 #testComments and jdbc-v2
     * INSERT_WITH_COMMENTS): comment text is plain text — the counts asserted below.
     * Actual: every commented {@code ?} is counted (4/6/3/1 respectively).
     *
     * <p>Fix: in the shared scan loop of {@link ChPreparedStatement#countPlaceholders}
     * / {@code substitute} / {@code rewriteToNamedParams} (ChPreparedStatement.java),
     * when not inside a string literal, consume {@code --}/{@code #}/{@code #!} line
     * comments through end-of-line and {@code /*}-comments with a nesting-depth
     * counter (emitting them verbatim on the rewriting paths).
     */
    @Test
    void knownBug_countPlaceholdersCountsQuestionMarksInsideComments() {
        // Only the first '?' is a parameter.
        assertEquals(1, ChPreparedStatement.countPlaceholders("select ?/* ?..? */ from x -- ?"));
        // Only the VALUES tuple counts; jdbc-v2 INSERT_WITH_COMMENTS.
        assertEquals(2, ChPreparedStatement.countPlaceholders(
                "-- line comment1 ?\n"
                        + "# line comment2 ?\n"
                        + "#! line comment3 ?\n"
                        + "/* block comment ? \n */"
                        + "INSERT INTO t VALUES (?, ?);"));
        // ClickHouse block comments nest, hiding b? and c?.
        assertEquals(1, ChPreparedStatement.countPlaceholders(
                "select ? /* a /* b? */ c? */ from t"));
        // The '?' sits in an unterminated comment.
        assertEquals(0, ChPreparedStatement.countPlaceholders("select 1 /* ? "));
    }

    /**
     * KNOWN BUG (fails until fixed): the scanner does not recognise backtick- or
     * double-quoted identifiers, so a {@code ?} inside a quoted identifier (legal in
     * ClickHouse, see jdbc-v2's complex-id cases) is counted as a bindable parameter.
     * (The substitution dimension of this bug is covered by
     * {@code ChPreparedStatementBindingTest#knownBug_substituteMustNotReplacePlaceholdersInsideQuotedIdentifiers};
     * this test covers counting.)
     *
     * <p>Expected: quoted identifiers are opaque — the counts asserted below. Actual:
     * each case over-counts (2/2/4 respectively).
     *
     * <p>Fix: in the shared scan loop (ChPreparedStatement.java), track backtick and
     * double-quote identifier state exactly like single-quote literal state
     * (including the doubled-character escapes {@code ``} and {@code ""}).
     */
    @Test
    void knownBug_countPlaceholdersCountsQuestionMarksInsideQuotedIdentifiers() {
        // `v?1` is an identifier.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT `v?1` FROM t WHERE a = ?"));
        // "v?" is an identifier.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT \"v?\" FROM t WHERE a = ?"));
        // Only the VALUES tuple; doubled closing quotes stay inside the identifiers.
        assertEquals(2, ChPreparedStatement.countPlaceholders(
                "INSERT INTO `with_complex_id`(`v?``1`, \"v?\"\"2\") VALUES (?, ?);"));
    }

    /**
     * KNOWN BUG (fails until fixed): substitution consumes bound values for
     * comment-hidden placeholders.
     *
     * <p>Expected: comments pass through verbatim, so one binding satisfies the one
     * real placeholder — {@code "SELECT 5 /* keep ? *}{@code / FROM t -- tail ?"} and
     * {@code "SELECT 1 -- ?"}. Actual: the commented {@code ?}s consume bindings, so
     * substitution throws "Missing value for parameter 2".
     *
     * <p>Fix: same comment-awareness in the shared scan loop of
     * {@link ChPreparedStatement#substitute} (ChPreparedStatement.java), emitting
     * comment text verbatim.
     */
    @Test
    void knownBug_substituteConsumesValuesForCommentedPlaceholders() throws SQLException {
        assertEquals("SELECT 5 /* keep ? */ FROM t -- tail ?",
                ChPreparedStatement.substitute(
                        "SELECT ? /* keep ? */ FROM t -- tail ?", new Object[] {null, 5}));
        assertEquals("SELECT 1 -- ?",
                ChPreparedStatement.substitute("SELECT ? -- ?", new Object[] {null, 1}));
    }

    /**
     * KNOWN BUG (fails until fixed): the server-side rewrite shares the scanner, so
     * comment-hidden {@code ?} also become named placeholders.
     *
     * <p>Expected: comments are left verbatim —
     * {@code "SELECT {_p1:String} /* ? *}{@code / -- ?\n"}. Actual:
     * {@code "SELECT {_p1:String} /* {_p2:String} *}{@code / -- {_p3:String}\n"}.
     *
     * <p>Fix: same comment-awareness in
     * {@link ChPreparedStatement#rewriteToNamedParams} (ChPreparedStatement.java).
     */
    @Test
    void knownBug_rewriteToNamedParamsRewritesCommentedPlaceholders() {
        assertEquals("SELECT {_p1:String} /* ? */ -- ?\n",
                ChPreparedStatement.rewriteToNamedParams("SELECT ? /* ? */ -- ?\n"));
    }

    /**
     * KNOWN BUG (fails until fixed): comment-hidden {@code ?} inflates the reported
     * parameter count and the phantom index is bindable.
     *
     * <p>Expected (jdbc-v2 PreparedStatementTest#testParameterCount): the count is 1
     * and binding index 2 throws SQLException. Actual: the count is 2 and
     * {@code setInt(2, ...)} is accepted.
     *
     * <p>Fix: comment-aware {@link ChPreparedStatement#countPlaceholders}
     * (ChPreparedStatement.java); the constructor-derived {@code parameterCount} and
     * {@code setParam} range check then follow automatically.
     */
    @Test
    void knownBug_parameterCountIncludesCommentedPlaceholders() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "-- where id = ?\nSELECT * FROM t WHERE id = ?");
        assertEquals(1, ps.getParameterMetaData().getParameterCount());
        assertThrows(SQLException.class, () -> ps.setInt(2, 8),
                "index 2 is out of range once the commented '?' is ignored");
    }

    // ---- end-to-end through the statement -----------------------------------

    @Test
    void executeUpdateSubstitutesAndForwardsToCore() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (id, name) VALUES (?, ?)");
        ps.setInt(1, 7);
        ps.setString(2, "a'b");
        ps.executeUpdate();

        assertEquals(1, core.executed.size());
        assertEquals("INSERT INTO t (id, name) VALUES (7, 'a\\'b')", core.executed.get(0));
    }

    @Test
    void nullParameterRendersAsSqlNull() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        ps.setNull(1, java.sql.Types.VARCHAR);
        ps.executeUpdate();

        assertEquals("INSERT INTO t (x) VALUES (NULL)", core.executed.get(0));
    }

    @Test
    void batchInsertCollapsesToSingleMultiRowStatement() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (id, name) VALUES (?, ?)");

        ps.setInt(1, 1);
        ps.setString(2, "alice");
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setString(2, "bob");
        ps.addBatch();

        int[] counts = ps.executeBatch();

        assertEquals(2, counts.length);
        assertEquals(1, core.executed.size());
        assertEquals(
                "INSERT INTO t (id, name) VALUES (1, 'alice'),(2, 'bob')",
                core.executed.get(0));
    }

    @Test
    void clearParametersResetsBindings() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        ps.setInt(1, 99);
        ps.clearParameters();
        // After clearing, the unbound parameter renders as NULL.
        ps.executeUpdate();
        assertEquals("INSERT INTO t (x) VALUES (NULL)", core.executed.get(0));
    }

    @Test
    void outOfRangeParameterIndexThrows() {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        assertThrows(SQLException.class, () -> ps.setInt(2, 1));
    }

    // ---- issue regressions (distributed slices; see ChJdbcIssuesTest) --------

    /**
     * A {@code Collection}/array binding renders as a ClickHouse array literal.
     * Regression for clickhouse-java#2329 (write an Array(String) via setObject).
     */
    @Test
    void rendersArrayLiteralFromCollection() {
        assertEquals("['a','b']", ChPreparedStatement.toLiteral(List.of("a", "b")));
        assertEquals("[1,2,3]", ChPreparedStatement.toLiteral(List.of(1, 2, 3)));
        assertEquals("[]", ChPreparedStatement.toLiteral(List.of()));
        // Nested arrays recurse.
        assertEquals("[[1,2],[3]]", ChPreparedStatement.toLiteral(List.of(List.of(1, 2), List.of(3))));
        // Java arrays are handled like collections.
        assertEquals("[1,2]", ChPreparedStatement.toLiteral(new int[] {1, 2}));
    }

    /** setObject(collection) flows an array literal end-to-end. Regression for #2329. */
    @Test
    void setObjectCollectionRendersArrayLiteralEndToEnd() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (id, arr) VALUES (?, ?)");
        ps.setString(1, "id01");
        ps.setObject(2, List.of("x", "y"));
        ps.executeUpdate();
        assertEquals("INSERT INTO t (id, arr) VALUES ('id01', ['x','y'])", core.executed.get(0));
    }

    /** UUID binds as a quoted string literal ClickHouse casts to UUID. Regression for #2327. */
    @Test
    void rendersUuidLiteral() {
        java.util.UUID uuid = java.util.UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        assertEquals("'61f0c404-5cb3-11e7-907b-a6006ad3dba0'", ChPreparedStatement.toLiteral(uuid));
    }

    /** IPv6/IPv4 bind as the canonical numeric address, quoted. Regression for #315. */
    @Test
    void rendersInetAddressLiteral() throws Exception {
        java.net.InetAddress v6 = java.net.InetAddress.getByName("2001:db8::1");
        assertEquals("'" + v6.getHostAddress() + "'", ChPreparedStatement.toLiteral(v6));
        java.net.InetAddress v4 = java.net.InetAddress.getByName("192.168.0.1");
        assertEquals("'192.168.0.1'", ChPreparedStatement.toLiteral(v4));
    }

    /** Sub-second timestamps keep their fractional part to nanoseconds. Regression for #612. */
    @Test
    void rendersFractionalTimestampLiteral() {
        // Nanosecond precision (DateTime64(9)) is preserved: nine fractional digits.
        Timestamp ts = Timestamp.valueOf("2026-05-30 13:45:07.123456789");
        assertEquals("'2026-05-30 13:45:07.123456789'", ChPreparedStatement.toLiteral(ts));
        // Whole-second values stay terse (no trailing .000000000).
        Timestamp whole = Timestamp.valueOf("2026-05-30 13:45:07");
        assertEquals("'2026-05-30 13:45:07'", ChPreparedStatement.toLiteral(whole));
    }

    // ---- JDBC contract edges (ported from clickhouse-java jdbc-v2
    // PreparedStatementTest and JdbcParameterizedQueryTest) --------------------

    /**
     * Blank SQL is accepted leniently with zero parameters. Note: jdbc-v2's parser
     * rejects null/blank SQL up front; our driver performs no such validation and
     * defers any failure to execution, so this pins the lenient contract. (Passing
     * {@code null} SQL is the separate failing
     * {@link #knownBug_prepareStatementWithNullSqlThrowsNpeInsteadOfSqlException}.)
     */
    @Test
    void blankSqlPreparesWithZeroParameters() throws SQLException {
        RecordingCore core = new RecordingCore();
        assertEquals(0, new ChPreparedStatement(conn(core), "")
                .getParameterMetaData().getParameterCount());
        assertEquals(0, new ChPreparedStatement(conn(core), " \n\t\r")
                .getParameterMetaData().getParameterCount());
    }

    /** Result-set metadata before execution is not supported by the native protocol. */
    @Test
    void getMetaDataBeforeExecutionThrowsNotSupported() {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "SELECT * FROM t WHERE id = ?");
        assertThrows(SQLFeatureNotSupportedException.class, ps::getMetaData);
    }

    /**
     * The generated-keys String overloads already throw on a PreparedStatement (they
     * throw on any statement — generated keys are unsupported). The plain String-arg
     * overloads are the separate failing
     * {@link #knownBug_plainStringArgOverloadsDoNotThrowOnPreparedStatement}.
     */
    @Test
    void stringArgGeneratedKeysOverloadsThrowOnPreparedStatement() {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "SELECT * FROM t WHERE id = ?");
        String sql = "SELECT 1";
        assertThrows(SQLException.class, () -> ps.execute(sql, java.sql.Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLException.class, () -> ps.execute(sql, new int[]{1}));
        assertThrows(SQLException.class, () -> ps.execute(sql, new String[]{"c"}));
        assertThrows(SQLException.class, () -> ps.executeUpdate(sql, java.sql.Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLException.class, () -> ps.executeUpdate(sql, new int[]{1}));
        assertThrows(SQLException.class, () -> ps.executeUpdate(sql, new String[]{"c"}));
    }

    /**
     * KNOWN BUG (fails until fixed): JDBC spec (and jdbc-v2
     * PreparedStatementTest#testMethodsNotAllowedToBeCalled) — the String-argument
     * {@code execute(String)}, {@code executeQuery(String)},
     * {@code executeUpdate(String)} and {@code addBatch(String)} overloads inherited
     * from {@link java.sql.Statement} must throw {@link SQLException} when called on
     * a {@link java.sql.PreparedStatement}.
     *
     * <p>Expected: each call below throws SQLException. Actual:
     * {@code ChPreparedStatement} silently inherits {@code ChStatement}'s working
     * implementations, so every call succeeds.
     *
     * <p>Fix: override the four methods in {@code ChPreparedStatement}
     * (ChPreparedStatement.java) to throw
     * {@code new SQLException("method not supported on a PreparedStatement")}, as
     * jdbc-v2's PreparedStatementImpl does.
     */
    @Test
    void knownBug_plainStringArgOverloadsDoNotThrowOnPreparedStatement() {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "SELECT * FROM t WHERE id = ?");
        String sql = "SELECT 1";
        assertThrows(SQLException.class, () -> ps.execute(sql));
        assertThrows(SQLException.class, () -> ps.executeQuery(sql));
        assertThrows(SQLException.class, () -> ps.executeUpdate(sql));
        assertThrows(SQLException.class, () -> ps.addBatch(sql));
    }

    /**
     * KNOWN BUG (fails until fixed): {@code prepareStatement(null)} must fail with a
     * {@link SQLException} (jdbc-v2 rejects null SQL up front with SQLException).
     *
     * <p>Expected: SQLException. Actual: a raw {@link NullPointerException} escapes —
     * the {@code ChPreparedStatement} constructor calls
     * {@code countPlaceholders(sql)} on the null without any validation.
     *
     * <p>Fix: null-check the SQL in the {@code ChPreparedStatement} constructor
     * (ChPreparedStatement.java) — declaring {@code throws SQLException} — or in
     * {@code ChConnection.prepareStatement} (ChConnection.java) before construction,
     * throwing {@code new SQLException("SQL must not be null")}.
     */
    @Test
    void knownBug_prepareStatementWithNullSqlThrowsNpeInsteadOfSqlException() {
        RecordingCore core = new RecordingCore();
        assertThrows(SQLException.class, () -> conn(core).prepareStatement(null));
    }

    /**
     * A {@code null} bound on one row of a batch does not throw and renders as NULL.
     * Regression for clickhouse-java#1373.
     */
    @Test
    void nullInBatchDoesNotThrowAndRendersNull() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (a, b) VALUES (?, ?)");
        ps.setString(1, "r1a");
        ps.setString(2, "r1b");
        ps.addBatch();
        ps.setString(1, "r2a");
        ps.setString(2, null); // null mid-batch
        ps.addBatch();

        int[] counts = ps.executeBatch();
        assertEquals(2, counts.length);
        assertEquals("INSERT INTO t (a, b) VALUES ('r1a', 'r1b'),('r2a', NULL)", core.executed.get(0));
    }
}
