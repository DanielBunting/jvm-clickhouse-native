package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * SQL-parsing coverage for the driver's deliberately simple scanner, ported from the
 * clickhouse-java parser suites (v1 {@code JdbcParameterizedQueryTest} /
 * {@code ClickHouseSqlParserFacadeTest} and jdbc-v2 {@code BaseSqlParserFacadeTest};
 * jdbc-v2 is the source of truth where the two disagree): placeholder counting across
 * CTE and misc statement shapes, {@code ?::type} cast adjacency, function-argument
 * placeholders, INSERT/VALUES clause extraction for batch building, and pass-through of
 * server-side {@code {name:Type}} parameter syntax.
 *
 * <p>The scanner's comment- and quoted-identifier awareness is covered in
 * {@link ChPreparedStatementTest}; this class covers the remaining dimensions
 * (ternary {@code ?:} disambiguation, backslash-escaped quotes, {@code VALUES}
 * keyword matching inside identifiers) without duplication.
 */
class ChSqlParsingTest {

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
            return null;
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
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", new Properties(), "default");
    }

    // ---- ?::type cast adjacency (jdbc-v2 BaseSqlParserFacadeTest#testStmtWithCasts,
    // v1 JdbcParameterizedQueryTest "select ?::?") -----------------------------

    @Test
    void countsPlaceholdersAdjacentToCastOperator() {
        // jdbc-v2 expects 3: two ?::integer casts plus the bare ?, with the quoted
        // '?:: integer' skipped as a literal.
        assertEquals(3, ChPreparedStatement.countPlaceholders(
                "SELECT ?::integer, ?, '?:: integer' FROM table WHERE v = ?::integer"));
        // v1 JdbcParameterizedQueryTest: both sides of ?::? are parameters.
        assertEquals(2, ChPreparedStatement.countPlaceholders("select ?::?"));
    }

    @Test
    void substitutesPlaceholderAdjacentToCastOperator() throws SQLException {
        assertEquals("SELECT 42::integer FROM t",
                ChPreparedStatement.substitute("SELECT ?::integer FROM t", new Object[] {null, 42}));
    }

    // ---- placeholders as function arguments (jdbc-v2
    // BaseSqlParserFacadeTest#testStmtWithFunction and the INSERT-with-function DP row)

    @Test
    void countsPlaceholdersInsideFunctionArguments() {
        assertEquals(4, ChPreparedStatement.countPlaceholders(
                "SELECT `parseDateTimeBestEffort`(?, ?) as dt FROM table"
                        + " WHERE v > `parseDateTimeBestEffort`(?, ?)"));
        assertEquals(4, ChPreparedStatement.countPlaceholders(
                "INSERT INTO `users` (`name`, `last_login`, `password`, `id`) VALUES\n"
                        + " (?, `parseDateTimeBestEffort`(?, ?), ?, 1)\n"));
    }

    // ---- ternary ?: disambiguation ------------------------------------------

    /**
     * The ClickHouse ternary operator {@code cond ? a : b} is plain text, not a
     * bindable parameter: the shared scanner disambiguates with a lookahead for a
     * {@code :} at the same nesting depth before the expression ends — a {@code ::}
     * is the cast operator, never a ternary colon (was knownBug 14; v1
     * JdbcParameterizedQueryTest#testParseJdbcQueries, jdbc-v2
     * BaseSqlParserFacadeTest#testParseSelectPrepared).
     */
    @Test
    void countPlaceholdersIgnoresTernaryOperatorQuestionMarks() {
        // Only the trailing bare '?' is a parameter.
        assertEquals(1, ChPreparedStatement.countPlaceholders(
                "select 1 ? 'a' : 'b', 2 ? (select 1) : 2, ?"));
        // WHERE c3 = ? and abs(?); the (true ? 1 : 0) is a ternary.
        assertEquals(2, ChPreparedStatement.countPlaceholders(
                "SELECT c1, c2, (true ? 1 : 0 ) as foo FROM tab1 WHERE c3 = ? AND c4 = abs(?)"));
        // The ?(...) aggregate-function placeholder and the trailing '?'.
        assertEquals(2, ChPreparedStatement.countPlaceholders(
                "select ?(number % 2 == 0 ? 1 : 0) from numbers(100) where number > ?"));
    }

    /**
     * Companion to the ternary count case: substitution leaves the ternary operator
     * verbatim, so with one binding the value lands on the real trailing placeholder
     * (was knownBug 14).
     */
    @Test
    void substituteLeavesTernaryOperatorQuestionMark() throws SQLException {
        assertEquals("select 1 ? 'a' : 'b', 3",
                ChPreparedStatement.substitute(
                        "select 1 ? 'a' : 'b', ?", new Object[] {null, 3}));
    }

    /**
     * A genuine placeholder followed later in the same expression by a real ternary
     * is still counted. In {@code "SELECT * FROM t WHERE a = ? AND b ? c : d"} the
     * FIRST {@code ?} is a bindable parameter and only the second one is the ternary
     * operator ({@code b ? c : d}): the scanner pairs each ternary {@code ':'} with
     * the NEAREST preceding unpaired {@code ?} at the same depth, so exactly 1
     * placeholder is counted and {@code setInt(1, ...)} binds it. (was knownBug 37)
     */
    @Test
    void placeholderBeforeLaterTernaryStillCounted() throws SQLException {
        String sql = "SELECT * FROM t WHERE a = ? AND b ? c : d";
        assertEquals(1, ChPreparedStatement.countPlaceholders(sql),
                "only the ternary's '?' is an operator; the first '?' is a parameter");
        // The binding lands on the first '?' and the ternary stays verbatim.
        assertEquals("SELECT * FROM t WHERE a = 42 AND b ? c : d",
                ChPreparedStatement.substitute(sql, new Object[] {null, 42}));
        // Binding surface: setInt(1, ...) must accept the index instead of throwing
        // because countPlaceholders reported 0 parameters.
        ChPreparedStatement ps = new ChPreparedStatement(conn(new RecordingCore()), sql);
        ps.setInt(1, 42);
    }

    // ---- backslash-escaped quote inside a literal ----------------------------

    /**
     * A backslash-escaped quote ({@code \'} — ClickHouse's default escaping style,
     * and what {@link ChPreparedStatement#quote} itself emits) stays inside the
     * literal: within a quoted region the scanner treats {@code \} as consuming the
     * following character, so only the real trailing {@code ?} substitutes (was
     * knownBug 15).
     */
    @Test
    void backslashEscapedQuoteStaysInsideLiteral() throws SQLException {
        // Exactly one real parameter either way (the buggy scanner also reports 1,
        // but it is the '?' INSIDE the literal; the substitution below exposes that).
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT 'it\\'s ?' , ?"));
        assertEquals("SELECT 'it\\'s ?' , 42",
                ChPreparedStatement.substitute("SELECT 'it\\'s ?' , ?", new Object[] {null, 42}));
    }

    // ---- CTE arg counting (jdbc-v2 BaseSqlParserFacadeTest#testCTEStatements) --
    //
    // Comment-bearing rows from the reference DP are omitted: comment blindness is
    // already pinned in ChPreparedStatementTest.

    @Test
    void countsPlaceholdersAcrossCteForms() {
        Object[][] cases = {
                {"with 'a' as a select 1, a union with 'b' as a all select 2, a", 0},
                {"with ? as a, ? as b select a, b", 2},
                {"with a as (select ?), b as (select 2) select * from a, b;", 1},
                {"(with ? as a select a)", 1},
                {"with 'a' as a select a", 0},
                {"WITH toDateTime(?) AS target_time SELECT * FROM table", 1},
                {"WITH toDate('2025-08-20') as DATE_END, events AS ( SELECT 1 ) SELECT * FROM events", 0},
                {"WITH ? as a, ? as b, body as ( select ? ) select a, b, body.* from body", 3},
                {"WITH a AS (SELECT ?), (SELECT 2) AS b SELECT b, *, c FROM a", 1},
                {"WITH a AS (SELECT ?), (SELECT 2) AS b, c as (SELECT ?) SELECT *, b FROM a, c", 2},
                {"WITH a AS (SELECT ?), (WITH ? as b1 SELECT 3, b1) AS b SELECT b, * FROM a", 2},
                {"with ? as val1, numz as (select val1, number from system.numbers limit 10) select * from numz", 1},
                {"WITH '2019-08-01 15:23:00' AS ts_upper_bound SELECT * FROM hits"
                        + " WHERE EventDate = toDate(?) AND EventTime <= ts_upper_bound;", 1},
        };
        for (Object[] c : cases) {
            assertEquals((int) c[1], ChPreparedStatement.countPlaceholders((String) c[0]), (String) c[0]);
        }
    }

    // ---- misc statement arg counting (jdbc-v2 BaseSqlParserFacadeTest#testMiscStatements)

    @Test
    void countsPlaceholdersAcrossMiscStatementShapes() {
        Object[][] cases = {
                {"SELECT myColumn FROM myTable WHERE myColumn in (?, ?, ?)", 3},
                {"select countIf(*, 1 = ?)", 1},
                {"select count(*) filter (where 1 = ?)", 1},
                {"SELECT * FROM `test_data`.`categories` WHERE id = 1::String or id = ?", 1},
                {"alter table user delete where reg_time = ?", 1},
                {"UPDATE db.table01 ON CLUSTER `default` SET col1 = ?, col2 = ? WHERE col3 > ?", 3},
                {"DELETE FROM table WHERE a = ? AND b = ?", 2},
                // Keyword-ish literals stay literal.
                {"insert into t (i, t) values (1, timestamp '2010-01-01 00:00:00')", 0},
                {"SELECT INTERVAL '1 day'", 0},
                // Lambdas and array literals contain no parameters.
                {"SELECT arrayFilter(x -> x > 0, [0, 1, 2, -3])", 0},
                {"SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res", 0},
                {"SELECT * FROM t WHERE hasAllTokens(message, ['peak', 'memory'])", 0},
                // A '?' inside a literal next to a real parameter.
                {"SELECT '##?0.1' as f, ? as a FROM table", 1},
                // Parameterized view invocation.
                {"SELECT result FROM test_view(myParam = ?)", 1},
                // Braces in a literal are not server-side params.
                {"SELECT quantilesTiming(0.1, 0.5, 0.9)(dummy) FROM remote('127.0.0.{2,3}',"
                        + " 'system', 'one') GROUP BY 1 WITH TOTALS", 0},
                {"SELECT v FROM t WHERE a > 10 AND event NOT IN (?)", 1},
        };
        for (Object[] c : cases) {
            assertEquals((int) c[1], ChPreparedStatement.countPlaceholders((String) c[0]), (String) c[0]);
        }
    }

    // ---- scanner edge tokens --------------------------------------------------

    /**
     * Comment-lookalike and operator characters are plain text to the placeholder
     * scanner: division {@code /}, subtraction {@code -}, a trailing lone {@code -},
     * {@code /} or {@code :}, and an unbalanced {@code )} never hide or invent a
     * placeholder.
     */
    @Test
    void scannerTreatsOperatorAndStrayTokensAsPlainText() {
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT 1/2 - 3 + ?"));
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT ? -"));
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT ? /"));
        assertEquals(0, ChPreparedStatement.countPlaceholders("SELECT a :"),
                "a trailing lone ':' pairs nothing");
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT a) = ?"),
                "an unbalanced ')' at depth 0 is ignored");
    }

    /**
     * Unterminated quoted regions consume the rest of the text (so a {@code ?} inside
     * them is never a placeholder), including when the region ends on a trailing
     * backslash with nothing left to escape.
     */
    @Test
    void unterminatedQuotedRegionSwallowsRestOfText() {
        assertEquals(0, ChPreparedStatement.countPlaceholders("SELECT 'abc ?"));
        assertEquals(0, ChPreparedStatement.countPlaceholders("SELECT `id ?"));
        assertEquals(0, ChPreparedStatement.countPlaceholders("SELECT '?\\"),
                "a trailing backslash cannot escape past the end of text");
    }

    /**
     * Block-comment scanning edges mirror the statement classifier: stray {@code /}
     * and {@code *} inside a (nesting) comment are text, and a comment cut off
     * mid-token stays unterminated, hiding any {@code ?} after it.
     */
    @Test
    void blockCommentEdgeTokensInPlaceholderScan() {
        assertEquals(1, ChPreparedStatement.countPlaceholders(
                "/* a /* nested */ c/d **e */ SELECT ?"));
        assertEquals(0, ChPreparedStatement.countPlaceholders("select 1 /*/ ?"),
                "'/*/' does not open and immediately close");
        assertEquals(0, ChPreparedStatement.countPlaceholders("select 1 /* ? *"),
                "a trailing '*' does not terminate the comment");
        assertEquals(0, ChPreparedStatement.countPlaceholders("select 1 /* ? /"),
                "a trailing '/' inside the comment is text, not a nested opener");
    }

    /** A placeholder with no bound value fails substitution with the parameter's 1-based index. */
    @Test
    void substituteWithMissingTrailingValueThrows() {
        SQLException e = assertThrows(SQLException.class, () ->
                ChPreparedStatement.substitute("SELECT ?, ?", new Object[] {null, 1}));
        assertEquals("Missing value for parameter 2", e.getMessage());
    }

    // ---- multi-group VALUES with literal '?' (jdbc-v2
    // BaseSqlParserFacadeTest#testPreparedStatementInsertSQL, v1 INSERT parsing) --

    @Test
    void multiRowValuesWithLiteralQuestionMarksHaveNoParameters() {
        assertEquals(0, ChPreparedStatement.countPlaceholders(
                "INSERT INTO `test_stmt_split2` VALUES (1, 'abc'), (2, '?'), (3, '?')"));
        // v1 ClickHouseSqlParserFacadeTest#testInsertStatement: 'values(' inside a
        // literal does not confuse the scan.
        assertEquals(0, ChPreparedStatement.countPlaceholders(
                "insert into test2(a,b) values('values(',',')"));
    }

    // ---- server-side {name:Type} parameter syntax pass-through (v1
    // ClickHouseSqlParserFacadeTest#testNewParameterSyntax) ---------------------
    //
    // The driver performs no client-side parsing of user-supplied server-side
    // parameters: they are not JDBC placeholders and travel to the server verbatim.

    @Test
    void userSuppliedServerSideParamSyntaxPassesThroughUntouched() throws SQLException {
        assertEquals(0, ChPreparedStatement.countPlaceholders("select {column_a:String}"));
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT {n:UInt32} + ?"));
        assertEquals("SELECT {n:UInt32} + 5",
                ChPreparedStatement.substitute("SELECT {n:UInt32} + ?", new Object[] {null, 5}));
        // The server-side rewrite path also leaves user-supplied params alone,
        // generating names only for the positional '?'.
        assertEquals("SELECT {n:UInt32} + {_p1:String}",
                ChPreparedStatement.rewriteToNamedParams("SELECT {n:UInt32} + ?"));
        // v1's alternate ":name(Type)" syntax is N/A as a binding feature here; it is
        // plain text to the scanner and passes through unchanged.
        assertEquals(0, ChPreparedStatement.countPlaceholders("select :column_a(String)"));
    }

    // ---- INSERT/VALUES clause extraction for batch building (v1
    // ClickHouseSqlParserFacadeTest#testInsertStatement positions, jdbc-v2
    // BaseSqlParserFacadeTest#testParseInsertPrepared) --------------------------

    @Test
    void batchCollapseHandlesMultiLineInsertWithQuotedTable() throws SQLException {
        // The jdbc-v2 testParseInsertPrepared template: newlines inside the column
        // list and extra whitespace around the VALUES tuple.
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO \n`table` (id, \nnum1, col3) \nVALUES    (?, ?, ?)   ");
        ps.setInt(1, 1);
        ps.setString(2, "alice");
        ps.setInt(3, 3);
        ps.addBatch();
        ps.setInt(1, 4);
        ps.setString(2, "bob");
        ps.setInt(3, 6);
        ps.addBatch();

        assertEquals(2, ps.executeBatch().length);
        assertEquals(1, core.executed.size());
        assertEquals(
                "INSERT INTO \n`table` (id, \nnum1, col3) \nVALUES (1, 'alice', 3),(4, 'bob', 6)",
                core.executed.get(0));
    }

    @Test
    void batchCollapseMatchesValuesKeywordCaseInsensitively() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "insert into t (a) values (?)");
        ps.setInt(1, 1);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.addBatch();
        ps.executeBatch();

        assertEquals("insert into t (a) values (1),(2)", core.executed.get(0));
    }

    /**
     * A batched INSERT template without a {@code VALUES} clause (e.g. the
     * {@code INSERT ... FORMAT ...} form from v1's testInsertStatement) cannot be
     * collapsed and fails up front. jdbc-v2 parses these positions instead; our
     * driver's contract is the explicit SQLException.
     */
    @Test
    void batchInsertWithoutValuesClauseThrows() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "insert into `test` (id, name) format RowBinary");
        ps.addBatch();
        assertThrows(SQLException.class, ps::executeBatch);
    }

    /**
     * A table (or column) name beginning with "values" does not confuse the batch
     * collapse: {@code indexOfValues} requires a word boundary on both sides of the
     * keyword, so the collapse splits at the real {@code VALUES} clause (was
     * knownBug 16).
     */
    @Test
    void batchCollapseIgnoresValuesPrefixedTableName() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO values_t (a) VALUES (?)");
        ps.setInt(1, 1);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.addBatch();
        ps.executeBatch();

        assertEquals("INSERT INTO values_t (a) VALUES (1),(2)", core.executed.get(0));

        // The mirror case: "values" at the END of an identifier (letter boundary on
        // the left) is not the keyword either.
        ChPreparedStatement ps2 = new ChPreparedStatement(
                conn(core), "INSERT INTO tvalues (a) VALUES (?)");
        ps2.setInt(1, 3);
        ps2.addBatch();
        ps2.executeBatch();
        assertEquals("INSERT INTO tvalues (a) VALUES (3)", core.executed.get(1));
    }

    /**
     * A quoted identifier NAMED {@code values} (backticked or double-quoted) is not
     * the VALUES keyword: the adjoining quote character counts as an identifier
     * character for the word-boundary check, so the collapse splits at the real
     * {@code VALUES} clause (companion to {@link #batchCollapseIgnoresValuesPrefixedTableName}).
     */
    @Test
    void batchCollapseIgnoresQuotedIdentifierNamedValues() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO `values` (v) VALUES (?)");
        ps.setInt(1, 1);
        ps.addBatch();
        ps.executeBatch();
        assertEquals("INSERT INTO `values` (v) VALUES (1)", core.executed.get(0));

        ChPreparedStatement ps2 = new ChPreparedStatement(
                conn(core), "INSERT INTO \"values\" (v) VALUES (?)");
        ps2.setInt(1, 2);
        ps2.addBatch();
        ps2.executeBatch();
        assertEquals("INSERT INTO \"values\" (v) VALUES (2)", core.executed.get(1));
    }
}
