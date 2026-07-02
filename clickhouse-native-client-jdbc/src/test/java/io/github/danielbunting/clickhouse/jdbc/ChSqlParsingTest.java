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
 * <p>The scanner is single-quote-aware only. Its comment- and quoted-identifier
 * blindness is already pinned in {@link ChPreparedStatementTest}; this class pins the
 * remaining gaps it exposes (ternary {@code ?:} disambiguation, backslash-escaped
 * quotes, {@code VALUES} keyword matching inside identifiers) without duplicating
 * those pins.
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
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", new Properties());
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
     * KNOWN BUG — pinned, not fixed (out of scope): the scanner counts every {@code ?}
     * outside a single-quoted literal, so the ClickHouse ternary operator
     * {@code cond ? a : b} is miscounted as a bindable parameter. The reference parsers
     * (v1 JdbcParameterizedQueryTest#testParseJdbcQueries, jdbc-v2
     * BaseSqlParserFacadeTest#testParseSelectPrepared) treat ternary {@code ?} as plain
     * text; the correct count is noted on each case. Fix the counts when the scanner
     * learns to disambiguate the ternary operator.
     */
    @Test
    void knownBug_countPlaceholdersCountsTernaryOperatorQuestionMarks() {
        // Correct count: 1 (only the trailing bare '?').
        assertEquals(3, ChPreparedStatement.countPlaceholders(
                "select 1 ? 'a' : 'b', 2 ? (select 1) : 2, ?"));
        // Correct count: 2 (WHERE '?' and abs(?)); the (true ? 1 : 0) is a ternary.
        assertEquals(3, ChPreparedStatement.countPlaceholders(
                "SELECT c1, c2, (true ? 1 : 0 ) as foo FROM tab1 WHERE c3 = ? AND c4 = abs(?)"));
        // Correct count: 2 (the ?(...) aggregate-function placeholder and the trailing ?).
        assertEquals(3, ChPreparedStatement.countPlaceholders(
                "select ?(number % 2 == 0 ? 1 : 0) from numbers(100) where number > ?"));
    }

    /**
     * KNOWN BUG — companion to the ternary miscount: substitution splices a bound value
     * into the ternary operator position, corrupting the statement.
     */
    @Test
    void knownBug_substituteReplacesTernaryOperatorQuestionMark() throws SQLException {
        // With only the one real binding, the ternary '?' consumes it and the real
        // placeholder then reports a missing parameter.
        assertThrows(SQLException.class, () -> ChPreparedStatement.substitute(
                "select 1 ? 'a' : 'b', ?", new Object[] {null, 3}));
        // With a surplus binding the shift is visible: should be "select 1 ? 'a' : 'b', 3".
        assertEquals("select 1 3 'a' : 'b', 4",
                ChPreparedStatement.substitute(
                        "select 1 ? 'a' : 'b', ?", new Object[] {null, 3, 4}));
    }

    // ---- backslash-escaped quote inside a literal ----------------------------

    /**
     * KNOWN BUG — pinned, not fixed (out of scope): the scanner only recognises the
     * doubled-quote ({@code ''}) escape, so a backslash-escaped quote ({@code \'} —
     * ClickHouse's default escaping style, and what {@link ChPreparedStatement#quote}
     * itself emits) is taken as the end of the literal. The scanner then counts the
     * {@code ?} still inside the literal and misses the real one after it.
     */
    @Test
    void knownBug_backslashEscapedQuoteEndsLiteralEarly() throws SQLException {
        // One '?' is found, but it is the one INSIDE the literal, not the real trailing one.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT 'it\\'s ?' , ?"));
        // Should be "SELECT 'it\'s ?' , 42".
        assertEquals("SELECT 'it\\'s 42' , ?",
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
     * KNOWN BUG — pinned, not fixed (out of scope): {@code indexOfValues} matches the
     * six letters {@code VALUES} anywhere outside a string literal, without a word
     * boundary, so a table (or column) name beginning with "values" is split as if it
     * were the VALUES keyword and the collapsed multi-row INSERT is corrupted. The
     * reference parsers tokenize and would find the real keyword.
     */
    @Test
    void knownBug_batchCollapseSplitsOnValuesPrefixedTableName() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO values_t (a) VALUES (?)");
        ps.setInt(1, 1);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.addBatch();
        ps.executeBatch();

        // Should be "INSERT INTO values_t (a) VALUES (1),(2)".
        assertEquals("INSERT INTO values _t (a) VALUES (1),_t (a) VALUES (2)",
                core.executed.get(0));
    }
}
