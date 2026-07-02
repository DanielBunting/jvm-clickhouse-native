package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link ChStatement} — statement-kind classification, the JDBC-spec
 * unsupported-feature contract, and the wrapper API. This class previously had no dedicated
 * test. No server: the unsupported-feature methods throw before touching the connection, so a
 * {@code null} connection is sufficient, and {@code producesResultSet} is a pure classifier.
 */
class ChStatementTest {

    private static ChStatement stmt() {
        // These methods throw (or classify) before dereferencing the connection.
        return new ChStatement((ChConnection) null);
    }

    @Test
    void producesResultSet_classifiesResultProducingStatements() {
        assertTrue(ChStatement.producesResultSet("SELECT 1"));
        assertTrue(ChStatement.producesResultSet("  select * from t"), "leading space + lowercase");
        assertTrue(ChStatement.producesResultSet("SHOW TABLES"));
        assertTrue(ChStatement.producesResultSet("DESC t"));
        assertTrue(ChStatement.producesResultSet("DESCRIBE TABLE t"), "DESCRIBE starts with DESC");
        assertTrue(ChStatement.producesResultSet("EXISTS TABLE t"));
        assertTrue(ChStatement.producesResultSet("WITH x AS (SELECT 1) SELECT * FROM x"));
    }

    @Test
    void producesResultSet_rejectsNonQueryStatements() {
        assertFalse(ChStatement.producesResultSet("INSERT INTO t VALUES (1)"));
        assertFalse(ChStatement.producesResultSet("CREATE TABLE t (a UInt8) ENGINE = Memory"));
        assertFalse(ChStatement.producesResultSet("DROP TABLE t"));
        assertFalse(ChStatement.producesResultSet(null), "null SQL is not result-producing");
    }

    // ---- statement-kind matrix (ported from clickhouse-java v1
    // ClickHouseSqlParserFacadeTest per-keyword tests and jdbc-v2
    // BaseSqlParserFacadeTest#testStatementsForResultSet; jdbc-v2 is the source of
    // truth for the hasResultSet expectation) ----------------------------------

    /**
     * DDL/DML/admin statement kinds route to the update path. Matches the jdbc-v2
     * no-result-set matrix. Notes: DELETE/UPDATE client-side rewriting and USE
     * database tracking are v1 features our driver does not have (N/A); only the
     * classification is asserted here.
     */
    @Test
    void producesResultSet_rejectsDdlDmlAndAdminStatements() {
        String[] noResultSet = {
                "ALTER TABLE alter_test ADD COLUMN Added0 UInt32",
                "ALTER TABLE test_db.test_table UPDATE a = 1, \"b\" = '2', `c`=3.3 WHERE d=123 and e=456",
                "ALTER TABLE tTt on cluster 'cc' delete WHERE d=123 and e=456",
                "ATTACH TABLE IF NOT EXISTS t.t ON CLUSTER cluster",
                "DETACH TABLE if exists t.t on cluster 'cc'",
                "delete from a",
                "DELETE FROM table WHERE a = 1 AND b = 2",
                "GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION",
                "REVOKE SELECT(a,b) ON db1.tableA FROM `user01`",
                "KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'",
                "KILL MUTATION WHERE database = 'default' AND table = 'table'",
                "OPTIMIZE TABLE table DEDUPLICATE",
                "RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak",
                "EXCHANGE TABLES table1 AND table2",
                "SET profile = 'profile-name-from-the-settings-file'",
                "SET ROLE role1",
                "SYSTEM RELOAD DICTIONARIES",
                "TRUNCATE TABLE `db1`.`table1`",
                "TRUNCATE DATABASE IF EXISTS db",
                "UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today()",
                "use system",
                "MOVE USER test TO local_directory",
                "UNDROP TABLE tab",
                // WATCH streams live-view results in ClickHouse, but neither jdbc-v2's
                // matrix nor our classifier treats it as result-producing.
                "watch system.processes",
        };
        for (String sql : noResultSet) {
            assertFalse(ChStatement.producesResultSet(sql), sql);
        }
    }

    /** Result-producing variants beyond the basic forms already asserted above. */
    @Test
    void producesResultSet_acceptsShowDescribeExistsVariants() {
        String[] hasResultSet = {
                "SHOW CREATE TABLE `db`.`test_table`",
                "SHOW PROCESSLIST",
                "SHOW GRANTS FOR `user01`",
                "SHOW SETTINGS LIKE 'send_timeout'",
                "DESCRIBE TABLE (select 1::Uint32)",
                "DESC TABLE table1",
                "EXISTS TABLE `db`.`table01`",
                "EXISTS DICTIONARY c",
        };
        for (String sql : hasResultSet) {
            assertTrue(ChStatement.producesResultSet(sql), sql);
        }
    }

    /**
     * EXPLAIN and CHECK statements and parenthesized SELECT/WITH queries all classify
     * as result-producing: the classifier recognises both keywords and skips leading
     * {@code '('} characters before sniffing (was knownBug 10; per the jdbc-v2 matrix,
     * BaseSqlParserFacadeTest#testStatementsForResultSet and the CTE suite).
     */
    @Test
    void producesResultSet_classifiesExplainCheckAndParenthesizedQueries() {
        assertTrue(ChStatement.producesResultSet("EXPLAIN SELECT 1"));
        assertTrue(ChStatement.producesResultSet("EXPLAIN AST SELECT 1"));
        assertTrue(ChStatement.producesResultSet(
                "EXPLAIN SELECT sum(number) FROM numbers(10) GROUP BY number"));
        assertTrue(ChStatement.producesResultSet("CHECK TABLE test_table"));
        assertTrue(ChStatement.producesResultSet("CHECK GRANT SELECT(col2) ON table_2"));
        assertTrue(ChStatement.producesResultSet("(SELECT 1)"));
        assertTrue(ChStatement.producesResultSet("(with a as (select 1) select * from a)"));
    }

    /**
     * Pins the lenient prefix-sniffing contract (v1 ClickHouseSqlParserFacadeTest
     * #testParseNonSql classifies garbage as UNKNOWN; our classifier has no UNKNOWN):
     * a bare or merged leading keyword is enough to classify as a query — there is no
     * word-boundary check or tokenization. Invalid SQL simply rides whichever path the
     * prefix selects and fails server-side.
     */
    @Test
    void producesResultSet_sniffsKeywordPrefixWithoutWordBoundary() {
        assertFalse(ChStatement.producesResultSet("invalid sql"));
        assertTrue(ChStatement.producesResultSet("select"), "bare keyword");
        assertTrue(ChStatement.producesResultSet("select (()"), "unbalanced garbage after keyword");
        assertTrue(ChStatement.producesResultSet("SELECTION AND OTHER GARBAGE"), "merged keyword");
        assertTrue(ChStatement.producesResultSet("SHOWCASE"), "merged keyword");
        assertTrue(ChStatement.producesResultSet("DESCENDING"), "merged keyword");
    }

    // ---- comment-prefixed classification (ported from clickhouse-java
    // jdbc-v2 StatementTest#testNewLineSQLParsing) -----------------------------

    /**
     * SQL beginning with a {@code --}/{@code #}/{@code #!} line comment classifies by
     * the first keyword after the comments: {@code producesResultSet} skips leading
     * whitespace and line comments before sniffing (was knownBug 9; jdbc-v2
     * StatementTest#testNewLineSQLParsing).
     */
    @Test
    void producesResultSet_lineCommentPrefixSkipped() {
        assertTrue(ChStatement.producesResultSet("-- comment\nSELECT 1"));
        assertTrue(ChStatement.producesResultSet(
                "-- SELECT amount FROM balance FINAL;\n\nSELECT amount FROM balance FINAL;"));
        assertTrue(ChStatement.producesResultSet(
                "-- SELECT * FROM balance\n\nWITH balance_cte AS (SELECT 1) SELECT * FROM balance_cte;"));
        assertTrue(ChStatement.producesResultSet("# hash comment\nSELECT 1"));
        assertTrue(ChStatement.producesResultSet("#! shebang-style comment\nSELECT 1"));
    }

    /**
     * Companion to the line-comment case — SQL beginning with a {@code /* *}{@code /}
     * block comment (ClickHouse block comments nest) classifies by the keyword after
     * the comment: the classifier skips blocks with a nesting-depth counter (was
     * knownBug 9; jdbc-v2 StatementTest#testNewLineSQLParsing).
     */
    @Test
    void producesResultSet_blockCommentPrefixSkipped() {
        assertTrue(ChStatement.producesResultSet("/* c */ SELECT 1"));
        assertTrue(ChStatement.producesResultSet("/* outer /* nested */ still comment */ SELECT 1"));
        assertTrue(ChStatement.producesResultSet("\n\t /* c */\n-- more\nSHOW TABLES"));
    }

    @Test
    void producesResultSet_commentOnlyOrBlankSqlIsNotAQuery() {
        assertFalse(ChStatement.producesResultSet(""));
        assertFalse(ChStatement.producesResultSet(" \n\t\r"));
        assertFalse(ChStatement.producesResultSet("-- only a comment"));
        assertFalse(ChStatement.producesResultSet("/* unterminated comment SELECT 1"));
        // A keyword that appears only INSIDE a comment must never classify as a query,
        // with or without the comment-skipping fix.
        assertFalse(ChStatement.producesResultSet("-- SELECT looks like a query\nINSERT INTO t VALUES (1)"));
        assertFalse(ChStatement.producesResultSet("/* SELECT */ INSERT INTO t VALUES (1)"));
    }

    @Test
    void generatedKeysOverloadsAreUnsupported() {
        ChStatement s = stmt();
        assertThrows(SQLFeatureNotSupportedException.class, s::getGeneratedKeys);
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.executeUpdate("INSERT INTO t VALUES (1)", Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.executeUpdate("x", new int[]{1}));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.executeUpdate("x", new String[]{"c"}));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.execute("x", Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.execute("x", new int[]{1}));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.execute("x", new String[]{"c"}));
    }

    @Test
    void namedCursorsAreUnsupported() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt().setCursorName("c"));
    }

    @Test
    void onlyForwardFetchDirectionIsAllowed() throws SQLException {
        ChStatement s = stmt();
        s.setFetchDirection(ResultSet.FETCH_FORWARD); // allowed, no throw
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.setFetchDirection(ResultSet.FETCH_UNKNOWN));
    }

    @Test
    void wrapperContract() throws SQLException {
        ChStatement s = stmt();
        assertTrue(s.isWrapperFor(Statement.class));
        assertSame(s, s.unwrap(Statement.class));
        assertFalse(s.isWrapperFor(String.class));
        assertThrows(SQLException.class, () -> s.unwrap(String.class));
    }

    // ---- execution paths against a recording fake core ----------------------

    /** A core connection that records executed/queried SQL and returns an empty result. */
    private static final class RecordingCore implements ClickHouseConnection {
        final List<String> executed = new ArrayList<>();
        final List<String> queried = new ArrayList<>();

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
            queried.add(sql);
            return new QueryResult() {
                @Override
                public List<String> columnNames() {
                    return Collections.emptyList();
                }

                @Override
                public List<String> columnTypes() {
                    return Collections.emptyList();
                }

                @Override
                public Iterator<Block> blocks() {
                    return Collections.emptyIterator();
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
        }
    }

    private static ChStatement connected(RecordingCore core) {
        ChConnection c = new ChConnection(core, "jdbc:chnative://localhost:9000/default", new Properties());
        return new ChStatement(c);
    }

    @Test
    void executeUpdateForwardsSqlAndSetsCounts() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChStatement s = connected(core);
        assertEquals(0, s.executeUpdate("INSERT INTO t VALUES (1)"));
        assertEquals(List.of("INSERT INTO t VALUES (1)"), core.executed);
        assertEquals(0, s.getUpdateCount());
        assertNull(s.getResultSet());
    }

    @Test
    void executeClassifiesQueryVsUpdate() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChStatement s = connected(core);
        assertFalse(s.execute("INSERT INTO t VALUES (1)"));
        assertTrue(s.execute("SELECT 1"));
        assertEquals(List.of("SELECT 1"), core.queried);
        assertTrue(s.getResultSet() != null);
        assertEquals(-1, s.getUpdateCount());
    }

    /**
     * {@code execute()} on a comment-prefixed SELECT reports a result set and routes
     * to {@code core().query} — the comment-skipping classifier makes the routing
     * decision on the first real keyword (was knownBug 9; jdbc-v2
     * StatementTest#testNewLineSQLParsing).
     */
    @Test
    void executeRoutesCommentPrefixedSelectToQueryPath() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChStatement s = connected(core);
        String sql = "-- SELECT amount FROM balance FINAL;\n\nSELECT amount FROM balance FINAL;";
        assertTrue(s.execute(sql), "a comment-prefixed SELECT must report a result set");
        assertEquals(List.of(sql), core.queried, "must be routed to query(), not execute()");
        assertTrue(core.executed.isEmpty(), "the update path must not see the query");
    }

    @Test
    void getMoreResultsReturnsFalseAndClosesResultSet() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChStatement s = connected(core);
        s.execute("SELECT 1");
        ResultSet rs = s.getResultSet();
        assertFalse(s.getMoreResults());
        assertTrue(rs.isClosed());
        assertNull(s.getResultSet());
        assertEquals(-1, s.getUpdateCount());
    }

    @Test
    void batchForwardsEachRowAndClears() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChStatement s = connected(core);
        s.addBatch("INSERT INTO t VALUES (1)");
        s.addBatch("INSERT INTO t VALUES (2)");
        int[] counts = s.executeBatch();
        assertEquals(2, counts.length);
        assertEquals(List.of("INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"), core.executed);
        // Batch is cleared after execution.
        assertEquals(0, s.executeBatch().length);
    }

    @Test
    void passthroughKnobsRoundTrip() throws SQLException {
        ChStatement s = connected(new RecordingCore());
        s.setMaxRows(50);
        assertEquals(50, s.getMaxRows());
        s.setQueryTimeout(9);
        assertEquals(9, s.getQueryTimeout());
        s.setFetchSize(100);
        assertEquals(100, s.getFetchSize());
    }

    @Test
    void closeIsIdempotentAndClosedStatementThrows() throws SQLException {
        ChStatement s = connected(new RecordingCore());
        s.close();
        s.close(); // idempotent
        assertTrue(s.isClosed());
        assertThrows(SQLException.class, () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
    }

    // ---- enquoteLiteral / enquoteIdentifier / isSimpleIdentifier -------------
    //
    // Ported from clickhouse-java client-v2 SQLUtilsTest. ChStatement does not
    // override these, so the JDK's java.sql.Statement default implementations
    // apply; where the reference asserts ClickHouse-specific deviations we assert
    // the JDK-default behavior instead and note the difference.

    @Test
    void enquoteLiteralSingleQuotesAndDoublesEmbeddedQuotes() throws SQLException {
        ChStatement s = stmt();
        assertEquals("'test 123'", s.enquoteLiteral("test 123"));
        assertEquals("'こんにちは世界'", s.enquoteLiteral("こんにちは世界"));
        assertEquals("'O''Reilly'", s.enquoteLiteral("O'Reilly"));
        assertEquals("'😊👍'", s.enquoteLiteral("😊👍"));
        assertEquals("''", s.enquoteLiteral(""));
        assertEquals("'single''quote''double''''quote\"'",
                s.enquoteLiteral("single'quote'double''quote\""));
    }

    /** Reference SQLUtils throws IllegalArgumentException; the JDK default NPEs on null. */
    @Test
    void enquoteLiteralNullInputThrowsNpe() {
        assertThrows(NullPointerException.class, () -> stmt().enquoteLiteral(null));
    }

    @Test
    void enquoteIdentifierQuotesOnlyWhenNeeded() throws SQLException {
        ChStatement s = stmt();
        // Simple identifiers pass through unquoted unless alwaysQuote is set.
        for (String id : new String[] {"column1", "table_name", "a1b2c3", "ColumnName", "UPPERCASE"}) {
            assertEquals(id, s.enquoteIdentifier(id, false), id);
            assertEquals("\"" + id + "\"", s.enquoteIdentifier(id, true), id);
        }
        // Non-simple identifiers are double-quoted either way.
        assertEquals("\"table.name\"", s.enquoteIdentifier("table.name", false));
        assertEquals("\"column with spaces\"", s.enquoteIdentifier("column with spaces", false));
        assertEquals("\"1column\"", s.enquoteIdentifier("1column", false));
        assertEquals("\"column-with-hyphen\"", s.enquoteIdentifier("column-with-hyphen", false));
        assertEquals("\"😊👍\"", s.enquoteIdentifier("😊👍", false));
        // JDK default requires an *alpha* first character, so unlike the reference's
        // SQLUtils, a leading underscore is not "simple" and gets quoted.
        assertEquals("\"_id\"", s.enquoteIdentifier("_id", false));
        // An already-quoted identifier is unwrapped and re-quoted.
        assertEquals("\"foo.bar\"", s.enquoteIdentifier("\"foo.bar\"", false));
    }

    @Test
    void enquoteIdentifierRejectsInvalidInput() {
        ChStatement s = stmt();
        // JDK default: length must be 1..128 (the reference's SQLUtils would quote "").
        assertThrows(SQLException.class, () -> s.enquoteIdentifier("", false));
        assertThrows(SQLException.class, () -> s.enquoteIdentifier("a".repeat(129), false));
        // JDK default: embedded double quotes are invalid (the reference doubles them).
        assertThrows(SQLException.class, () -> s.enquoteIdentifier("column\"with\"quotes", false));
        assertThrows(SQLException.class, () -> s.enquoteIdentifier("ident\0null", false));
        // JDK default NPEs on null (reference throws IllegalArgumentException).
        assertThrows(NullPointerException.class, () -> s.enquoteIdentifier(null, false));
        assertThrows(NullPointerException.class, () -> s.enquoteIdentifier(null, true));
    }

    @Test
    void isSimpleIdentifierAcceptsAlphaThenAlnumUnderscore() throws SQLException {
        ChStatement s = stmt();
        for (String id : new String[] {
                "Hello", "hello_world", "Hello123", "H", "a".repeat(128),
                "testName", "TEST_NAME", "test123", "t123", "t"}) {
            assertTrue(s.isSimpleIdentifier(id), id);
        }
    }

    @Test
    void isSimpleIdentifierRejectsSpecialsBoundsAndPunctuation() throws SQLException {
        ChStatement s = stmt();
        for (String id : new String[] {
                "G'Day", "\"\"Bruce Wayne\"\"", "GoodDay$", "Hello\"\"World",
                "\"\"Hello\"\"World\"\"", "", "123test", "_test", "test-name",
                "test name", "test\"name", "test.name", "a".repeat(129)}) {
            assertFalse(s.isSimpleIdentifier(id), id);
        }
        // JDK default NPEs on null (reference throws IllegalArgumentException).
        assertThrows(NullPointerException.class, () -> s.isSimpleIdentifier(null));
    }
}
