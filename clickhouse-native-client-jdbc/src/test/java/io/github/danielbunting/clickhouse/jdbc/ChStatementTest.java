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
}
