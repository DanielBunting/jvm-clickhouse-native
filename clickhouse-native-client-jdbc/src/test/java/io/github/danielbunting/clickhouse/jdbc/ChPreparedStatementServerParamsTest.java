package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the server-side query-parameter binding path of
 * {@link ChPreparedStatement} (enabled by the {@code server_side_params} connection
 * property): {@code ?} → {@code {_pN:String}} rewriting and value binding. The core
 * connection is a fake that records the SQL and {@link QueryParameters} it receives.
 */
class ChPreparedStatementServerParamsTest {

    /** Records the SQL + params passed to the server-side query/execute overloads. */
    private static final class RecordingCore implements ClickHouseConnection {
        final List<String> sql = new ArrayList<>();
        final List<QueryParameters> params = new ArrayList<>();

        @Override
        public long executeScalar(String s) {
            return 0;
        }

        @Override
        public void execute(String s) {
            sql.add(s);
            params.add(QueryParameters.EMPTY);
        }

        @Override
        public void execute(String s, QueryParameters p) {
            sql.add(s);
            params.add(p);
        }

        @Override
        public QueryResult query(String s) {
            sql.add(s);
            params.add(QueryParameters.EMPTY);
            return emptyResult();
        }

        @Override
        public QueryResult query(String s, QueryParameters p) {
            sql.add(s);
            params.add(p);
            return emptyResult();
        }

        @Override
        public <T> Stream<T> query(String s, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }

        /** A no-column, no-row result so {@code executeQuery} can wrap it in a ChResultSet. */
        private static QueryResult emptyResult() {
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
                    return Collections.<Block>emptyList().iterator();
                }

                @Override
                public void close() {
                    // no-op
                }
            };
        }
    }

    private static ChConnection serverParamConn(RecordingCore core) {
        Properties info = new Properties();
        info.setProperty("server_side_params", "true");
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", info);
    }

    // ---- rewrite helper -----------------------------------------------------

    @Test
    void rewritesPositionalPlaceholdersToNamedTypedParams() {
        assertEquals(
                "SELECT {_p1:String} WHERE a = {_p2:String}",
                ChPreparedStatement.rewriteToNamedParams("SELECT ? WHERE a = ?"));
    }

    @Test
    void rewriteLeavesQuestionMarksInsideStringLiterals() {
        assertEquals(
                "SELECT {_p1:String} WHERE s = 'is it?'",
                ChPreparedStatement.rewriteToNamedParams("SELECT ? WHERE s = 'is it?'"));
    }

    // ---- end-to-end through the statement -----------------------------------

    @Test
    void executeUpdateSendsRewrittenSqlAndBoundParams() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                serverParamConn(core), "INSERT INTO t (id, name) VALUES (?, ?)");
        ps.setInt(1, 7);
        ps.setString(2, "a'b");
        ps.executeUpdate();

        assertEquals(1, core.sql.size());
        assertEquals(
                "INSERT INTO t (id, name) VALUES ({_p1:String}, {_p2:String})",
                core.sql.get(0));
        QueryParameters p = core.params.get(0);
        // Values are sent unquoted; the server quotes/casts via {name:String}.
        assertEquals("7", p.wireValue("_p1"));
        assertEquals("a'b", p.wireValue("_p2"));
    }

    @Test
    void executeQueryRoutesThroughServerSideQuery() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                serverParamConn(core), "SELECT * FROM t WHERE id = ?");
        ps.setLong(1, 99L);
        ps.executeQuery();

        assertEquals("SELECT * FROM t WHERE id = {_p1:String}", core.sql.get(0));
        assertEquals("99", core.params.get(0).wireValue("_p1"));
    }

    @Test
    void nullBindingSendsNullSentinel() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                serverParamConn(core), "INSERT INTO t (x) VALUES (?)");
        ps.setNull(1, java.sql.Types.VARCHAR);
        ps.executeUpdate();

        assertEquals("\\N", core.params.get(0).wireValue("_p1"));
    }

    @Test
    void batchExecutesEachRowAsParameterizedStatement() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                serverParamConn(core), "INSERT INTO t (id, name) VALUES (?, ?)");

        ps.setInt(1, 1);
        ps.setString(2, "alice");
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setString(2, "bob");
        ps.addBatch();

        int[] counts = ps.executeBatch();

        assertEquals(2, counts.length);
        assertEquals(2, core.sql.size());
        assertEquals(
                "INSERT INTO t (id, name) VALUES ({_p1:String}, {_p2:String})",
                core.sql.get(0));
        assertEquals("1", core.params.get(0).wireValue("_p1"));
        assertEquals("alice", core.params.get(0).wireValue("_p2"));
        assertEquals("2", core.params.get(1).wireValue("_p1"));
        assertEquals("bob", core.params.get(1).wireValue("_p2"));
    }

    @Test
    void unboundParameterIsTreatedAsNull() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                serverParamConn(core), "INSERT INTO t (x) VALUES (?)");
        // No setX call: the parameter is unbound.
        ps.executeUpdate();
        assertEquals("\\N", core.params.get(0).wireValue("_p1"));
        // Sanity: an unrelated name is unbound.
        assertNull(core.params.get(0).wireValue("_p2"));
    }
}
