package io.github.danielbunting.clickhouse.test;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * A do-nothing {@link ClickHouseConnection} for constructing driver-layer wrappers (JDBC
 * {@code ChConnection}, ADBC {@code ChAdbcConnection}, …) in server-free unit tests that never
 * actually execute. Every method is overridable; the query-shaped ones throw
 * {@link UnsupportedOperationException} so a test that unexpectedly reaches the wire fails loudly.
 *
 * <p>For tests that need recorded SQL, scripted results or scripted failures, use
 * {@link ScriptedConnection}.
 */
public class FakeClickHouseConnection implements ClickHouseConnection {

    @Override
    public long executeScalar(String sql) {
        return 0;
    }

    @Override
    public void execute(String sql) {
    }

    @Override
    public QueryResult query(String sql) {
        throw new UnsupportedOperationException();
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
