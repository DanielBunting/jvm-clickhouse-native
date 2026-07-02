package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * A do-nothing {@link ClickHouseConnection} for constructing {@link ChConnection}/
 * {@link ChPreparedStatement} in server-free unit tests that never actually execute
 * (e.g. parameter-metadata and placeholder-counting tests).
 */
class FakeCore implements ClickHouseConnection {

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
