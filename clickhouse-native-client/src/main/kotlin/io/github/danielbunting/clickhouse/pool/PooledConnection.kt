package io.github.danielbunting.clickhouse.pool

import io.github.danielbunting.clickhouse.BulkInserter
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.QueryResult
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Stream

/**
 * A [ClickHouseConnection] handed out by a [ClickHouseConnectionPool]. Every
 * operation delegates to the borrowed underlying connection (whose own guard still
 * enforces single-operation-at-a-time within the borrow); [close] returns the
 * underlying connection to the pool instead of closing the socket, so try-with-resources
 * works naturally.
 */
internal class PooledConnection(
    private val delegate: ClickHouseConnection,
    private val pool: ClickHouseConnectionPool,
) : ClickHouseConnection {

    private val returned = AtomicBoolean(false)

    override fun executeScalar(sql: String): Long {
        return delegate.executeScalar(sql)
    }

    override fun execute(sql: String) {
        delegate.execute(sql)
    }

    override fun execute(sql: String, settings: Map<String, String>) {
        delegate.execute(sql, settings)
    }

    override fun query(sql: String): QueryResult {
        return delegate.query(sql)
    }

    override fun query(sql: String, settings: Map<String, String>): QueryResult {
        return delegate.query(sql, settings)
    }

    override fun query(sql: String, params: QueryParameters?): QueryResult {
        return delegate.query(sql, params)
    }

    override fun execute(sql: String, params: QueryParameters?) {
        delegate.execute(sql, params)
    }

    override fun executeScalar(sql: String, params: QueryParameters?): Long {
        return delegate.executeScalar(sql, params)
    }

    override fun <T> query(sql: String, type: Class<T>): Stream<T> {
        return delegate.query(sql, type)
    }

    override fun <T> createBulkInserter(table: String, type: Class<T>): BulkInserter<T> {
        return delegate.createBulkInserter(table, type)
    }

    override fun queryAsync(sql: String): CompletableFuture<QueryResult> {
        return delegate.queryAsync(sql)
    }

    override fun cancel() {
        delegate.cancel()
    }

    override fun isPoisoned(): Boolean {
        return delegate.isPoisoned()
    }

    /** Returns the underlying connection to the pool (idempotent). */
    override fun close() {
        if (returned.compareAndSet(false, true)) {
            pool.returnConnection(delegate)
        }
    }
}
