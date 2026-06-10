package io.github.danielbunting.clickhouse

/**
 * Thrown when a [ClickHouseConnection] is used by more than one operation at a
 * time — for example from two threads concurrently, or by starting a new operation
 * before a streaming [QueryResult] or an open [BulkInserter] from the same
 * connection has been closed.
 *
 * A connection is a single TCP socket driving a stateful request/response protocol
 * and is **not thread-safe**. Rather than let concurrent use silently corrupt the
 * wire stream, the connection fails fast with this exception. For concurrent access use
 * a [io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool] (or one
 * connection per thread).
 */
public open class ConcurrentConnectionUseException(message: String?) : ClickHouseException(message)
