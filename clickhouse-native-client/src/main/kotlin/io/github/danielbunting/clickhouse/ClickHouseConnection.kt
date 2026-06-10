package io.github.danielbunting.clickhouse

import io.github.danielbunting.clickhouse.internal.FailoverConnector
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream

/**
 * The public entry point: a single native-TCP connection to a ClickHouse server.
 * Owns the handshake, the packet dispatch loop, the codec registry, and
 * compression. Not thread-safe; one connection serves one caller at a time
 * (pool externally).
 *
 * **Contract frozen in W0.2.** The [open] factory and all methods are
 * implemented by an internal class in tasks W2.1–W2.5.
 */
public interface ClickHouseConnection : AutoCloseable {

    /** Runs a query returning a single numeric scalar, e.g. `SELECT 1`. */
    public fun executeScalar(sql: String): Long

    /** Runs DDL/DML with no result set. */
    public fun execute(sql: String)

    /**
     * Runs DDL/DML with no result set, applying [settings] as per-query ClickHouse
     * server settings (e.g. `max_execution_time`, `async_insert`). Per-query
     * settings override the connection's configured default settings key-by-key.
     *
     * @param sql      the statement
     * @param settings per-query server settings (insertion order preserved); may be empty
     */
    public fun execute(sql: String, settings: @JvmSuppressWildcards Map<String, String>) {
        execute(sql)
    }

    /** Runs a SELECT, returning a lazy block iterator. */
    public fun query(sql: String): QueryResult

    /**
     * Runs a SELECT returning a lazy block iterator, applying [settings] as per-query
     * ClickHouse server settings. Per-query settings override the connection's configured
     * default settings key-by-key.
     *
     * @param sql      the SELECT statement
     * @param settings per-query server settings (insertion order preserved); may be empty
     * @return a lazy block iterator
     */
    public fun query(sql: String, settings: @JvmSuppressWildcards Map<String, String>): QueryResult {
        return query(sql)
    }

    /**
     * Runs a SELECT with *server-side* [QueryParameters], returning a lazy block
     * iterator. The SQL references parameters as `{name:Type}`; the bindings travel
     * separately on the wire so the server parses each value against its declared `Type`
     * — no client-side string interpolation, hence no SQL-injection or type-fidelity hazard.
     * Build [QueryParameters] from a map via [QueryParameters.of].
     *
     * Default throws [UnsupportedOperationException]; the standard connection
     * (and the pooled wrapper) override it. The default keeps lightweight
     * implementations/fakes that only need the no-parameter surface source-compatible.
     */
    public fun query(sql: String, params: QueryParameters?): QueryResult {
        if (params == null || params.isEmpty()) {
            return query(sql)
        }
        throw UnsupportedOperationException(
            "Server-side query parameters are not supported by this connection"
        )
    }

    /** Runs DDL/DML with server-side [QueryParameters] and no result set. */
    public fun execute(sql: String, params: QueryParameters?) {
        if (params == null || params.isEmpty()) {
            execute(sql)
            return
        }
        throw UnsupportedOperationException(
            "Server-side query parameters are not supported by this connection"
        )
    }

    /** Runs a scalar query with server-side [QueryParameters]. */
    public fun executeScalar(sql: String, params: QueryParameters?): Long {
        if (params == null || params.isEmpty()) {
            return executeScalar(sql)
        }
        throw UnsupportedOperationException(
            "Server-side query parameters are not supported by this connection"
        )
    }

    /** Runs a SELECT, mapping each row to [type] via the object mapper (W1.E3). */
    public fun <T> query(sql: String, type: Class<T>): Stream<T>

    /** Opens a bulk inserter targeting [table], mapping [type]'s fields to columns. */
    public fun <T> createBulkInserter(table: String, type: Class<T>): BulkInserter<T>

    /** Asynchronous variant of [query]. */
    public fun queryAsync(sql: String): CompletableFuture<QueryResult>

    /**
     * Requests cancellation of the in-flight query on this connection by sending a
     * `Cancel` packet to the server.
     *
     * **Cross-thread by design.** Cancellation normally originates on a different
     * thread than the one blocked reading results; this is the one operation permitted
     * concurrently with an active query. The default implementation is a no-op for
     * connections that do not support cancellation.
     */
    public fun cancel() {
        // No-op by default; real connections override.
    }

    /**
     * Whether this connection is poisoned — its protocol stream is at an unknown offset or
     * its socket is broken after a protocol/I/O error (or a mid-INSERT desync). A poisoned
     * connection must not be reused; a `ClickHouseConnectionPool` discards (and
     * replaces) it on return instead of recycling it. A clean server query exception does
     * NOT poison the connection — it stays usable.
     *
     * @return `true` if the connection must be discarded rather than reused
     */
    public fun isPoisoned(): Boolean {
        return false
    }

    override fun close()

    public companion object {

        /**
         * Opens a connection and performs the handshake.
         *
         * When the config lists several [endpoints][Endpoint], they are tried in
         * [LoadBalancingPolicy] order with connect-time failover: the first endpoint that
         * connects wins, a connect failure falls over to the next, and if all fail an aggregated
         * [ConnectionException] (listing every attempt) is thrown. A single-host config
         * behaves exactly as before — a one-element endpoint list.
         */
        @JvmStatic
        public fun open(config: ClickHouseConfig): ClickHouseConnection {
            return FailoverConnector.forConfig(config).open()
        }
    }
}
