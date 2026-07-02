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

    /**
     * Runs a SELECT with *server-side* [QueryParameters] AND per-query [settings]
     * (the combination of the two-argument overloads: the bindings travel on the wire
     * as typed placeholders while the settings override the connection's configured
     * defaults key-by-key for this query only).
     *
     * The default composes the existing overloads when either side is empty, so an
     * implementation that supports each individually gets the combination for free in
     * those cases; a genuinely combined call throws [UnsupportedOperationException]
     * unless overridden (the standard connection and the pooled wrapper override it).
     *
     * @param sql      the SELECT statement referencing `{name:Type}` placeholders
     * @param params   the server-side parameter bindings; may be null/empty
     * @param settings per-query server settings (insertion order preserved); may be empty
     * @return a lazy block iterator
     */
    public fun query(
        sql: String,
        params: QueryParameters?,
        settings: @JvmSuppressWildcards Map<String, String>,
    ): QueryResult {
        if (settings.isEmpty()) {
            return query(sql, params)
        }
        if (params == null || params.isEmpty()) {
            return query(sql, settings)
        }
        throw UnsupportedOperationException(
            "Combined server-side query parameters and per-query settings are not supported" +
                " by this connection"
        )
    }

    /**
     * Runs DDL/DML with server-side [QueryParameters] AND per-query [settings], no
     * result set. Same composition/default semantics as the combined [query] overload.
     */
    public fun execute(
        sql: String,
        params: QueryParameters?,
        settings: @JvmSuppressWildcards Map<String, String>,
    ) {
        if (settings.isEmpty()) {
            execute(sql, params)
            return
        }
        if (params == null || params.isEmpty()) {
            execute(sql, settings)
            return
        }
        throw UnsupportedOperationException(
            "Combined server-side query parameters and per-query settings are not supported" +
                " by this connection"
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

    /**
     * Opens a bulk inserter whose rows are mapped by a caller-supplied
     * [RowMapperFactory][io.github.danielbunting.clickhouse.mapping.RowMapperFactory] rather than
     * by introspecting [type], targeting every insertable column. Convenience for
     * [createBulkInserter] with a `null` column list.
     */
    public fun <T> createBulkInserter(
        table: String,
        type: Class<T>,
        mapperFactory: io.github.danielbunting.clickhouse.mapping.RowMapperFactory<T>,
    ): BulkInserter<T> {
        return createBulkInserter(table, type, null, mapperFactory)
    }

    /**
     * Opens a bulk inserter whose rows are mapped by a caller-supplied
     * [RowMapperFactory][io.github.danielbunting.clickhouse.mapping.RowMapperFactory] rather than
     * by introspecting [type]. The factory is invoked with the target's column names once the
     * server's sample block is read; this lets non-POJO sources (e.g. Arrow vectors) feed the
     * native inserter.
     *
     * If [columns] is non-null the INSERT names exactly those columns, so a caller can ingest a
     * subset and any omitted column takes its server-side DEFAULT; pass `null` to target every
     * insertable column. The default throws; native connections override it.
     */
    public fun <T> createBulkInserter(
        table: String,
        type: Class<T>,
        columns: List<String>?,
        mapperFactory: io.github.danielbunting.clickhouse.mapping.RowMapperFactory<T>,
    ): BulkInserter<T> {
        throw UnsupportedOperationException(
            "createBulkInserter with a RowMapperFactory is not supported by this connection"
        )
    }

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
    /**
     * Lightweight liveness probe (reference: the official client's `ping()`): reports
     * whether the server answers on this connection, without running a query. The
     * default implementation falls back to `SELECT 1`; the real connection overrides it
     * with the protocol-level `Ping`/`Pong` exchange. Never throws. While another
     * operation is in flight (e.g. a streaming `QueryResult` holds the connection) the
     * probe answers `true` without touching the socket: an active stream proves the
     * connection is alive, and interleaving a `Ping` mid-stream would corrupt it.
     */
    public fun ping(): Boolean {
        return try {
            executeScalar("SELECT 1") == 1L
        } catch (e: Exception) {
            false
        }
    }

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
