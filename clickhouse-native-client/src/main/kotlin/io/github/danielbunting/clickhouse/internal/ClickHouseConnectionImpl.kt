package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.BulkInserter
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.QueryResult
import io.github.danielbunting.clickhouse.mapping.RowMapper
import io.github.danielbunting.clickhouse.mapping.RowMappers
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.protocol.ServerPacket
import io.github.danielbunting.clickhouse.types.Column
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.util.Spliterator
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.stream.Stream
import java.util.stream.StreamSupport

/**
 * Default [ClickHouseConnection] implementation: a thin query/command
 * surface over a single [NativeClient] transport.
 *
 * This class drives the native-protocol flow described by [NativeClient]:
 * for SELECTs it sends the `Query` packet (and the trailing empty client data
 * block, handled inside [NativeClient.sendQuery]) then drains/decodes
 * server packets; for inserts it delegates to a [BulkInserter].
 *
 * **Not thread-safe.** A connection serves one caller at a time; the single
 * socket cannot multiplex queries. [queryAsync] merely offloads the
 * (still serial) call onto a daemon executor — callers must not run two operations
 * on the same connection concurrently.
 *
 * Task W2.2 / W2.5.
 */
public class ClickHouseConnectionImpl
/**
 * Wraps an already-connected, handshaken transport.
 *
 * @param client the native transport (constructed and connected by the caller)
 */
public constructor(private val client: NativeClient) : ClickHouseConnection {

    /**
     * Fail-fast guard that serializes logical operations on this single-socket
     * connection. Held for the duration of `execute`/`executeScalar`,
     * for a lazy [query] result until it is closed, and for a
     * [BulkInserter] from `init()` to `complete()`/`close()`.
     */
    private val guard = ConnectionGuard()

    init {
        // The FLATTENED native serialization that the self-framing Variant / Dynamic /
        // JSON codecs require is no longer enabled via a one-off SET statement here:
        // NativeClientImpl folds it into the per-query settings slot on every Query
        // packet (see NativeClientImpl#effectiveSettings), so it travels with the
        // request and survives without a separate round-trip. Callers can override it
        // through connection-default or per-query settings.
    }

    override fun executeScalar(sql: String): Long {
        return executeScalar(sql, QueryParameters.EMPTY)
    }

    override fun executeScalar(sql: String, params: QueryParameters?): Long {
        guard.acquire()
        try {
            return executeScalarLocked(sql, params)
        } finally {
            guard.release()
        }
    }

    private fun executeScalarLocked(sql: String, params: QueryParameters?): Long {
        client.sendQuery(sql, null, params)
        var result = 0L
        var haveValue = false
        var finished = false
        while (!finished) {
            val msg = client.readMessage()
            val type = msg.type()
            if (type == ServerPacket.END_OF_STREAM) {
                finished = true
            } else if (type == ServerPacket.EXCEPTION) {
                throw msg.exception()!!
            } else if (!haveValue
                && (type == ServerPacket.DATA
                    || type == ServerPacket.TOTALS
                    || type == ServerPacket.EXTREMES)
            ) {
                val block = msg.block()
                if (block != null && block.rowCount() > 0 && block.columnCount() > 0) {
                    result = scalarOf(block.column(0), 0)
                    haveValue = true
                }
            }
            // PROGRESS / PROFILE_INFO / header (0-row) block — ignored; keep draining.
        }
        if (!haveValue) {
            throw ProtocolException("executeScalar: query returned no rows: " + sql)
        }
        return result
    }

    override fun execute(sql: String) {
        execute(sql, QueryParameters.EMPTY)
    }

    override fun execute(sql: String, settings: @JvmSuppressWildcards Map<String, String>) {
        guard.acquire()
        try {
            executeLocked(sql, settings, QueryParameters.EMPTY)
        } finally {
            guard.release()
        }
    }

    override fun execute(sql: String, params: QueryParameters?) {
        guard.acquire()
        try {
            executeLocked(sql, null, params)
        } finally {
            guard.release()
        }
    }

    override fun execute(
        sql: String,
        params: QueryParameters?,
        settings: @JvmSuppressWildcards Map<String, String>,
    ) {
        guard.acquire()
        try {
            executeLocked(sql, settings, params)
        } finally {
            guard.release()
        }
    }

    private fun executeLocked(sql: String, settings: Map<String, String>?, params: QueryParameters?) {
        client.sendQuery(sql, settings, params)
        var finished = false
        while (!finished) {
            val msg = client.readMessage()
            val type = msg.type()
            if (type == ServerPacket.END_OF_STREAM) {
                finished = true
            } else if (type == ServerPacket.EXCEPTION) {
                throw msg.exception()!!
            }
            // DATA / PROGRESS / PROFILE_INFO — ignored for a no-result statement.
        }
    }

    override fun query(sql: String): QueryResult {
        return query(sql, QueryParameters.EMPTY)
    }

    override fun query(sql: String, settings: @JvmSuppressWildcards Map<String, String>): QueryResult {
        return queryInternal(sql, settings, QueryParameters.EMPTY)
    }

    override fun query(sql: String, params: QueryParameters?): QueryResult {
        return queryInternal(sql, null, params)
    }

    override fun query(
        sql: String,
        params: QueryParameters?,
        settings: @JvmSuppressWildcards Map<String, String>,
    ): QueryResult {
        return queryInternal(sql, settings, params)
    }

    private fun queryInternal(sql: String, settings: Map<String, String>?, params: QueryParameters?): QueryResult {
        // The connection stays "in use" until the lazy result is fully consumed or
        // closed; the QueryResult releases the guard at that point.
        guard.acquire()
        var handedOff = false
        try {
            client.sendQuery(sql, settings, params)
            val result: QueryResult = QueryResultImpl(client, Runnable { guard.release() })
            handedOff = true
            return result
        } finally {
            if (!handedOff) {
                guard.release()
            }
        }
    }

    override fun <T> query(sql: String, type: Class<T>): Stream<T> {
        // Lazy: the connection stays "in use" until the returned Stream is closed or
        // fully consumed. The guard is acquired here and released exactly once, wired
        // into the Stream's onClose (mirroring how query(String) hands the guard off
        // to the QueryResult). Does NOT call the public query(String) (which would
        // re-acquire the guard) — builds the lazy result directly.
        guard.acquire()
        var handedOff = false
        try {
            client.sendQuery(sql)
            // The QueryResult releases the guard exactly once when its stream is fully
            // drained or close()d; that same release is also wired into the Stream's
            // onClose below so a closed-but-unconsumed stream cannot leak the guard.
            val result: QueryResult = QueryResultImpl(client, Runnable { guard.release() })
            val stream = streamMapped(result, type)
            handedOff = true
            return stream
        } finally {
            if (!handedOff) {
                // Construction (sendQuery / header read / mapper build) failed before
                // the stream took ownership of the guard — release it here.
                guard.release()
            }
        }
    }

    override fun <T> createBulkInserter(table: String, type: Class<T>): BulkInserter<T> {
        // The inserter acquires the shared guard in init() and releases it in
        // complete()/close(). NativeClient exposes no config accessor, so the batch
        // size is supplied here.
        return BulkInserterImpl(client, table, type, batchSize(), guard)
    }

    override fun queryAsync(sql: String): CompletableFuture<QueryResult> {
        // NOTE: the connection is single-threaded. This does NOT add concurrency to
        // a single connection — it only offloads the blocking call to a daemon
        // thread. Do not issue another operation on this connection until the
        // returned future completes.
        return CompletableFuture.supplyAsync({ query(sql) }, ASYNC_EXECUTOR)
    }

    override fun cancel() {
        // Cross-thread by design: do NOT acquire the guard here — the querying thread
        // holds it. NativeClient.cancel() writes the Cancel packet to the output stream
        // while that thread reads the input stream.
        client.cancel()
    }

    override fun isPoisoned(): Boolean {
        return client.isPoisoned()
    }

    override fun ping(): Boolean {
        // The guard serializes the ping against any in-flight operation on this
        // single-threaded connection. A held guard means an operation is actively
        // streaming — the connection is demonstrably alive — and the socket must not
        // be touched mid-stream, so a busy connection answers true without probing.
        // tryAcquire (not acquire) keeps the interface's "never throws" contract.
        if (!guard.tryAcquire()) {
            return true
        }
        try {
            return client.ping()
        } finally {
            guard.release()
        }
    }

    override fun close() {
        client.close()
    }

    /**
     * The configured insert batch size. [NativeClient] does not expose the
     * config, so we fall back to ClickHouse's default block size. A future revision
     * could thread `ClickHouseConfig` into this class.
     *
     * @return the batch size for bulk inserts
     */
    private fun batchSize(): Int {
        // VERIFY: NativeClient has no config accessor in the frozen contract; using
        // the ClickHouse default block size (matches ClickHouseConfig's default).
        return 65_536
    }

    /**
     * Forward-only [Spliterator] that maps rows on demand by walking the
     * underlying block iterator one block at a time, mapping each row through
     * [mapper]. A single `Object[]` row buffer is reused across all rows
     * of a block (only the reference values it holds escape, into the mapped object),
     * so there is no per-row `Object[]` churn and no eager `List`.
     *
     * Blocks are pulled from [blocks] lazily: the first row becomes available
     * after the first block is fetched, not after the whole result is materialized.
     *
     * @param T the mapped row type
     */
    private class MappedRowSpliterator<T>(
        private val blocks: Iterator<Block>,
        private val mapper: RowMapper<T>,
        /** Run once if [tryAdvance] throws, to release the owning connection guard. */
        private val onFailure: Runnable,
    ) : Spliterator<T> {

        /** Current block being walked, or `null` before the first / after exhaustion. */
        private var block: Block? = null

        /** Next row index within [block]. */
        private var row = 0

        /** Row count of [block]. */
        private var rowCount = 0

        /** Reused positional row buffer, sized to the current block's column count. */
        private var rowBuffer: Array<Any?>? = null

        override fun tryAdvance(action: Consumer<in T>?): Boolean {
            if (action == null) {
                throw NullPointerException("action")
            }
            // Any failure while pulling/decoding/mapping a row (transport error, a server
            // EXCEPTION packet, a mapping error) must release the owning connection guard
            // even if the caller drives the spliterator manually without a terminal op or
            // try-with-resources (in which case the stream's onClose would not run). The
            // release callback (result::close) is idempotent, so this composes safely with
            // onClose on the normal close/exhaustion paths.
            try {
                // Advance to a block that still has rows to yield.
                var block = this.block
                while (block == null || row >= rowCount) {
                    if (!blocks.hasNext()) {
                        this.block = null
                        return false
                    }
                    block = blocks.next()
                    this.block = block
                    rowCount = block.rowCount()
                    row = 0
                    val cols = block.columnCount()
                    // Reuse the buffer across blocks when the width matches.
                    val buffer = rowBuffer
                    if (buffer == null || buffer.size != cols) {
                        rowBuffer = arrayOfNulls(cols)
                    }
                }
                val r = row++
                val rowBuffer = this.rowBuffer!!
                val cols = rowBuffer.size
                for (c in 0 until cols) {
                    rowBuffer[c] = valueAt(block.column(c), r)
                }
                // The mapper copies the values it needs out of the buffer; the buffer is
                // then reused for the next row.
                action.accept(mapper.map(rowBuffer))
                return true
            } catch (e: RuntimeException) {
                throw releaseOnFailure(e)
            } catch (e: Error) {
                throw releaseOnFailure(e)
            }
        }

        /** Runs [onFailure] (suppressing any secondary failure into [e]) and returns [e] for rethrow. */
        private fun releaseOnFailure(e: Throwable): Throwable {
            try {
                onFailure.run()
            } catch (suppressed: RuntimeException) {
                e.addSuppressed(suppressed)
            } catch (suppressed: Error) {
                e.addSuppressed(suppressed)
            }
            return e
        }

        override fun trySplit(): Spliterator<T>? {
            // Forward-only over a streamed transport: cannot be split for parallelism.
            return null
        }

        override fun estimateSize(): Long {
            return Long.MAX_VALUE // unknown row count
        }

        override fun characteristics(): Int {
            return Spliterator.ORDERED or Spliterator.NONNULL
        }
    }

    public companion object {

        /** Shared daemon executor backing [queryAsync]. */
        private val ASYNC_EXECUTOR: Executor = Executors.newCachedThreadPool(object : ThreadFactory {
            private val counter = AtomicLong()

            override fun newThread(r: Runnable): Thread {
                val t = Thread(r, "chnative-async-" + counter.incrementAndGet())
                t.isDaemon = true
                return t
            }
        })

        /**
         * Builds a lazy [Stream] of [T] over [result]'s block iterator,
         * mapping rows on demand (no eager `List`). Closing the stream (or exhausting
         * it) closes [result], which drains the transport and runs its release
         * callback — releasing the owning connection guard exactly once.
         *
         * Package-private in the Java original so it can be unit-tested with a fake
         * [QueryResult].
         *
         * @param T      the mapped row type
         * @param result the lazy query result (assumed to own the connection guard release)
         * @param type   the target row class
         * @return a lazy, single-use stream that must be closed (try-with-resources)
         */
        @JvmStatic
        public fun <T> streamMapped(result: QueryResult, type: Class<T>): Stream<T> {
            try {
                val columns = result.columnNames().toTypedArray()
                val mapper = RowMappers.forClass(type, *columns)
                val spliterator: Spliterator<T> =
                    MappedRowSpliterator(result.blocks(), mapper, Runnable { result.close() })
                // result.close() drains the stream AND runs the guard release callback; it
                // is idempotent, so calling it on close after natural exhaustion is safe.
                return StreamSupport.stream(spliterator, false).onClose { result.close() }
            } catch (e: RuntimeException) {
                // Header was read but mapper construction / block-iterator issue failed
                // before a stream could take ownership — close the result so the guard is
                // released, then rethrow.
                result.close()
                throw e
            }
        }

        // ------------------------------------------------------------------
        // Pure helpers (unit-tested)
        // ------------------------------------------------------------------

        /**
         * Coerces the value at [row] of [column] to a `long` scalar.
         *
         * Used by [executeScalar]. The value is obtained via the
         * column's codec (`codec().get(values, row)`); any [Number] is
         * narrowed to `long`, a [Boolean] maps to `1/0`, and a
         * numeric [String] is parsed. A `null` value (nullable column) is
         * rejected.
         *
         * @param column the source column
         * @param row    the row index
         * @return the value as a `long`
         * @throws ClickHouseException if the value is null or not coercible to a long
         */
        @JvmStatic
        public fun scalarOf(column: Column, row: Int): Long {
            return coerceLong(valueAt(column, row))
        }

        /**
         * Coerces a boxed value to `long`.
         *
         * @param value the boxed value (Number, Boolean, or numeric String)
         * @return the value as a `long`
         * @throws ClickHouseException if [value] cannot be represented as a long
         */
        @JvmStatic
        public fun coerceLong(value: Any?): Long {
            if (value == null) {
                throw ClickHouseException("executeScalar: scalar value is NULL")
            }
            if (value is Number) {
                return value.toLong()
            }
            if (value is Boolean) {
                return if (value) 1L else 0L
            }
            if (value is String) {
                try {
                    return java.lang.Long.parseLong(value.trim())
                } catch (e: NumberFormatException) {
                    throw ClickHouseException(
                        "executeScalar: cannot coerce String '" + value + "' to long", e
                    )
                }
            }
            throw ClickHouseException(
                "executeScalar: cannot coerce value of type "
                    + value.javaClass.name + " to long"
            )
        }

        /**
         * Extracts the value at [row] of [column], honoring the null-map.
         *
         * @param column the source column
         * @param row    the row index
         * @return the boxed value, or `null` for a nullable column whose row is null
         */
        @JvmStatic
        @Suppress("UNCHECKED_CAST")
        public fun valueAt(column: Column, row: Int): Any? {
            val nulls = column.nulls()
            if (column.isNullable && nulls != null && nulls[row]) {
                return null
            }
            val codec = column.codec() as ColumnCodec<Any>
            return codec.get(column.values() as Any, row)
        }
    }
}
