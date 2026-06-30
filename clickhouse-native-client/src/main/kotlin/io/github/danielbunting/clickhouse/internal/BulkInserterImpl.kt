package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.BulkInserter
import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.ServerException
import io.github.danielbunting.clickhouse.mapping.ColumnBinder
import io.github.danielbunting.clickhouse.mapping.RowMapper
import io.github.danielbunting.clickhouse.mapping.RowMapperFactory
import io.github.danielbunting.clickhouse.mapping.RowMappers
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.protocol.ServerPacket
import io.github.danielbunting.clickhouse.types.Column
import io.github.danielbunting.clickhouse.types.ColumnCodec
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry

/**
 * Allocation-lean [BulkInserter] driven by a [NativeClient]. Rows are
 * accumulated into per-column primitive buffers (column-major) and shipped as
 * native `Data` blocks; no per-row SQL or boxing beyond what the
 * [RowMapper] produces.
 *
 * Flow (native INSERT, ported from CH.Native):
 *
 *  1. [init] sends `INSERT INTO <table> VALUES` via
 *     [NativeClient.sendQuery], then reads server messages until the
 *     sample/header `Data` block arrives. That block defines the target
 *     column names and types; from it the per-column codecs and value buffers are
 *     built.
 *  2. [add] / [addRange] map rows into the column
 *     buffers, flushing a full `Data` block when the batch fills.
 *  3. [complete] flushes any remainder, sends the terminating empty
 *     `Data` block, then drains server messages until
 *     [ServerPacket.END_OF_STREAM].
 *
 * An [ServerPacket.EXCEPTION] packet at any point is decoded and rethrown
 * as a [ServerException].
 *
 * Not thread-safe; intended for single-threaded use within a try-with-resources
 * block. [close] is idempotent and does **not** close the underlying
 * [NativeClient]; the owning `ClickHouseConnection` owns that lifecycle.
 *
 * @param T the row type, mapped to columns by [RowMapper]
 */
public class BulkInserterImpl<T>
/**
 * As the four-argument constructor, but driven by the
 * owning connection's shared [ConnectionGuard] so that a concurrent operation
 * on the same connection fails fast rather than corrupting the protocol stream.
 *
 * @param guard the owning connection's guard (acquired in [init])
 */
internal constructor(
    client: NativeClient?,
    table: String?,
    type: Class<T>?,
    batchSize: Int,
    /** Connection guard held from [init] to [complete]/[close]. */
    private val guard: ConnectionGuard,
    /**
     * Optional override that builds the [RowMapper] from the target column names instead of
     * introspecting [type]. When set, the reflective `bind`-into-scratch path is always used
     * (the typed [ColumnBinder] fast path, which needs a POJO, is disabled).
     */
    private val mapperFactory: RowMapperFactory<T>? = null,
    /**
     * Explicit target column list. When non-null the INSERT names these columns
     * (`INSERT INTO t (a, b) VALUES`), so the sample block — and thus the inserted set — is
     * restricted to them and any omitted column takes its server-side DEFAULT. When null all of
     * the table's insertable columns are used.
     */
    private val insertColumns: List<String>? = null,
) : BulkInserter<T> {

    private val client: NativeClient
    private val table: String
    private val type: Class<T>
    private val batchSize: Int

    /** Target columns from the sample block; reused as buffer holders across blocks. */
    private var columns: Array<Column?>? = null

    /** Codec per column, resolved from the sample block (codec()-or-registry). */
    private var codecs: Array<ColumnCodec<*>?>? = null

    /** Mapper from [T] to a positional row aligned with [columns]. */
    private var mapper: RowMapper<T>? = null

    /**
     * Per-column typed binders that write primitives straight into the column arrays
     * (bypassing [rowScratch]) for numeric columns; `null` when binder
     * construction failed and the reflective [RowMapper.bind] path is used.
     */
    private var binders: Array<ColumnBinder>? = null

    /** Scratch row buffer reused by [RowMapper.bind]; length == column count. */
    private var rowScratch: Array<Any?>? = null

    /** Number of rows currently buffered (and the write cursor into the column arrays). */
    private var bufferedRows = 0

    private var initialized = false
    private var completed = false
    private var closed = false

    /**
     * Whether the server terminated this INSERT with an `Exception` packet (in
     * [complete] or during init's sample-block read). A server exception is a TERMINAL,
     * in-spec wire event: the query is over and the connection is reusable — [close]
     * must neither poison nor try to send a terminating block after it.
     */
    private var serverErrored = false

    init {
        if (client == null) {
            throw IllegalArgumentException("client must not be null")
        }
        if (table == null || table.isEmpty()) {
            throw IllegalArgumentException("table must not be null or empty")
        }
        if (type == null) {
            throw IllegalArgumentException("type must not be null")
        }
        if (batchSize <= 0) {
            throw IllegalArgumentException("batchSize must be positive: " + batchSize)
        }
        this.client = client
        this.table = table
        this.type = type
        this.batchSize = batchSize
    }

    /**
     * Creates a bulk inserter. Does not connect or send anything; call
     * [init] first.
     *
     * @param client    the transport to drive, already connected and handshaked
     * @param table     the target table name (used verbatim in the INSERT query)
     * @param type      the row class, introspected by the [RowMapper]
     * @param batchSize the maximum rows buffered before an automatic flush
     */
    public constructor(client: NativeClient?, table: String?, type: Class<T>?, batchSize: Int) :
        this(client, table, type, batchSize, ConnectionGuard())

    /**
     * Sends `INSERT INTO <table> VALUES` and reads server messages until the
     * sample/header `Data` block arrives (rowCount 0). From that block this
     * resolves each column's [ColumnCodec] (preferring a codec already set by
     * `BlockCodec`, else resolving the raw type via a
     * [DefaultCodecRegistry] seeded with the server timezone), allocates a
     * per-column value array of [batchSize] (plus a null-map for nullable
     * columns), and builds the [RowMapper] keyed on the sample column names.
     */
    override fun init() {
        ensureOpen()
        if (initialized) {
            throw IllegalStateException("init() already called")
        }
        // Hold the connection for the whole insert; released in complete()/close().
        guard.acquire()
        try {
            initLocked()
        } catch (e: io.github.danielbunting.clickhouse.ServerException) {
            // The server rejected the INSERT (e.g. unknown table) with a terminal
            // Exception packet: the query is over and the wire is in spec, so the
            // connection remains reusable. Record it so close() stays a no-op.
            serverErrored = true
            guard.release()
            throw e
        } catch (e: RuntimeException) {
            // initLocked has already sent "INSERT INTO ... VALUES"; any other failure
            // leaves the connection mid-INSERT (the server is awaiting data), so it
            // must not be reused.
            client.markPoisoned()
            guard.release()
            throw e
        }
    }

    /** Performs the actual init I/O; the connection guard is already held. */
    private fun initLocked() {
        val columnClause = insertColumns
            ?.joinToString(", ", " (", ")") { "`" + it + "`" }
            ?: ""
        client.sendQuery("INSERT INTO " + table + columnClause + " VALUES")

        val sample = readSampleBlock()
        val columnCount = sample.columnCount()
        if (columnCount == 0) {
            throw ProtocolException(
                "INSERT sample block for table '" + table + "' has no columns"
            )
        }

        val columns = arrayOfNulls<Column>(columnCount)
        val codecs = arrayOfNulls<ColumnCodec<*>>(columnCount)
        this.columns = columns
        this.codecs = codecs
        this.rowScratch = arrayOfNulls(columnCount)
        val columnNames = arrayOfNulls<String>(columnCount)
        var fallback: DefaultCodecRegistry? = null

        for (i in 0 until columnCount) {
            val sampleCol = sample.column(i)
            val col = Column(sampleCol.name(), sampleCol.type())

            var codec = sampleCol.codec()
            if (codec == null) {
                if (fallback == null) {
                    fallback = DefaultCodecRegistry(client.serverTimezone())
                }
                codec = fallback.resolve(sampleCol.type())
            }
            col.codec(codec)
            col.values(codec.allocate(batchSize))
            // A non-null sample null-map signals a Nullable(...) column; allocate ours.
            if (sampleCol.isNullable) {
                col.nulls(BooleanArray(batchSize))
            }

            columns[i] = col
            codecs[i] = codec
            columnNames[i] = sampleCol.name()
        }

        // The name and codec arrays are fully populated by the loop above; the cast
        // mirrors the Java original's non-null String[] / ColumnCodec<?>[] locals.
        @Suppress("UNCHECKED_CAST")
        val names = columnNames as Array<String>
        @Suppress("UNCHECKED_CAST")
        val resolvedCodecs = codecs as Array<ColumnCodec<*>>

        val mapperFactory = this.mapperFactory
        if (mapperFactory != null) {
            // Caller-supplied mapper (e.g. Arrow-backed): always use the reflective scratch path.
            this.mapper = mapperFactory.create(names)
            this.binders = null
        } else {
            this.mapper = RowMappers.forClass(type, *names)

            // Build typed binders for the write path. If anything goes wrong, fall back
            // entirely to the reflective Object-scratch path (binders == null).
            try {
                val nullableFlags = BooleanArray(columnCount)
                for (i in 0 until columnCount) {
                    nullableFlags[i] = columns[i]!!.isNullable
                }
                this.binders = RowMappers.columnBinders(type, names, resolvedCodecs, nullableFlags)
            } catch (e: RuntimeException) {
                this.binders = null
            }
        }

        this.bufferedRows = 0
        this.initialized = true
    }

    /**
     * Reads server messages until the sample/header `Data` block (the first
     * `Data` packet) is seen, ignoring interleaved `Progress`/
     * `ProfileInfo`. An `Exception` packet is rethrown; an unexpected
     * terminal packet is a protocol violation.
     */
    private fun readSampleBlock(): Block {
        while (true) {
            val msg = client.readMessage()
            when (msg.type()) {
                ServerPacket.DATA, ServerPacket.TOTALS, ServerPacket.EXTREMES ->
                    return msg.block()!!
                ServerPacket.EXCEPTION ->
                    throw msg.exception()!!
                ServerPacket.PROGRESS, ServerPacket.PROFILE_INFO, ServerPacket.LOG,
                ServerPacket.PROFILE_EVENTS, ServerPacket.TABLE_COLUMNS, ServerPacket.PONG -> {
                    // Informational; keep reading for the sample block.
                }
                ServerPacket.END_OF_STREAM ->
                    throw ProtocolException(
                        "Server ended INSERT stream before sending a sample block for '"
                            + table + "'"
                    )
                else ->
                    throw ProtocolException(
                        "Unexpected packet while awaiting INSERT sample block: " + msg.type()
                    )
            }
        }
    }

    /**
     * Maps [row] into the column buffers at the current cursor via
     * [RowMapper.bind] and the per-column [ColumnCodec.set]. A null field
     * on a nullable column sets the column's null-map bit. When the cursor reaches
     * [batchSize] a [Block] is flushed automatically.
     */
    override fun add(row: T) {
        ensureInitialized()

        val r = bufferedRows
        val columns = this.columns!!
        val binders = this.binders
        if (binders != null) {
            // Fast path: per-column typed binders write primitives straight into the
            // column arrays with no Object[] scratch and no boxing for numeric columns.
            for (i in columns.indices) {
                val col = columns[i]!!
                val nulls = col.nulls()
                if (nulls != null) {
                    nulls[r] = false // reset; OBJECT binder sets it true for null values
                }
                // row!! mirrors the Java original's implicit NPE on a null row: the
                // binder's getters dereference the row object unconditionally.
                binders[i].bind(row!!, col.values(), r, nulls)
            }
        } else {
            // Fallback: reflective Object-scratch path.
            val codecs = this.codecs!!
            val rowScratch = this.rowScratch!!
            mapper!!.bind(row, rowScratch)
            for (i in columns.indices) {
                val col = columns[i]!!
                val value = rowScratch[i]
                val nulls = col.nulls()
                if (value == null) {
                    if (nulls != null) {
                        nulls[r] = true
                        // Leave the value slot at the codec's default; it is masked by the null-map.
                        continue
                    }
                    // A non-nullable column cannot store null; fail clearly rather than letting the
                    // codec NPE while unboxing (or silently coerce, e.g. Array -> []).
                    throw IllegalArgumentException(
                        "Cannot insert null into non-nullable column '" + col.name() + "'")
                }
                if (nulls != null) {
                    nulls[r] = false
                }
                setValue(codecs[i]!!, col.values(), r, value)
                // Clear the scratch slot to avoid retaining references after binding.
                rowScratch[i] = null
            }
        }

        bufferedRows = r + 1
        if (bufferedRows >= batchSize) {
            flush()
        }
    }

    override fun addRange(rows: Iterable<T>) {
        ensureInitialized()
        // No manual null check: the parameter is non-null in the BulkInserter contract,
        // and Kotlin's compiler-generated parameter guard already rejects a null from
        // Java callers before the body runs.
        for (row in rows) {
            add(row)
        }
    }

    /**
     * Flushes any buffered rows, sends the terminating empty `Data` block,
     * then drains server messages until [ServerPacket.END_OF_STREAM]. An
     * `Exception` packet is rethrown.
     */
    override fun complete() {
        ensureInitialized()
        try {
            flush()
            client.sendEmptyData()

            while (true) {
                val msg = client.readMessage()
                when (msg.type()) {
                    ServerPacket.END_OF_STREAM -> {
                        completed = true
                        return
                    }
                    ServerPacket.EXCEPTION -> {
                        serverErrored = true
                        throw msg.exception()!!
                    }
                    ServerPacket.DATA, ServerPacket.TOTALS, ServerPacket.EXTREMES,
                    ServerPacket.PROGRESS, ServerPacket.PROFILE_INFO, ServerPacket.LOG,
                    ServerPacket.PROFILE_EVENTS -> {
                        // Trailing informational/echo packets; keep draining.
                    }
                    else -> {
                        // The terminating empty block is already on the wire and an
                        // unknown packet was consumed: the wire position is lost, so
                        // close() must not send a second terminator and a pool must
                        // discard this connection.
                        client.markPoisoned()
                        throw ProtocolException(
                            "Unexpected packet while completing INSERT: " + msg.type()
                        )
                    }
                }
            }
        } finally {
            // The insert is over (success or failure); free the connection.
            guard.release()
        }
    }

    /**
     * Sends the currently buffered rows as one `Data` block (no-op when empty),
     * then resets the row cursor so the column arrays are reused for the next batch.
     */
    private fun flush() {
        if (bufferedRows == 0) {
            return
        }

        val block = Block()
        val rows = bufferedRows
        val columns = this.columns!!
        for (i in columns.indices) {
            val col = columns[i]!!
            col.rowCount(rows)
            block.addColumn(col)
        }
        block.rowCount(rows)

        client.sendData(block)
        bufferedRows = 0
    }

    /**
     * Idempotent. Releases buffer references but intentionally does not close the
     * underlying [NativeClient], whose lifecycle is owned by the
     * `ClickHouseConnection`.
     */
    override fun close() {
        if (closed) {
            return
        }
        closed = true
        // An INSERT that started (initialized) but did not complete() leaves the server
        // awaiting data. When the wire is still byte-clean (no I/O failure poisoned the
        // client, and the server did not already terminate the query with an Exception
        // packet), END the INSERT gracefully: discard any buffered-but-unflushed rows
        // (abandonment must not commit surprise data), send the terminating empty block,
        // and drain to END_OF_STREAM. The connection then stays healthy and reusable.
        // Only if that graceful termination itself fails is the wire genuinely desynced,
        // and only then is the connection poisoned (so a pool discards it).
        if (initialized && !completed && !serverErrored && !client.isPoisoned()) {
            try {
                bufferedRows = 0
                client.sendEmptyData()
                drainQuietlyToEndOfStream()
            } catch (t: Throwable) {
                client.markPoisoned()
            }
        }
        // Release the connection guard (idempotent — complete() may have released it).
        guard.release()
        // Drop references so the buffers can be collected; do not touch the client.
        columns = null
        codecs = null
        rowScratch = null
        mapper = null
        binders = null
    }

    /**
     * Drains server messages until `END_OF_STREAM`, for [close]'s graceful termination.
     * A server `Exception` here is terminal and in-spec (the INSERT is over either way),
     * so it ends the drain WITHOUT throwing — close() must not mask the original error
     * that aborted the insert. Any protocol violation propagates to close()'s catch,
     * which poisons.
     */
    private fun drainQuietlyToEndOfStream() {
        while (true) {
            val msg = client.readMessage()
            when (msg.type()) {
                ServerPacket.END_OF_STREAM -> return
                ServerPacket.EXCEPTION -> {
                    serverErrored = true
                    return
                }
                ServerPacket.DATA, ServerPacket.TOTALS, ServerPacket.EXTREMES,
                ServerPacket.PROGRESS, ServerPacket.PROFILE_INFO, ServerPacket.LOG,
                ServerPacket.PROFILE_EVENTS -> {
                    // Trailing informational/echo packets; keep draining.
                }
                else ->
                    throw ProtocolException(
                        "Unexpected packet while terminating INSERT: " + msg.type()
                    )
            }
        }
    }

    private fun ensureOpen() {
        if (closed) {
            throw IllegalStateException("BulkInserter is closed")
        }
    }

    private fun ensureInitialized() {
        ensureOpen()
        if (!initialized) {
            throw IllegalStateException("init() must be called before use")
        }
    }

    private companion object {

        /**
         * Sets [value] at [row] in [array] through [codec],
         * bridging the wildcard codec/array generics with an unchecked cast that is safe
         * because [array] is exactly what `codec.allocate` produced.
         */
        @Suppress("UNCHECKED_CAST")
        private fun setValue(codec: ColumnCodec<*>, array: Any?, row: Int, value: Any?) {
            (codec as ColumnCodec<Any>).set(array as Any, row, value)
        }
    }
}
