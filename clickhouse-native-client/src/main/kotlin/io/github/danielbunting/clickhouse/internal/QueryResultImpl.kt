package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.QueryResult
import io.github.danielbunting.clickhouse.ServerException
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.protocol.ServerPacket
import java.util.Collections
import java.util.NoSuchElementException

/**
 * Lazy [QueryResult] over the server's response stream for a single SELECT.
 *
 * Construction immediately reads server packets until the first `DATA`
 * packet arrives (the **header block**): a zero-row block describing the result
 * schema. The header is captured for [columnNames]/[columnTypes]
 * and is *not* yielded as a data block. [blocks] then returns a
 * forward-only iterator that pulls [NativeClient.readMessage] on demand,
 * yielding subsequent non-empty `DATA` blocks and stopping at
 * `END_OF_STREAM`.
 *
 * If the server sends an `EXCEPTION` packet at any point (including before
 * the header), the originating [ServerException] is thrown from the call that
 * encounters it (the constructor, or `hasNext()`/`next()`).
 *
 * Progress and profile-info packets are silently skipped.
 *
 * **Threading.** Not thread-safe; tied to the single-threaded
 * [NativeClient] it drives. The iterator must be fully consumed or
 * [close] must be called to drain the stream before the connection is
 * reused for another query.
 *
 * Task W2.2.
 */
internal class QueryResultImpl
/**
 * As the single-argument constructor, but invokes [onRelease] exactly
 * once when the stream is fully drained or [close]d — used to release the
 * owning connection's [ConnectionGuard].
 *
 * @param client    the transport that has already had its `Query` packet sent
 * @param onRelease released-once callback (may be `null` for a no-op)
 */
constructor(
    private val client: NativeClient,
    /** Invoked exactly once when the result stream is fully drained or closed (releases the connection guard). */
    private val onRelease: Runnable?,
) : QueryResult {

    private val header: Block
    private val columnNames: List<String>
    private val columnTypes: List<String>

    /** True once `END_OF_STREAM` has been observed (or the stream abandoned). */
    private var finished = false

    /** True once a [blocks] iterator has been handed out (single-use). */
    private var iteratorIssued = false

    /** True once [onRelease] has run (ensures the connection is released at most once). */
    private var released = false

    // ---- summary accumulation (Progress packets are increments; ProfileInfo is final) ----
    private var readRows = 0L
    private var readBytes = 0L
    private var totalRowsToRead = 0L
    private var writtenRows = 0L
    private var writtenBytes = 0L
    private var elapsedNanos = 0L
    private var appliedLimit = false
    private var rowsBeforeLimit = 0L

    /** Folds one server message into the running summary; returns the message. */
    private fun observe(msg: ServerMessage): ServerMessage {
        val progress = msg.progress()
        if (progress != null) {
            readRows += progress.rows
            readBytes += progress.bytes
            if (progress.totalRows > totalRowsToRead) {
                totalRowsToRead = progress.totalRows
            }
            writtenRows += progress.writtenRows
            writtenBytes += progress.writtenBytes
            elapsedNanos += progress.elapsedNanos
        }
        val profile = msg.profileInfo()
        if (profile != null) {
            appliedLimit = profile.appliedLimit
            rowsBeforeLimit = profile.rowsBeforeLimit
        }
        return msg
    }

    override fun summary(): io.github.danielbunting.clickhouse.QuerySummary {
        return io.github.danielbunting.clickhouse.QuerySummary(
            readRows, readBytes, totalRowsToRead,
            writtenRows, writtenBytes, elapsedNanos,
            appliedLimit, rowsBeforeLimit)
    }

    /**
     * Reads the response stream up to and including the header (first `DATA`)
     * block, capturing the result schema.
     *
     * @param client the transport that has already had its `Query` packet sent
     * @throws ServerException  if the server replied with an exception packet
     * @throws ProtocolException if the stream ended before any header block arrived
     */
    constructor(client: NativeClient) : this(client, null)

    init {
        var headerBlock: Block? = null
        // Pull packets until the schema-describing header DATA block appears.
        while (headerBlock == null) {
            val msg = observe(client.readMessage())
            val type = msg.type()
            if (type == ServerPacket.DATA) {
                // The first DATA packet is always the header (0-row schema) block.
                headerBlock = msg.block()
            } else if (type == ServerPacket.EXCEPTION) {
                this.finished = true
                markReleased()
                throw msg.exception()!!
            } else if (type == ServerPacket.END_OF_STREAM) {
                // No header at all (e.g. an empty/non-SELECT result). Treat as an
                // empty result with no columns rather than failing.
                this.finished = true
                markReleased()
                headerBlock = Block()
            }
            // PROGRESS / PROFILE_INFO / TOTALS / EXTREMES / LOG before the header
            // are ignored; keep reading.
        }

        this.header = headerBlock
        val names = ArrayList<String>(header.columnCount())
        val types = ArrayList<String>(header.columnCount())
        for (i in 0 until header.columnCount()) {
            val c = header.column(i)
            names.add(c.name())
            types.add(c.type())
        }
        this.columnNames = Collections.unmodifiableList(names)
        this.columnTypes = Collections.unmodifiableList(types)
    }

    override fun columnNames(): List<String> {
        return columnNames
    }

    override fun columnTypes(): List<String> {
        return columnTypes
    }

    /** The captured header block (zero rows); exposed for internal mapping helpers. */
    fun header(): Block {
        return header
    }

    override fun blocks(): Iterator<Block> {
        if (iteratorIssued) {
            throw IllegalStateException("blocks() may only be consumed once")
        }
        iteratorIssued = true
        return BlockIterator()
    }

    override fun close() {
        // Drain any remaining packets so the underlying connection is left in a
        // clean state for reuse. We do NOT close the client here — the connection
        // owns the client lifecycle. The connection guard is released regardless of
        // how the drain ends.
        try {
            while (!finished) {
                val msg = observe(client.readMessage())
                val type = msg.type()
                if (type == ServerPacket.END_OF_STREAM) {
                    finished = true
                } else if (type == ServerPacket.EXCEPTION) {
                    finished = true
                    throw msg.exception()!!
                }
                // ignore DATA/PROGRESS/PROFILE_INFO while draining
            }
        } finally {
            markReleased()
        }
    }

    /** Runs the release callback at most once, freeing the owning connection. */
    private fun markReleased() {
        if (!released) {
            released = true
            onRelease?.run()
        }
    }

    /**
     * Forward-only iterator that pulls the next non-empty `DATA` block from
     * the transport on demand. Empty blocks (the header was already consumed; any
     * further zero-row blocks are skipped) and progress/profile packets are
     * transparently skipped. `END_OF_STREAM` ends iteration; `EXCEPTION`
     * surfaces as a [ServerException].
     */
    private inner class BlockIterator : Iterator<Block> {

        /** Look-ahead buffer for the next block to return, or `null`. */
        private var nextBlock: Block? = null

        override fun hasNext(): Boolean {
            if (nextBlock != null) {
                return true
            }
            if (finished) {
                return false
            }
            nextBlock = pull()
            return nextBlock != null
        }

        override fun next(): Block {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            val result = nextBlock!!
            nextBlock = null
            return result
        }

        /**
         * Reads server packets until a non-empty data block, end-of-stream, or an
         * exception is reached.
         *
         * @return the next data block, or `null` at end of stream
         */
        private fun pull(): Block? {
            while (true) {
                val msg = observe(client.readMessage())
                val type = msg.type()
                if (type == ServerPacket.END_OF_STREAM) {
                    finished = true
                    markReleased()
                    return null
                } else if (type == ServerPacket.EXCEPTION) {
                    finished = true
                    markReleased()
                    throw msg.exception()!!
                } else if (type == ServerPacket.DATA
                    || type == ServerPacket.TOTALS
                    || type == ServerPacket.EXTREMES
                ) {
                    val b = msg.block()
                    if (b != null && !b.isEmpty) {
                        return b
                    }
                    // empty trailing block — skip
                }
                // PROGRESS / PROFILE_INFO / LOG — skip and continue.
            }
        }
    }
}
