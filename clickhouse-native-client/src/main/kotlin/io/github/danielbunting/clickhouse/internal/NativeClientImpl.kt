package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ConnectionException
import io.github.danielbunting.clickhouse.Endpoint
import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.ServerException
import io.github.danielbunting.clickhouse.compress.CompressedBlockCodec
import io.github.danielbunting.clickhouse.compress.CompressedFrameInputStream
import io.github.danielbunting.clickhouse.compress.CompressionMethod
import io.github.danielbunting.clickhouse.compress.Compressor
import io.github.danielbunting.clickhouse.compress.Compressors
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.protocol.BlockCodec
import io.github.danielbunting.clickhouse.protocol.ClientPacket
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryReader
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryWriter
import io.github.danielbunting.clickhouse.protocol.Handshake
import io.github.danielbunting.clickhouse.protocol.ServerHello
import io.github.danielbunting.clickhouse.protocol.ServerPacket
import io.github.danielbunting.clickhouse.protocol.ServerPacketReader
import io.github.danielbunting.clickhouse.types.CodecRegistry
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.time.ZoneId
import java.util.LinkedHashMap
import javax.net.ssl.SNIHostName
import javax.net.ssl.SNIServerName
import javax.net.ssl.SSLSocket

/**
 * Default [NativeClient]: a single ClickHouse native-TCP connection.
 *
 * Owns one [Socket], a [DefaultBinaryReader]/[DefaultBinaryWriter]
 * pair over its streams, and the per-connection [CodecRegistry] (seeded with the
 * server timezone so `DateTime` columns decode correctly). The constructor opens
 * the socket and completes the native handshake; from then on the higher layers drive
 * [sendQuery], [sendData], [sendEmptyData] and [readMessage].
 *
 * **Wire-format authority is the CH.Native .NET source.** The `Query` packet
 * field order and the protocol-revision gating constants below are the standard
 * ClickHouse-native layout; every position that needs a final cross-check against
 * CH.Native is annotated `// VERIFY against CH.Native`.
 *
 * Task W2.1 + W2.3.
 */
public class NativeClientImpl
/**
 * Opens a socket to [endpoint], completes the native handshake, and prepares the
 * per-connection codec registry. The [endpoint]'s host/port override the config's
 * single-host fields, letting the failover open path target any configured node while
 * reusing all other config (credentials, timeouts, compression).
 *
 * @param config   the connection configuration. Must not be `null`.
 * @param endpoint the specific endpoint to connect to. Must not be `null`.
 * @throws ConnectionException if the socket cannot be opened or the handshake fails
 */
public constructor(config: ClickHouseConfig?, endpoint: Endpoint?) : NativeClient {

    // --- connection state ------------------------------------------------

    private val config: ClickHouseConfig
    private val socket: Socket
    private val reader: DefaultBinaryReader
    private val writer: DefaultBinaryWriter
    private val hello: ServerHello
    private val timezone: ZoneId
    private val registry: CodecRegistry
    private val compressionEnabled: Boolean

    /**
     * Serializes all writes to the shared output stream. Normal send operations
     * (`sendQuery`/`sendData`/`sendEmptyData`) and a cross-thread
     * [cancel] may run on different threads; the `BufferedOutputStream`
     * is not thread-safe, so every method that touches the writer holds this monitor
     * for the duration of its frame. The read path (`readMessage`) is on the
     * other half-duplex stream and is intentionally NOT guarded by this lock, so a
     * reader blocked in `readMessage()` never blocks a cancelling thread from
     * writing the `Cancel` packet.
     */
    private val writeLock = Any()

    /**
     * Set once a `Cancel` packet has been written for the current query. Purely
     * informational (e.g. for callers that want to distinguish a cancelled drain from
     * a natural end-of-stream); it does not gate the wire protocol.
     */
    @Volatile
    private var cancelled = false

    /**
     * Per-connection staging buffer + writer + compressor for the compressed block
     * write path, reused across blocks instead of allocated per block.
     *
     * These are mutable, non-thread-safe scratch objects. They are safe to reuse
     * because this client serializes every operation behind a single-operation guard
     * (one outstanding `sendQuery`/`sendData` at a time on a connection),
     * and each is an *instance* field — never static — so buffers are never
     * shared across connections. They are lazily created on first compressed write
     * and remain `null` when compression is disabled.
     */
    private var compressStaging: ResettableByteArrayOutputStream? = null
    private var compressStagingWriter: DefaultBinaryWriter? = null
    private var compressor: Compressor? = null

    /**
     * Per-connection compressed-block READ stream + reader, reused across blocks instead
     * of allocated per block (the dual of the compress-staging write fields above).
     *
     * [compressedFrames] wraps the connection's single socket [reader] once
     * and is [rewound][CompressedFrameInputStream.resetFrames] before each block; it
     * never reads ahead, so after each block the socket reader sits exactly at the next packet
     * code. [compressedReader] is [unbuffered][DefaultBinaryReader.unbuffered]
     * on purpose: it holds NO leftover bytes between reads, so rewinding the frame stream
     * cannot leave stale bytes that would corrupt the next block.
     *
     * Safe to reuse for the same reasons as the write-path staging fields: the client
     * serializes every operation (one outstanding read at a time on a connection), and
     * these are *instance* fields — never static — so they are never shared across
     * connections. Lazily created on the first compressed read; remain `null` when
     * compression is disabled or only uncompressed (telemetry) blocks are read.
     */
    private var compressedFrames: CompressedFrameInputStream? = null
    private var compressedReader: DefaultBinaryReader? = null

    @Volatile
    private var closed = false

    /**
     * Set once the wire is at an unknown offset or the socket is broken — i.e. after a
     * [ProtocolException] or [ConnectionException] (I/O) escapes a read/write,
     * or a mid-INSERT failure leaves the stream desynced. A clean server query exception
     * does NOT set this (the wire stays in spec and the connection is reusable). The pool
     * uses [isPoisoned] to discard a borrowed connection on return instead of
     * recycling stale/broken state.
     */
    @Volatile
    private var poisoned = false

    /**
     * Opens a socket to the config's first resolved endpoint, completes the native
     * handshake, and prepares the per-connection codec registry.
     *
     * This single-endpoint constructor is kept for callers (and tests) that do not
     * use failover. The failover-aware open path supplies an explicit [Endpoint]
     * via the two-argument constructor.
     *
     * @param config the connection configuration (host, port, credentials, timeouts,
     *               compression). Must not be `null`.
     * @throws ConnectionException if the socket cannot be opened or the handshake fails
     */
    public constructor(config: ClickHouseConfig?) :
        this(config, if (config == null) null else config.endpoints()[0])

    init {
        if (config == null) {
            throw ConnectionException("config must not be null")
        }
        if (endpoint == null) {
            throw ConnectionException("endpoint must not be null")
        }
        this.config = config
        this.compressionEnabled = config.compression() != CompressionMethod.NONE

        var s: Socket? = null
        try {
            val connectMillis = toMillis(config.connectTimeout())
            if (config.tls()) {
                s = openTlsSocket(config, endpoint, connectMillis)
            } else {
                s = Socket()
                s.connect(InetSocketAddress(endpoint.host, endpoint.port), connectMillis)
            }
            s.soTimeout = toMillis(config.socketTimeout())
            s.tcpNoDelay = true

            this.socket = s
            this.reader = DefaultBinaryReader(s.getInputStream())
            this.writer = DefaultBinaryWriter(s.getOutputStream())

            Handshake.sendHello(writer, config)
            writer.flush()
            this.hello = Handshake.readHello(reader)

            // Handshake addendum: send the quota_key string the server expects
            // (via receiveAddendum) before the first query. Omitting it desyncs
            // the next packet and surfaces as "[62] Empty query".
            if (Math.min(DBMS_TCP_PROTOCOL_VERSION.toLong(), hello.protocolRevision)
                >= MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY
            ) {
                writer.writeString("") // empty quota_key
                writer.flush()
            }
        } catch (e: ConnectionException) {
            closeQuietly(s)
            throw e
        } catch (e: IOException) {
            closeQuietly(s)
            throw ConnectionException(
                "Failed to connect to ClickHouse at " + endpoint, e
            )
        } catch (e: RuntimeException) {
            closeQuietly(s)
            throw ConnectionException(
                "Handshake with ClickHouse at " + endpoint + " failed", e
            )
        }

        this.timezone = resolveZone(hello.timezone)
        this.registry = DefaultCodecRegistry(timezone)
    }

    override fun serverHello(): ServerHello {
        return hello
    }

    override fun serverTimezone(): ZoneId {
        return timezone
    }

    /**
     * Effective protocol revision negotiated with the server: the minimum of the
     * client and server advertised revisions. Optional Query/ClientInfo fields are
     * gated on this value.
     */
    private fun effectiveRevision(): Int {
        return Math.min(DBMS_TCP_PROTOCOL_VERSION.toLong(), hello.protocolRevision).toInt()
    }

    override fun sendQuery(
        sql: String,
        settings: @JvmSuppressWildcards Map<String, String>?,
        params: QueryParameters?,
    ) {
        ensureOpen()
        // A new query resets any cancellation state carried over from a previous one.
        cancelled = false
        val revision = effectiveRevision()
        val effective = effectiveSettings(settings)
        val effectiveParams = params ?: QueryParameters.EMPTY
        if (!effectiveParams.isEmpty() && revision < MIN_PROTOCOL_VERSION_WITH_PARAMETERS) {
            throw ProtocolException(
                "Server protocol revision " + revision + " does not support query parameters"
                    + " (requires >= " + MIN_PROTOCOL_VERSION_WITH_PARAMETERS + ")"
            )
        }
        synchronized(writeLock) {
            try {
                writer.writeVarUInt(ClientPacket.QUERY.code.toLong())

                // query_id — empty lets the server generate one.
                writer.writeString("")

                // ClientInfo block (// VERIFY against CH.Native: field order + gating).
                if (revision >= MIN_REVISION_WITH_CLIENT_INFO) {
                    writeClientInfo(revision)
                }

                // Per-query settings, terminated by an empty setting name.
                // VERIFY against CH.Native: settings are (String name, ...) pairs ended by "".
                writeSettings(writer, effective, revision)

                // Inter-server secret (empty: we are a plain client).
                if (revision >= MIN_REVISION_WITH_INTERSERVER_SECRET) {
                    writer.writeString("")
                }

                // Query processing stage.
                writer.writeVarUInt(QUERY_PROCESSING_STAGE_COMPLETE.toLong())

                // Compression flag: tells the server whether to compress its responses.
                // VERIFY against CH.Native: 1 = enabled, 0 = disabled.
                writer.writeVarUInt(if (compressionEnabled) 1L else 0L)

                // The SQL text.
                writer.writeString(sql)

                // Trailing query parameters: each is a custom setting (name, flags, string
                // value) in the STRINGS_WITH_FLAGS settings format, terminated by an empty
                // name. The server binds these into the {name:Type} placeholders in the SQL.
                // VERIFY against CH.Native: parameters list uses the custom-setting form, ended by "".
                if (revision >= MIN_PROTOCOL_VERSION_WITH_PARAMETERS) {
                    writeQueryParameters(writer, effectiveParams)
                }

                writer.flush()

                // The client must follow a Query with an empty data block (no inline INSERT data).
                sendBlock(Block())
            } catch (e: IOException) {
                poisoned = true
                throw ConnectionException("Failed to send query", e)
            }
        }
    }

    /**
     * Writes the `ClientInfo` sub-structure of a Query packet.
     *
     * @param revision the effective negotiated protocol revision used to gate fields
     */
    @Throws(IOException::class)
    private fun writeClientInfo(revision: Int) {
        // query_kind: 1 = initial query (an empty value would mean "no query").
        writer.writeUInt8(QUERY_KIND_INITIAL_QUERY)

        writer.writeString("")          // initial_user
        writer.writeString("")          // initial_query_id
        writer.writeString("0.0.0.0:0") // initial_address

        // initial_query_start_time_microseconds.
        if (revision >= MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME) {
            writer.writeInt64(0L)
        }

        writer.writeUInt8(CLIENT_INTERFACE_TCP) // interface = TCP

        writer.writeString("")                    // os_user
        writer.writeString("")                    // client_hostname
        writer.writeString(Handshake.CLIENT_NAME) // client_name
        writer.writeVarUInt(Handshake.CLIENT_VERSION_MAJOR.toLong())
        writer.writeVarUInt(Handshake.CLIENT_VERSION_MINOR.toLong())
        writer.writeVarUInt(DBMS_TCP_PROTOCOL_VERSION.toLong()) // client tcp protocol version

        // quota_key
        if (revision >= MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO) {
            writer.writeString("")
        }

        // distributed_depth
        if (revision >= MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH) {
            writer.writeVarUInt(0L)
        }

        // client_version_patch
        if (revision >= MIN_REVISION_WITH_VERSION_PATCH) {
            writer.writeVarUInt(0L)
        }

        // OpenTelemetry trace context: 0 = none present.
        if (revision >= MIN_REVISION_WITH_OPENTELEMETRY) {
            writer.writeUInt8(0)
        }

        // Parallel-replicas "collaborate with initiator" + counts: all zero for a plain client.
        if (revision >= MIN_REVISION_WITH_PARALLEL_REPLICAS) {
            writer.writeVarUInt(0L) // collaborate_with_initiator
            writer.writeVarUInt(0L) // count_participating_replicas
            writer.writeVarUInt(0L) // number_of_current_replica
        }
    }

    /**
     * Merges the effective connection-default settings (the flattened-serialization
     * setting plus the user's configured [ClickHouseConfig.settings]) with the
     * supplied per-query [overrides], preserving insertion order. Precedence,
     * lowest to highest: the flattened-serialization default, the configured connection
     * settings, then the per-query [overrides] (each later layer wins key-by-key).
     *
     * @param overrides per-query settings (may be `null`/empty)
     * @return the effective, insertion-ordered settings map (never null, never empty)
     */
    private fun effectiveSettings(overrides: Map<String, String>?): Map<String, String> {
        val merged = mergeSettings(config.settings(), overrides) as LinkedHashMap<String, String>
        // Server-side enforcement of the configured queryTimeout: travel as
        // max_execution_time unless the caller (or connection settings) already set it.
        val timeout = config.queryTimeout()
        if (timeout != null && !timeout.isZero && !timeout.isNegative
            && !merged.containsKey("max_execution_time")
        ) {
            // Round UP to whole seconds — max_execution_time is integral seconds and a
            // sub-second timeout must not become "no timeout".
            val seconds = (timeout.toMillis() + 999) / 1000
            merged["max_execution_time"] = seconds.toString()
        }
        return merged
    }

    override fun ping(): Boolean {
        if (closed || poisoned) {
            return false
        }
        return try {
            synchronized(writeLock) {
                writer.writeVarUInt(ClientPacket.PING.code.toLong())
                writer.flush()
            }
            // The server may interleave Progress packets before the Pong; skip them.
            while (true) {
                val msg = readMessage()
                when (msg.type()) {
                    ServerPacket.PONG -> return true
                    ServerPacket.PROGRESS, ServerPacket.LOG, ServerPacket.PROFILE_EVENTS -> {
                        // keep draining
                    }
                    else -> return false
                }
            }
            @Suppress("UNREACHABLE_CODE")
            false
        } catch (e: Exception) {
            false
        }
    }

    override fun sendData(block: Block) {
        ensureOpen()
        synchronized(writeLock) {
            try {
                sendBlock(block)
            } catch (e: IOException) {
                poisoned = true
                throw ConnectionException("Failed to send data block", e)
            }
        }
    }

    override fun sendEmptyData() {
        ensureOpen()
        synchronized(writeLock) {
            try {
                sendBlock(Block())
            } catch (e: IOException) {
                poisoned = true
                throw ConnectionException("Failed to send terminating empty data block", e)
            }
        }
    }

    override fun cancel() {
        // Cross-thread entry point. Deliberately does NOT call ensureOpen() the way the
        // send path does: a cancel racing with close() should quietly no-op, not throw.
        if (closed) {
            return
        }
        // The Cancel packet is a single VarUInt packet code with no body.
        // VERIFY against CH.Native: Cancel is sent as just ClientPacket.CANCEL with no payload.
        synchronized(writeLock) {
            if (closed) {
                return
            }
            cancelled = true
            try {
                writer.writeVarUInt(ClientPacket.CANCEL.code.toLong())
                writer.flush()
            } catch (e: IOException) {
                // The socket is broken; the in-flight read will fail too. Poison so the
                // connection is discarded rather than reused with an unknown wire offset.
                poisoned = true
                throw ConnectionException("Failed to send Cancel packet", e)
            }
        }
    }

    /** Whether a `Cancel` packet has been written for the current query. */
    public fun isCancelled(): Boolean {
        return cancelled
    }

    /**
     * Writes a `Data` packet: packet code, the (unused, uncompressed) table name,
     * and the block — compressed per config. The table-name string is never compressed;
     * only the block bytes are.
     *
     * @param block the block to send (an empty block terminates an insert / follows a query)
     */
    @Throws(IOException::class)
    private fun sendBlock(block: Block) {
        val revision = effectiveRevision()

        // Serialize the block into the reusable staging buffer BEFORE touching the socket
        // writer: a codec failure during serialization (e.g. a value the column codec
        // rejects) must leave the wire byte-clean — no stray Data packet header — so the
        // connection stays healthy and reusable. For the compressed path this is a pure
        // reorder of the previous code (the block was always staged for compression); the
        // uncompressed path gains one in-memory copy, negligible next to the socket write.
        // Reuse one staging buffer + writer + compressor per connection across blocks.
        // Safe because the single-operation guard means only one block is ever being
        // written at a time on this connection; these are instance fields, never shared
        // across connections.
        var staging = compressStaging
        if (staging == null) {
            staging = ResettableByteArrayOutputStream(COMPRESS_STAGING_INITIAL_CAPACITY)
            compressStaging = staging
            compressStagingWriter = DefaultBinaryWriter(staging)
        }
        staging.reset()
        val stagingWriter = compressStagingWriter!!
        try {
            BlockCodec.write(stagingWriter, block, revision)
            stagingWriter.flush()
        } catch (t: Throwable) {
            // The staging WRITER buffers internally, so a failed serialization can leave
            // partial bytes in its buffer that would corrupt the next block. Discard the
            // staging pair; the next sendBlock rebuilds it fresh. The socket writer was
            // never touched, so the wire stays byte-clean.
            compressStaging = null
            compressStagingWriter = null
            throw t
        }

        // The block is fully serialized; only now emit wire bytes.
        writer.writeVarUInt(ClientPacket.DATA.code.toLong())
        // Table name — empty for ordinary inserts / query data. NOT compressed.
        // VERIFY against CH.Native: temporary-table name precedes the block, default "".
        writer.writeString("")
        if (compressionEnabled) {
            if (compressor == null) {
                compressor = Compressors.compressor(config.compression())
            }
            // Compress directly from the staging buffer's backing array + length —
            // no toByteArray() copy.
            CompressedBlockCodec.write(
                writer,
                staging.buffer(), 0, staging.length(), compressor!!
            )
        } else {
            writer.writeBytes(staging.buffer(), 0, staging.length())
        }
        writer.flush()
    }

    override fun readMessage(): ServerMessage {
        ensureOpen()
        try {
            val code = reader.readVarUInt().toInt()
            val type = ServerPacket.fromCode(code)
                ?: throw ProtocolException("Unknown server packet code: " + code)
            when (type) {
                ServerPacket.DATA, ServerPacket.TOTALS, ServerPacket.EXTREMES ->
                    return ServerMessage.block(type, readDataBlock())
                ServerPacket.EXCEPTION -> {
                    val ex = ServerPacketReader.readException(reader)
                    return ServerMessage.exception(ex)
                }
                ServerPacket.PROGRESS -> {
                    val progress = ServerPacketReader.readProgress(reader, effectiveRevision())
                    return ServerMessage.progress(progress)
                }
                ServerPacket.PROFILE_INFO -> {
                    val profileInfo = ServerPacketReader.readProfileInfo(reader)
                    return ServerMessage.profileInfo(profileInfo)
                }
                ServerPacket.LOG, ServerPacket.PROFILE_EVENTS -> {
                    // Block-bearing telemetry packets (query log / profile events). These
                    // are always sent UNCOMPRESSED, even when result blocks are compressed.
                    // Consume the block to stay in sync; report as benign so callers skip it.
                    readBlockBody(false)
                    return ServerMessage.of(type)
                }
                ServerPacket.TABLE_COLUMNS -> {
                    // External-table columns metadata: external table name + columns
                    // description, both strings. Consumed and reported as benign so
                    // callers can skip it while staying in sync.
                    reader.readString()
                    reader.readString()
                    return ServerMessage.of(type)
                }
                ServerPacket.END_OF_STREAM, ServerPacket.PONG ->
                    return ServerMessage.of(type)
                else ->
                    throw ProtocolException(
                        "Unsupported server packet type: " + type + " (code " + code + ")"
                    )
            }
        } catch (e: ServerException) {
            // A server query exception is reported via the EXCEPTION packet (returned above),
            // not thrown here; this defensive branch rethrows WITHOUT poisoning because the
            // wire stays in spec after a clean server-side exception (the connection is reusable).
            throw e
        } catch (e: RuntimeException) {
            // ProtocolException (unknown packet, bad framing) or any codec-level decode error:
            // the read consumed an unknown number of bytes, so the stream offset is now unknown.
            poisoned = true
            throw e
        } catch (e: IOException) {
            // Socket-level failure: the connection is broken.
            poisoned = true
            throw ConnectionException("Failed to read server message", e)
        }
    }

    override fun isPoisoned(): Boolean {
        return poisoned
    }

    override fun markPoisoned() {
        poisoned = true
    }

    /**
     * Reads the body of a block-bearing packet (the packet code is already consumed):
     * the table-name string followed by the block, decompressing the block frame when
     * the connection uses compression.
     *
     * @return the decoded block
     */
    @Throws(IOException::class)
    private fun readDataBlock(): Block {
        return readBlockBody(compressionEnabled)
    }

    /**
     * Reads a block-bearing packet body (the packet code is already consumed): the
     * table-name string followed by the block. Result blocks honor the connection's
     * compression setting; telemetry blocks (LOG / PROFILE_EVENTS) are always
     * uncompressed and pass `compressed = false`.
     */
    @Throws(IOException::class)
    private fun readBlockBody(compressed: Boolean): Block {
        // Table name — discarded; never compressed.
        reader.readString()
        val revision = effectiveRevision()
        if (compressed) {
            // A single native block is the concatenation of one OR MORE consecutive
            // compressed frames (the server splits a block once its uncompressed
            // serialization exceeds the per-frame limit, ~1 MiB). Decode the block
            // through a stream that transparently advances to the next frame when the
            // current decompressed buffer is exhausted, reassembling multi-frame
            // blocks. The stream serves bytes only from the current frame and never
            // reads ahead, so it stops exactly at this block's last frame and leaves
            // `reader` positioned at the next packet code.
            //
            // The frame stream and its (UNBUFFERED) reader are reused across every block
            // of this connection rather than reallocated per block. Safety: the client
            // serializes operations (one outstanding read at a time), these are instance
            // fields never shared across connections, and the reader is unbuffered so no
            // stale bytes can survive the frame-cursor reset between blocks. We rewind the
            // frame stream BEFORE reading so it re-reads the NEXT block's frames from `reader`
            // (which the previous block left positioned at the next frame).
            var frames = compressedFrames
            if (frames == null) {
                frames = CompressedFrameInputStream(reader)
                compressedFrames = frames
                compressedReader = DefaultBinaryReader.unbuffered(frames)
            }
            frames.resetFrames()
            return BlockCodec.read(compressedReader!!, registry, revision)
        }
        return BlockCodec.read(reader, registry, revision)
    }

    override fun close() {
        if (closed) {
            return
        }
        closed = true
        closeQuietly(socket)
    }

    private fun ensureOpen() {
        if (closed) {
            throw ConnectionException("Connection is closed")
        }
    }

    public companion object {

        // --- protocol revision constants -------------------------------------
        // The client advertises Handshake.CLIENT_PROTOCOL_REVISION in the Hello; the
        // server replies with its own revision. The effective revision used to gate
        // optional Query/ClientInfo fields is min(client, server).

        /** DBMS TCP protocol revision the client speaks. */
        // VERIFY against CH.Native: DBMS_TCP_PROTOCOL_VERSION (kept in sync with Handshake).
        public const val DBMS_TCP_PROTOCOL_VERSION: Int = Handshake.CLIENT_PROTOCOL_REVISION

        /** Minimum revision that carries a `ClientInfo` block in the Query packet. */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_CLIENT_INFO.
        private const val MIN_REVISION_WITH_CLIENT_INFO: Int = 54_032

        /** Minimum revision that carries a quota key inside `ClientInfo`. */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO.
        private const val MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: Int = 54_060

        /** Minimum revision that carries the server version patch (also used in ClientInfo). */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_VERSION_PATCH.
        private const val MIN_REVISION_WITH_VERSION_PATCH: Int = 54_401

        /** Minimum revision that carries an inter-server secret on the Query packet. */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET.
        private const val MIN_REVISION_WITH_INTERSERVER_SECRET: Int = 54_441

        /** Minimum revision that carries an OpenTelemetry trace context in `ClientInfo`. */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_OPENTELEMETRY.
        private const val MIN_REVISION_WITH_OPENTELEMETRY: Int = 54_442

        /** Minimum revision that carries the distributed-query "collaborate with initiator" fields. */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS.
        private const val MIN_REVISION_WITH_PARALLEL_REPLICAS: Int = 54_453

        /** Minimum revision that carries the `initial_query_start_time` in `ClientInfo`. */
        // VERIFY against CH.Native: DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME.
        private const val MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME: Int = 54_449

        /** Minimum revision that carries the distributed-query depth in `ClientInfo`. */
        // VERIFY against CH.Native: DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH.
        private const val MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: Int = 54_448

        /** Minimum revision that carries a trailing query-parameters list on the Query packet. */
        // VERIFY against CH.Native: DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS.
        private const val MIN_PROTOCOL_VERSION_WITH_PARAMETERS: Int = 54_459

        /**
         * Minimum revision at which per-query settings are serialized as
         * *strings-with-flags* rather than the legacy binary-typed form. Modern
         * ClickHouse (25.x) is well past this, so this client always uses the
         * strings-with-flags encoding when it sends any settings.
         */
        // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS.
        private const val MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: Int = 54_429

        /**
         * Per-setting flag byte for the "custom" bit used when serializing query parameters
         * in the `STRINGS_WITH_FLAGS` settings format. Query parameters are custom
         * settings, so each carries this flag between its name and its (string) value.
         */
        // VERIFY against CH.Native: BaseSettingsHelpers::Flags { IMPORTANT = 0x01, CUSTOM = 0x02 };
        // query parameters are serialized as custom settings, so the flag is CUSTOM (0x02).
        private const val SETTING_FLAG_CUSTOM: Int = 0x02

        /**
         * Minimum revision at which the client must send a handshake *addendum*
         * (the `quota_key` string) immediately after the Hello exchange. The
         * server's `receiveAddendum()` reads it before the first query, so
         * omitting it desyncs the very next packet.
         */
        // VERIFY against CH.Native: DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY.
        private const val MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY: Int = 54_458

        // --- ClientInfo enum values ------------------------------------------

        /** `ClientInfo.QueryKind.INITIAL_QUERY`. */
        // VERIFY against CH.Native: ClientInfo query-kind enum.
        private const val QUERY_KIND_INITIAL_QUERY: Int = 1

        /** `ClientInfo.Interface.TCP`. */
        // VERIFY against CH.Native: ClientInfo interface enum.
        private const val CLIENT_INTERFACE_TCP: Int = 1

        /** `QueryProcessingStage.Complete`. */
        // VERIFY against CH.Native: query_processing_stage Complete.
        private const val QUERY_PROCESSING_STAGE_COMPLETE: Int = 2

        /** Initial capacity for the reused compression staging buffer. */
        private const val COMPRESS_STAGING_INITIAL_CAPACITY: Int = 64 * 1024

        /**
         * The native serialization setting the self-framing `Variant` / `Dynamic`
         * / `JSON` codecs require. Sent as a connection-default setting on every query
         * (so it survives without a separate `SET` round-trip) rather than as a one-off
         * statement; callers can still override it via connection or per-query settings.
         */
        public const val FLATTENED_SERIALIZATION_SETTING: String =
            "output_format_native_use_flattened_dynamic_and_json_serialization"

        /**
         * Serializes the query-parameters slot of a Query packet using ClickHouse's
         * `STRINGS_WITH_FLAGS` settings format: for each parameter, the bare name,
         * then the per-setting flags byte ([SETTING_FLAG_CUSTOM]), then the textual
         * value as a binary string; the list is terminated by an empty name string.
         *
         * Package-private and static in the Java original so it can be unit-tested by
         * capturing a [io.github.danielbunting.clickhouse.protocol.BinaryWriter] over a
         * byte buffer without opening a socket.
         *
         * @param w      the destination writer
         * @param params the bindings (an empty set emits only the terminator)
         */
        // VERIFY against CH.Native: query-parameters serialization == Settings::write with
        // SettingsWriteFormat::STRINGS_WITH_FLAGS — (name, flags=CUSTOM, value) per entry, "" terminator.
        @JvmStatic
        @Throws(IOException::class)
        public fun writeQueryParameters(w: BinaryWriter, params: QueryParameters?) {
            if (params != null && !params.isEmpty()) {
                for (e in params.asMap().entries) {
                    w.writeString(e.key)                        // parameter name (bare, no "param_" prefix)
                    w.writeVarUInt(SETTING_FLAG_CUSTOM.toLong()) // custom-setting flag
                    // A custom setting's value is a ClickHouse Field DUMP, not raw text — the server
                    // calls Field::restoreFromDump on it. Query-parameter values are bound as String
                    // fields, so dump them as a single-quoted, escaped literal (e.g. 42 -> '42'); a
                    // NULL binding dumps as the bare Field token NULL. Verified end-to-end on a live
                    // 25.8 server (MergedFeaturesIT) — error [536] "Couldn't restore Field from dump"
                    // is what an un-dumped value produces.
                    val value = params.wireValue(e.key)
                    w.writeString(dumpFieldValue(value)) // Field dump of the textual value
                }
            }
            w.writeString("") // empty name terminates the parameter list
        }

        /**
         * Renders a query-parameter wire value as a ClickHouse Field dump: the `\N` NULL
         * sentinel becomes the bare token `NULL`; any other value becomes a single-quoted
         * String-Field literal with `\\` and `'` escaped (FieldVisitorDump form).
         */
        private fun dumpFieldValue(value: String?): String {
            if ("\\N" == value) {
                return "NULL"
            }
            return "'" + value!!.replace("\\", "\\\\").replace("'", "\\'") + "'"
        }

        /**
         * Merges, in order of increasing precedence: the flattened-serialization default,
         * the connection-default [connectionSettings], then the per-query
         * [overrides] (each later layer wins key-by-key). Insertion order is preserved.
         * Package-private and static in the Java original so the merge precedence can be
         * unit-tested directly.
         *
         * @param connectionSettings connection-default settings (may be `null`/empty)
         * @param overrides          per-query settings (may be `null`/empty)
         * @return the effective, insertion-ordered settings map (never null, never empty)
         */
        @JvmStatic
        public fun mergeSettings(
            connectionSettings: Map<String, String>?,
            overrides: Map<String, String>?,
        ): Map<String, String> {
            val merged = LinkedHashMap<String, String>()
            // Opportunistic default first so callers can override it; harmless on servers
            // that recognise the setting, and modern (25.x) servers always do.
            merged[FLATTENED_SERIALIZATION_SETTING] = "1"
            if (connectionSettings != null) {
                merged.putAll(connectionSettings)
            }
            if (overrides != null) {
                merged.putAll(overrides)
            }
            return merged
        }

        /**
         * Serializes the [settings] into the Query-packet settings slot using the
         * ClickHouse *settings-as-strings-with-flags* form, terminated by an
         * empty setting name. For each entry this writes:
         *
         *  1. the setting name (length-prefixed string),
         *  2. a flags `VarUInt` — `0` for a normal user-provided setting
         *     (the `IMPORTANT`/`CUSTOM`/`OBSOLETE` bits are all clear),
         *  3. the setting value (length-prefixed string).
         *
         * The list is terminated by an empty name. A `null`/empty map writes just
         * the terminator, matching the previous "send none" behavior.
         *
         * On protocol revisions older than
         * [MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS] the legacy binary-typed
         * settings form would be required; this client targets CH 25.x (well past that
         * revision) and only emits the strings-with-flags form, so it refuses to send
         * settings against an older server rather than corrupt the wire.
         *
         * Package-private and static in the Java original so it can be unit-tested by
         * capturing the bytes a [BinaryWriter] produces.
         *
         * @param out      the binary writer
         * @param settings the effective settings (may be `null`/empty)
         * @param revision the negotiated protocol revision
         */
        @JvmStatic
        @Throws(IOException::class)
        public fun writeSettings(out: BinaryWriter, settings: Map<String, String?>?, revision: Int) {
            if (settings != null && !settings.isEmpty()
                && revision < MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
            ) {
                throw ProtocolException(
                    "Server protocol revision " + revision + " predates settings-as-strings ("
                        + MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
                        + "); cannot send per-query/connection settings"
                )
            }
            if (settings != null) {
                for (e in settings.entries) {
                    out.writeString(e.key)
                    // Flags: 0 = a normal, writable, non-custom, non-obsolete setting.
                    // VERIFY against CH.Native: BaseSettingsHelpers::writeFlags writes the
                    // SettingFlags (IMPORTANT=0x01, CUSTOM=0x02, OBSOLETE=0x04) as a VarUInt.
                    out.writeVarUInt(0L)
                    val value = e.value
                    out.writeString(value ?: "")
                }
            }
            // Empty name terminates the settings list.
            out.writeString("")
        }

        /**
         * Opens a TLS socket to `endpoint.host:endpoint.port` (the failover-selected
         * node), reusing all other TLS settings from [config].
         *
         * A plain socket is connected first (so `connectTimeout` applies to the
         * TCP connect), then wrapped in an [SSLSocket] layered over it. SNI and
         * endpoint identification (the `"HTTPS"` algorithm, i.e. RFC 2818 hostname
         * verification) are enabled unless `verifyHostname` is disabled or
         * `insecureSkipVerify` is set. The TLS handshake is performed eagerly so
         * any certificate/handshake failure surfaces here as a [ConnectionException]
         * rather than mid-protocol. The native protocol then proceeds unchanged over the
         * TLS streams.
         */
        @Throws(IOException::class)
        private fun openTlsSocket(config: ClickHouseConfig, endpoint: Endpoint, connectMillis: Int): Socket {
            val factory = TlsSockets.buildSocketFactory(config)

            val plain = Socket()
            val ssl: SSLSocket
            try {
                plain.connect(InetSocketAddress(endpoint.host, endpoint.port), connectMillis)
                // Layer TLS over the connected plain socket; autoClose=true so closing the
                // SSLSocket also closes the underlying plain socket.
                ssl = factory.createSocket(plain, endpoint.host, endpoint.port, true) as SSLSocket
            } catch (e: IOException) {
                closeQuietly(plain)
                throw e
            }

            try {
                val params = ssl.sslParameters
                // SNI: advertise the connect host so the server can select the right cert.
                params.serverNames = java.util.List.of<SNIServerName>(SNIHostName(endpoint.host))
                if (config.verifyHostname() && !config.insecureSkipVerify()) {
                    // RFC 2818 hostname verification against the presented certificate.
                    params.endpointIdentificationAlgorithm = "HTTPS"
                } else {
                    params.endpointIdentificationAlgorithm = null
                }
                ssl.sslParameters = params

                // Eager handshake: fail fast on cert/trust problems.
                ssl.startHandshake()
            } catch (e: IOException) {
                closeQuietly(ssl)
                throw e
            }
            return ssl
        }

        // --- helpers ---------------------------------------------------------

        /**
         * Resolves the server-supplied timezone string into a [ZoneId], falling back
         * to [UTC][ZoneId.of] when it is blank or unrecognised.
         *
         * @param timezone the timezone string from the server Hello (may be empty)
         * @return the resolved zone, or UTC
         */
        @JvmStatic
        public fun resolveZone(timezone: String?): ZoneId {
            if (timezone == null || timezone.isEmpty()) {
                return ZoneId.of("UTC")
            }
            return try {
                ZoneId.of(timezone)
            } catch (e: java.time.DateTimeException) {
                ZoneId.of("UTC")
            }
        }

        /**
         * Converts a (possibly `null`) [java.time.Duration] to a non-negative
         * millisecond value clamped to `int`, where `0` means "no timeout".
         *
         * @param duration the duration, or `null`
         * @return milliseconds in `[0, Integer.MAX_VALUE]`
         */
        @JvmStatic
        public fun toMillis(duration: java.time.Duration?): Int {
            if (duration == null || duration.isZero || duration.isNegative) {
                return 0
            }
            val millis = duration.toMillis()
            if (millis > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE
            }
            return millis.toInt()
        }

        private fun closeQuietly(s: Socket?) {
            if (s != null) {
                try {
                    s.close()
                } catch (ignored: IOException) {
                    // Best effort: nothing actionable on close failure.
                }
            }
        }
    }
}
