package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.protocol.ServerHello
import java.time.ZoneId

/**
 * Internal transport over a single ClickHouse native-TCP socket. Owns the
 * [io.github.danielbunting.clickhouse.protocol.BinaryReader]/`BinaryWriter`,
 * the handshake, packet framing, and per-block (de)compression. Higher layers
 * (`ClickHouseConnectionImpl`, `BulkInserterImpl`) drive it and never
 * touch the socket directly.
 *
 * **Wave 2.0 frozen internal contract.** Implementation: `NativeClientImpl`
 * (task W2-A). Construction connects and completes the handshake.
 */
public interface NativeClient : AutoCloseable {

    /** Server identity/capabilities from the handshake. */
    public fun serverHello(): ServerHello

    /** Server timezone (from the Hello), used to decode `DateTime`; defaults to UTC if absent. */
    public fun serverTimezone(): ZoneId

    /**
     * Sends a `Query` packet (client info, settings, requested compression,
     * query stage, query string) followed by the empty client data block that
     * tells the server there is no inline INSERT data on this query.
     *
     * Equivalent to [sendQuery] with no per-query settings or parameters (the
     * connection's configured default settings still apply).
     */
    public fun sendQuery(sql: String) {
        sendQuery(sql, null, null)
    }

    /**
     * As [sendQuery] with per-query [settings] but no query parameters.
     */
    public fun sendQuery(sql: String, settings: @JvmSuppressWildcards Map<String, String>?) {
        sendQuery(sql, settings, null)
    }

    /**
     * As [sendQuery] with server-side [params] but no per-query settings.
     */
    public fun sendQuery(sql: String, params: QueryParameters?) {
        sendQuery(sql, null, params)
    }

    /**
     * Sends a `Query` packet, serializing both per-query server [settings] (the
     * connection's defaults overlaid with these, per-query winning key-by-key) into the
     * settings slot, and [params] into the trailing query-parameters slot so the
     * *server* binds the `{name:Type}` placeholders in [sql]. Either may
     * be `null`/empty. Settings use the settings-as-strings-with-flags form on modern
     * protocol revisions; non-empty [params] require a parameters-capable revision.
     *
     * @param sql      the SQL, referencing parameters as `{name:Type}`
     * @param settings per-query server settings (may be `null`/empty)
     * @param params   server-side parameter bindings (may be `null`/empty)
     */
    public fun sendQuery(
        sql: String,
        settings: @JvmSuppressWildcards Map<String, String>?,
        params: QueryParameters?,
    )

    /** Sends a `Data` packet carrying [block] (insert path), compressed per config. */
    public fun sendData(block: Block)

    /**
     * Protocol-level liveness probe: sends a `Ping` packet and reports whether a
     * `Pong` came back. Never throws — any failure (closed, poisoned, I/O, protocol)
     * reports `false`.
     */
    public fun ping(): Boolean {
        return false
    }

    /** Sends the terminating empty `Data` block that ends an insert stream. */
    public fun sendEmptyData()

    /**
     * Reads and decodes the next server packet, blocking until one arrives. Data
     * blocks are decompressed per config and parsed via `BlockCodec`.
     */
    public fun readMessage(): ServerMessage

    /**
     * Writes a `Cancel` client packet to the socket's output stream, asking the
     * server to stop executing the in-flight query.
     *
     * **Cross-thread by design.** Cancellation almost always originates on a
     * thread *other* than the one blocked in [readMessage] on the same
     * socket. The native protocol supports this: `Cancel` is written to the
     * OutputStream while the owning thread keeps reading the InputStream (separate
     * half-duplex streams). This is the *one* operation a non-owning thread may
     * invoke on a connection; it is internally serialized against the normal send path
     * (query/data writes) so two writers cannot interleave on the shared
     * `BufferedOutputStream`.
     *
     * This method only sends the request. The owning reader thread is responsible
     * for draining the remaining packets until `END_OF_STREAM`/exception, which
     * leaves the connection reusable. If draining cannot complete cleanly the caller
     * marks the connection poisoned ([markPoisoned]), reusing the existing
     * poison semantics.
     *
     * A best-effort no-op if the connection is already closed.
     */
    public fun cancel()

    /**
     * Whether this connection's wire is at an unknown offset or its socket is broken — set
     * after a protocol/I/O failure (or a mid-INSERT desync), but NOT after a clean server
     * query exception. A poisoned connection must be discarded, not reused/pooled.
     */
    public fun isPoisoned(): Boolean

    /**
     * Marks this connection poisoned. Called by higher layers when an operation leaves the
     * stream in an indeterminate state that `readMessage`/send cannot detect on their
     * own (e.g. a bulk INSERT abandoned or failed mid-stream).
     */
    public fun markPoisoned()

    override fun close()
}
