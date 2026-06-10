package io.github.danielbunting.clickhouse.protocol

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.ServerException
import java.io.IOException

/**
 * Decodes server → client packets from the native TCP protocol.
 *
 * The reader pulls the leading `VarUInt` packet code, resolves it via
 * [ServerPacket.fromCode], and decodes the body for the control
 * packets it understands: [ServerPacket.EXCEPTION], [ServerPacket.PROGRESS],
 * [ServerPacket.PROFILE_INFO], [ServerPacket.PONG] and
 * [ServerPacket.END_OF_STREAM].
 *
 * Packets whose body is a data block (e.g. [ServerPacket.DATA],
 * [ServerPacket.TOTALS], [ServerPacket.EXTREMES],
 * [ServerPacket.LOG]) are **not** decoded here: the reader only
 * reports the packet type so the higher-level dispatch loop (task W2) can route
 * the underlying stream to `BlockCodec` (task D3). For those, the
 * returned [Packet.body] is `null` and the underlying reader is
 * left positioned at the start of the block body.
 *
 * **Contract authority:** field orders verified against the CH.Native
 * .NET source; positions marked `// VERIFY` need a final cross-check.
 *
 * Task W1.D2.
 */
public class ServerPacketReader(private val input: BinaryReader) {

    /**
     * A decoded server packet: its [ServerPacket] type and, for the
     * control packets this reader understands, the decoded body.
     *
     * The `body` is:
     * - a [ServerException] for [ServerPacket.EXCEPTION],
     * - a [Progress] for [ServerPacket.PROGRESS],
     * - a [ProfileInfo] for [ServerPacket.PROFILE_INFO],
     * - `null` for [ServerPacket.PONG] and [ServerPacket.END_OF_STREAM] (no body), and
     * - `null` for block-bearing packets, whose body is left in the
     *   stream for `BlockCodec`.
     *
     * @property type the resolved packet type
     * @property body the decoded body, or `null` (see above)
     */
    @JvmRecord
    public data class Packet(val type: ServerPacket, val body: Any?)

    /**
     * Reads the next packet code and decodes the body for the recognised
     * control packets. For block-bearing or bodyless packets the returned
     * [Packet.body] is `null`.
     *
     * @return the next decoded packet
     * @throws ServerException if the packet is an [ServerPacket.EXCEPTION]
     *                         — the exception is decoded and thrown directly so
     *                         callers do not have to inspect the body
     * @throws ProtocolException if the packet code is not a known [ServerPacket]
     * @throws IOException     on an underlying I/O error
     */
    @Throws(IOException::class)
    public fun read(): Packet {
        val code = input.readVarUInt().toInt()
        val type = ServerPacket.fromCode(code)
            ?: throw ProtocolException("Unknown server packet code: $code")
        return when (type) {
            ServerPacket.EXCEPTION -> throw readException(input)
            ServerPacket.PROGRESS -> Packet(type, readProgress(input))
            ServerPacket.PROFILE_INFO -> Packet(type, readProfileInfo(input))
            ServerPacket.PONG, ServerPacket.END_OF_STREAM -> Packet(type, null)
            else ->
                // DATA / TOTALS / EXTREMES / LOG and other block- or
                // structure-bearing packets: leave the body in the stream.
                Packet(type, null)
        }
    }

    public companion object {

        /** Minimum revision carrying `total_rows_to_read` in PROGRESS. */
        private const val MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 54_058

        /** Minimum revision carrying `written_rows`/`written_bytes` in PROGRESS. */
        private const val MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54_372

        /** Minimum revision carrying `elapsed_ns` in PROGRESS. */
        private const val MIN_REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS = 54_453

        /**
         * Decodes the body of an [ServerPacket.EXCEPTION] packet (the packet
         * code must already have been consumed).
         *
         * Wire layout (// VERIFY against CH.Native):
         * `Int32 code`, `String name`, `String message`,
         * `String stackTrace`, `UInt8 hasNested`. Nested exceptions
         * (when `hasNested != 0`) are chained on the wire; this reader decodes
         * the leading exception and skips the chain.
         *
         * @param in the reader positioned at the start of the exception body
         * @return the decoded [ServerException]
         * @throws IOException on an underlying I/O error
         */
        @JvmStatic
        @Throws(IOException::class)
        public fun readException(`in`: BinaryReader): ServerException {
            val code = `in`.readInt32()
            val name = `in`.readString()
            val message = `in`.readString()
            val stackTrace = `in`.readString()
            var hasNested = `in`.readUInt8()
            // VERIFY against CH.Native: nested exceptions are appended recursively
            // when hasNested != 0; we decode only the leading exception and consume
            // the rest of the chain so the stream is left at the packet boundary.
            while (hasNested != 0) {
                `in`.readInt32() // nested code
                `in`.readString() // nested name
                `in`.readString() // nested message
                `in`.readString() // nested stack trace
                hasNested = `in`.readUInt8()
            }
            return ServerException(code, name, message, stackTrace)
        }

        /**
         * Decodes the body of a [ServerPacket.PROGRESS] packet (the packet
         * code must already have been consumed).
         *
         * Wire layout (// VERIFY against CH.Native): `VarUInt rows`,
         * `VarUInt bytes`, `VarUInt totalRows`, then on newer protocol
         * revisions `VarUInt writtenRows`, `VarUInt writtenBytes` and
         * `VarUInt elapsedNanos`. Because this reader is revision-agnostic it
         * reads only the three always-present counters; the optional counters are
         * reported as `0`. The dispatch loop (W2), which knows the negotiated
         * protocol revision, may read the trailing fields itself when present.
         *
         * @param in the reader positioned at the start of the progress body
         * @return the decoded [Progress]
         * @throws IOException on an underlying I/O error
         */
        @JvmStatic
        @Throws(IOException::class)
        public fun readProgress(`in`: BinaryReader): Progress {
            // Backwards-compatible default: read the full modern field set.
            return readProgress(`in`, Integer.MAX_VALUE)
        }

        /**
         * Revision-aware PROGRESS decode. The trailing counters are gated on the
         * negotiated protocol revision; reading too few desynchronises the stream.
         *
         * @param in       the reader positioned at the start of the progress body
         * @param revision the negotiated protocol revision
         * @return the decoded [Progress]
         * @throws IOException on an underlying I/O error
         */
        @JvmStatic
        @Throws(IOException::class)
        public fun readProgress(`in`: BinaryReader, revision: Int): Progress {
            val rows = `in`.readVarUInt()
            val bytes = `in`.readVarUInt()
            val totalRows =
                if (revision >= MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) `in`.readVarUInt() else 0L
            var writtenRows = 0L
            var writtenBytes = 0L
            if (revision >= MIN_REVISION_WITH_CLIENT_WRITE_INFO) {
                writtenRows = `in`.readVarUInt()
                writtenBytes = `in`.readVarUInt()
            }
            val elapsedNanos =
                if (revision >= MIN_REVISION_WITH_SERVER_QUERY_TIME_IN_PROGRESS) {
                    `in`.readVarUInt()
                } else {
                    0L
                }
            return Progress(rows, bytes, totalRows, writtenRows, writtenBytes, elapsedNanos)
        }

        /**
         * Decodes the body of a [ServerPacket.PROFILE_INFO] packet (the packet
         * code must already have been consumed).
         *
         * Wire layout (// VERIFY against CH.Native): `VarUInt rows`,
         * `VarUInt blocks`, `VarUInt bytes`, `UInt8 appliedLimit`,
         * `VarUInt rowsBeforeLimit`, `UInt8 calculatedRowsBeforeLimit`.
         *
         * @param in the reader positioned at the start of the profile-info body
         * @return the decoded [ProfileInfo]
         * @throws IOException on an underlying I/O error
         */
        @JvmStatic
        @Throws(IOException::class)
        public fun readProfileInfo(`in`: BinaryReader): ProfileInfo {
            val rows = `in`.readVarUInt()
            val blocks = `in`.readVarUInt()
            val bytes = `in`.readVarUInt()
            val appliedLimit = `in`.readUInt8() != 0
            val rowsBeforeLimit = `in`.readVarUInt()
            val calculatedRowsBeforeLimit = `in`.readUInt8() != 0
            return ProfileInfo(rows, blocks, bytes, appliedLimit, rowsBeforeLimit, calculatedRowsBeforeLimit)
        }
    }
}
