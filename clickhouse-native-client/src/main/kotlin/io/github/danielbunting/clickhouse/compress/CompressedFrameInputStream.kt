package io.github.danielbunting.clickhouse.compress

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import java.io.IOException
import java.io.InputStream

/**
 * Streaming view over a sequence of ClickHouse compressed frames that presents the
 * concatenated *decompressed* bytes as a plain [InputStream].
 *
 * ClickHouse compresses a single native data block into one **or more**
 * consecutive compressed frames: when a block's uncompressed serialization exceeds
 * the per-frame limit (~1 MiB) the server splits it into multiple frames, each with
 * its own `[CityHash128 checksum (16B)][method (1B)][compressed_size u32]
 * [uncompressed_size u32][payload]` header. The bytes of the block are the
 * concatenation of every frame's decompressed payload.
 *
 * This stream reads frames lazily, one at a time, from the underlying
 * [BinaryReader]. Each [read] call serves bytes only from the current
 * decompressed frame; when that frame is exhausted it transparently advances to the
 * *next* frame (reading its checksum + header + payload, verifying the
 * CityHash128 checksum, and decompressing it). Crucially it never reads a frame's
 * bytes until a consumer actually requests a byte beyond the current frame, so a
 * consumer that reads exactly one block's worth of bytes (e.g.
 * `BlockCodec.read`) stops precisely
 * at that block's last frame and leaves the underlying reader positioned at the start
 * of the next protocol packet — i.e. the stream is boundary-safe and must NOT be
 * wrapped in a buffer that reads ahead past what the consumer requests.
 *
 * Not thread-safe; intended to be created and drained by a single block read on a
 * single connection.
 */
public class CompressedFrameInputStream(source: BinaryReader) : InputStream() {

    private val input: BinaryReader = source

    /** Decompressed bytes of the current frame. */
    private var current: ByteArray = EMPTY
    private var pos: Int = 0
    private var limit: Int = 0

    @Throws(IOException::class)
    override fun read(): Int {
        if (pos >= limit && !advance()) {
            return -1
        }
        return current[pos++].toInt() and 0xFF
    }

    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        if (off < 0 || len < 0 || off + len > b.size) {
            throw IndexOutOfBoundsException("off=$off, len=$len, b.length=${b.size}")
        }
        if (len == 0) {
            return 0
        }
        if (pos >= limit && !advance()) {
            return -1
        }
        // Serve only from the current frame so a consumer reading exactly one block's
        // worth of bytes never forces the next frame to be loaded prematurely.
        val n = minOf(len, limit - pos)
        System.arraycopy(current, pos, b, off, n)
        pos += n
        return n
    }

    override fun available(): Int {
        return limit - pos
    }

    /**
     * Returns this stream to its initial, never-read state so it can be reused for the
     * NEXT block read against the SAME underlying [BinaryReader].
     *
     * Only the local frame cursor is cleared (`current`/`pos`/`limit`);
     * the underlying reader is unchanged — it is the connection's single, long-lived
     * socket reader, currently positioned at the start of the next block's first frame.
     * After this call the next [read]/`advance` re-reads frames from the reader
     * exactly as a freshly constructed stream would, so reuse + reset is observationally
     * identical to allocating a new stream per block.
     *
     * Safe only because this stream never reads ahead past a consumed block (see the
     * class KDoc): when a block read finishes, the reader sits exactly at the next
     * frame, so there are no stale wire bytes to discard here.
     *
     * Deliberately named `resetFrames` rather than overriding [InputStream.reset]:
     * the `InputStream` contract ties `reset()` to a prior `mark(int)` and
     * throws when unsupported, whereas this is an unconditional frame-cursor rewind with no
     * mark semantics. Keeping it a distinct method avoids surprising any generic
     * `InputStream` consumer.
     */
    public fun resetFrames() {
        current = EMPTY
        pos = 0
        limit = 0
    }

    /**
     * Loads the next compressed frame into [current], verifying its checksum
     * and decompressing its payload.
     *
     * @return `true` if a frame with at least one decompressed byte was loaded;
     *         `false` if the underlying reader is exhausted (clean EOF before a
     *         frame header)
     */
    @Throws(IOException::class)
    private fun advance(): Boolean {
        // A frame with uncompressed_size == 0 would otherwise leave limit == 0 and
        // loop; keep advancing until we have bytes or hit EOF.
        while (true) {
            val next = CompressedBlockCodec.readFrameOrNull(input) ?: return false
            current = next
            pos = 0
            limit = next.size
            if (limit > 0) {
                return true
            }
        }
    }

    private companion object {
        private val EMPTY = ByteArray(0)
    }
}
