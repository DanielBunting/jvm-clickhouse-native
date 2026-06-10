package io.github.danielbunting.clickhouse.protocol

import java.io.BufferedInputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

/**
 * Default [BinaryReader] implementation reading ClickHouse native-protocol
 * primitives from an [InputStream].
 *
 * The supplied stream is wrapped in a [BufferedInputStream] internally so
 * callers need not buffer it themselves, unless created via [unbuffered],
 * which reads directly from an already-in-memory source. All multi-byte integers are read in
 * little-endian order. `VarUInt` is unsigned LEB128; `VarInt` is
 * zig-zag-encoded LEB128. Strings are a `VarUInt` byte-length prefix followed
 * by that many UTF-8 bytes.
 *
 * This class is not thread-safe; a single reader instance is intended to be used
 * by a single connection at a time.
 */
public class DefaultBinaryReader private constructor(source: InputStream, buffered: Boolean) :
    BinaryReader {

    private val input: InputStream =
        if (!buffered) {
            source
        } else {
            if (source is BufferedInputStream) source else BufferedInputStream(source)
        }

    /** Reused little-endian view over `scratch`; lazily (re)allocated. */
    private var scratch: ByteArray = EMPTY_SCRATCH
    private var scratchBuf: ByteBuffer = EMPTY_SCRATCH_BUF

    /**
     * Creates a reader over the given stream.
     *
     * @param source the underlying byte source; wrapped in a buffered stream internally.
     *               Must not be `null`.
     */
    public constructor(source: InputStream) : this(source, true)

    @Throws(IOException::class)
    override fun readByteUnsigned(): Int {
        val b = input.read()
        if (b < 0) {
            throw EOFException("Unexpected end of stream while reading a byte")
        }
        return b
    }

    @Throws(IOException::class)
    override fun readByteOrEof(): Int {
        return input.read()
    }

    @Throws(IOException::class)
    override fun readInt8(): Byte {
        return readByteUnsigned().toByte()
    }

    @Throws(IOException::class)
    override fun readInt16(): Short {
        val b0 = readByteUnsigned()
        val b1 = readByteUnsigned()
        return (b0 or (b1 shl 8)).toShort()
    }

    @Throws(IOException::class)
    override fun readInt32(): Int {
        val b0 = readByteUnsigned()
        val b1 = readByteUnsigned()
        val b2 = readByteUnsigned()
        val b3 = readByteUnsigned()
        return b0 or (b1 shl 8) or (b2 shl 16) or (b3 shl 24)
    }

    @Throws(IOException::class)
    override fun readInt64(): Long {
        val lo = readInt32().toLong() and 0xFFFFFFFFL
        val hi = readInt32().toLong() and 0xFFFFFFFFL
        return lo or (hi shl 32)
    }

    @Throws(IOException::class)
    override fun readUInt8(): Int {
        return readByteUnsigned()
    }

    @Throws(IOException::class)
    override fun readUInt16(): Int {
        return readInt16().toInt() and 0xFFFF
    }

    @Throws(IOException::class)
    override fun readUInt32(): Long {
        return readInt32().toLong() and 0xFFFFFFFFL
    }

    @Throws(IOException::class)
    override fun readUInt64(): Long {
        return readInt64()
    }

    @Throws(IOException::class)
    override fun readFloat32(): Float {
        return java.lang.Float.intBitsToFloat(readInt32())
    }

    @Throws(IOException::class)
    override fun readFloat64(): Double {
        return java.lang.Double.longBitsToDouble(readInt64())
    }

    @Throws(IOException::class)
    override fun readVarUInt(): Long {
        var value = 0L
        var shift = 0
        for (i in 0 until MAX_VARINT_BYTES) {
            val b = readByteUnsigned()
            value = value or ((b and 0x7F).toLong() shl shift)
            if ((b and 0x80) == 0) {
                return value
            }
            shift += 7
        }
        throw IOException("VarUInt is too long (more than $MAX_VARINT_BYTES bytes)")
    }

    @Throws(IOException::class)
    override fun readVarInt(): Long {
        val raw = readVarUInt()
        // Zig-zag decode: (raw >>> 1) ^ -(raw & 1)
        return (raw ushr 1) xor -(raw and 1L)
    }

    @Throws(IOException::class)
    override fun readString(): String {
        val len = readVarUInt()
        if (len < 0 || len > Integer.MAX_VALUE) {
            throw IOException("String length out of range: " + java.lang.Long.toUnsignedString(len))
        }
        if (len == 0L) {
            return ""
        }
        val bytes = readBytes(len.toInt())
        return String(bytes, StandardCharsets.UTF_8)
    }

    @Throws(IOException::class)
    override fun readBytes(count: Int): ByteArray {
        if (count < 0) {
            throw IllegalArgumentException("count must be non-negative: $count")
        }
        val dest = ByteArray(count)
        readFully(dest, 0, count)
        return dest
    }

    @Throws(IOException::class)
    override fun readFully(dest: ByteArray, offset: Int, length: Int) {
        if (offset < 0 || length < 0 || offset + length > dest.size) {
            throw IndexOutOfBoundsException(
                "offset=$offset, length=$length, dest.length=${dest.size}"
            )
        }
        var read = 0
        while (read < length) {
            val n = input.read(dest, offset + read, length - read)
            if (n < 0) {
                throw EOFException("Unexpected end of stream: needed $length bytes, got $read")
            }
            read += n
        }
    }

    // ----- Bulk fixed-width reads (improvement 04) -----
    //
    // These copy a contiguous fixed-width run off the wire into a reused scratch
    // byte[] (one readFully per chunk) and reinterpret it via a LITTLE_ENDIAN
    // ByteBuffer view. The byte sequence consumed is identical to looping the
    // per-value reader; only the CPU path differs (no per-byte arithmetic).
    // ClickHouse is little-endian, so the buffer's order is fixed to LITTLE_ENDIAN.

    /**
     * Ensures `scratch`/`scratchBuf` can hold [bytes] and
     * returns the little-endian buffer positioned at 0 with limit [bytes].
     */
    private fun scratch(bytes: Int): ByteBuffer {
        if (scratch.size < bytes) {
            scratch = ByteArray(bytes)
            scratchBuf = ByteBuffer.wrap(scratch).order(ByteOrder.LITTLE_ENDIAN)
        }
        scratchBuf.clear()
        scratchBuf.limit(bytes)
        return scratchBuf
    }

    @Throws(IOException::class)
    override fun readInto(dest: LongArray, n: Int) {
        val step = chunkValues(java.lang.Long.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Long.BYTES
            val buf = scratch(bytes)
            readFully(scratch, 0, bytes)
            buf.asLongBuffer().get(dest, off, count)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun readInto(dest: IntArray, n: Int) {
        val step = chunkValues(Integer.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * Integer.BYTES
            val buf = scratch(bytes)
            readFully(scratch, 0, bytes)
            buf.asIntBuffer().get(dest, off, count)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun readInto(dest: ShortArray, n: Int) {
        val step = chunkValues(java.lang.Short.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Short.BYTES
            val buf = scratch(bytes)
            readFully(scratch, 0, bytes)
            buf.asShortBuffer().get(dest, off, count)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun readInto(dest: DoubleArray, n: Int) {
        val step = chunkValues(java.lang.Double.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Double.BYTES
            val buf = scratch(bytes)
            readFully(scratch, 0, bytes)
            buf.asDoubleBuffer().get(dest, off, count)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun readInto(dest: FloatArray, n: Int) {
        val step = chunkValues(java.lang.Float.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Float.BYTES
            val buf = scratch(bytes)
            readFully(scratch, 0, bytes)
            buf.asFloatBuffer().get(dest, off, count)
            off += step
        }
    }

    public companion object {

        /** Maximum number of bytes a 64-bit unsigned LEB128 value may occupy. */
        private const val MAX_VARINT_BYTES = 10

        /**
         * Upper bound (in bytes) of the reusable scratch buffer for bulk reads. Large
         * runs are read in chunks of this size so a single column cannot force an
         * unbounded allocation. Chosen as a power of two that comfortably holds many
         * values of every fixed width and aligns to all of them (8 | size).
         */
        private const val MAX_BULK_CHUNK_BYTES = 1 shl 16 // 64 KiB

        private val EMPTY_SCRATCH = ByteArray(0)
        private val EMPTY_SCRATCH_BUF: ByteBuffer =
            ByteBuffer.wrap(EMPTY_SCRATCH).order(ByteOrder.LITTLE_ENDIAN)

        /** Bytes per chunk for a run of `count` values, each [width] bytes. */
        private fun chunkValues(width: Int): Int {
            val v = MAX_BULK_CHUNK_BYTES / width
            return if (v < 1) 1 else v
        }

        /**
         * Creates a reader that reads *directly* from [source] with NO internal
         * [BufferedInputStream].
         *
         * Use this only when the source is already a fully in-memory byte array (e.g. the
         * decompressed bytes served by `CompressedFrameInputStream`): per-byte and
         * per-chunk reads are then plain array indexing with no syscalls, so an interposed
         * buffer would add only an extra copy and an 8 KiB allocation with no benefit.
         *
         * Crucially, an unbuffered reader holds NO leftover bytes between reads. That is
         * what makes the instance safe to reuse across boundary-sensitive sources that may be
         * `reset()` between logical units (e.g. one per native block): nothing can
         * survive a reset to corrupt the next unit. A buffered reader could not give this
         * guarantee. The socket reader, by contrast, MUST stay buffered (use the public
         * [DefaultBinaryReader] constructor).
         *
         * @param source the underlying byte source, read directly. Must not be `null`.
         * @return an unbuffered reader over [source]
         */
        @JvmStatic
        public fun unbuffered(source: InputStream): DefaultBinaryReader {
            return DefaultBinaryReader(source, false)
        }
    }
}
