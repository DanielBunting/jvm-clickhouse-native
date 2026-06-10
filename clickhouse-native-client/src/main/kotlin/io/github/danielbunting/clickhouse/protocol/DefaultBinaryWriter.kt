package io.github.danielbunting.clickhouse.protocol

import java.io.BufferedOutputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

/**
 * Default [BinaryWriter] implementation writing ClickHouse native-protocol
 * primitives to an [OutputStream].
 *
 * The supplied stream is wrapped in a [BufferedOutputStream] internally so
 * callers need not buffer it themselves. All multi-byte integers are emitted in
 * little-endian order. `VarUInt` is unsigned LEB128; `VarInt` is
 * zig-zag-encoded LEB128. Strings are a `VarUInt` byte-length prefix followed
 * by that many UTF-8 bytes.
 *
 * This class is not thread-safe. Callers must invoke [flush] to ensure
 * buffered bytes reach the underlying stream.
 */
public class DefaultBinaryWriter(sink: OutputStream) : BinaryWriter {

    private val out: OutputStream =
        if (sink is BufferedOutputStream) sink else BufferedOutputStream(sink)

    /** Reused little-endian view over `scratch`; lazily (re)allocated. */
    private var scratch: ByteArray = EMPTY_SCRATCH
    private var scratchBuf: ByteBuffer = EMPTY_SCRATCH_BUF

    @Throws(IOException::class)
    override fun writeInt8(v: Byte) {
        out.write(v.toInt() and 0xFF)
    }

    @Throws(IOException::class)
    override fun writeInt16(v: Short) {
        out.write(v.toInt() and 0xFF)
        out.write((v.toInt() ushr 8) and 0xFF)
    }

    @Throws(IOException::class)
    override fun writeInt32(v: Int) {
        out.write(v and 0xFF)
        out.write((v ushr 8) and 0xFF)
        out.write((v ushr 16) and 0xFF)
        out.write((v ushr 24) and 0xFF)
    }

    @Throws(IOException::class)
    override fun writeInt64(v: Long) {
        out.write((v and 0xFF).toInt())
        out.write(((v ushr 8) and 0xFF).toInt())
        out.write(((v ushr 16) and 0xFF).toInt())
        out.write(((v ushr 24) and 0xFF).toInt())
        out.write(((v ushr 32) and 0xFF).toInt())
        out.write(((v ushr 40) and 0xFF).toInt())
        out.write(((v ushr 48) and 0xFF).toInt())
        out.write(((v ushr 56) and 0xFF).toInt())
    }

    @Throws(IOException::class)
    override fun writeUInt8(v: Int) {
        out.write(v and 0xFF)
    }

    @Throws(IOException::class)
    override fun writeUInt16(v: Int) {
        writeInt16(v.toShort())
    }

    @Throws(IOException::class)
    override fun writeUInt32(v: Long) {
        writeInt32(v.toInt())
    }

    @Throws(IOException::class)
    override fun writeUInt64(v: Long) {
        writeInt64(v)
    }

    @Throws(IOException::class)
    override fun writeFloat32(v: Float) {
        writeInt32(java.lang.Float.floatToIntBits(v))
    }

    @Throws(IOException::class)
    override fun writeFloat64(v: Double) {
        writeInt64(java.lang.Double.doubleToLongBits(v))
    }

    @Throws(IOException::class)
    override fun writeVarUInt(v: Long) {
        // Unsigned LEB128; treat v as a 64-bit unsigned value.
        var value = v
        do {
            var b = (value and 0x7F).toInt()
            value = value ushr 7
            if (value != 0L) {
                b = b or 0x80
            }
            out.write(b)
        } while (value != 0L)
    }

    @Throws(IOException::class)
    override fun writeVarInt(v: Long) {
        // Zig-zag encode then emit as VarUInt.
        val zig = (v shl 1) xor (v shr 63)
        writeVarUInt(zig)
    }

    @Throws(IOException::class)
    override fun writeString(v: String) {
        val bytes = v.toByteArray(StandardCharsets.UTF_8)
        writeVarUInt(bytes.size.toLong())
        out.write(bytes, 0, bytes.size)
    }

    @Throws(IOException::class)
    override fun writeBytes(src: ByteArray, offset: Int, length: Int) {
        if (offset < 0 || length < 0 || offset + length > src.size) {
            throw IndexOutOfBoundsException(
                "offset=$offset, length=$length, src.length=${src.size}"
            )
        }
        out.write(src, offset, length)
    }

    @Throws(IOException::class)
    override fun flush() {
        out.flush()
    }

    // ----- Bulk fixed-width writes (improvement 04) -----
    //
    // These fill a reused scratch byte[] via a LITTLE_ENDIAN ByteBuffer view and
    // emit each chunk with a single write. The byte sequence produced is identical
    // to looping the per-value writer; only the CPU path differs. ClickHouse is
    // little-endian, so the buffer's order is fixed to LITTLE_ENDIAN.

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
    override fun writeFrom(src: LongArray, n: Int) {
        val step = chunkValues(java.lang.Long.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Long.BYTES
            val buf = scratch(bytes)
            buf.asLongBuffer().put(src, off, count)
            out.write(scratch, 0, bytes)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun writeFrom(src: IntArray, n: Int) {
        val step = chunkValues(Integer.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * Integer.BYTES
            val buf = scratch(bytes)
            buf.asIntBuffer().put(src, off, count)
            out.write(scratch, 0, bytes)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun writeFrom(src: ShortArray, n: Int) {
        val step = chunkValues(java.lang.Short.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Short.BYTES
            val buf = scratch(bytes)
            buf.asShortBuffer().put(src, off, count)
            out.write(scratch, 0, bytes)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun writeFrom(src: DoubleArray, n: Int) {
        val step = chunkValues(java.lang.Double.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Double.BYTES
            val buf = scratch(bytes)
            buf.asDoubleBuffer().put(src, off, count)
            out.write(scratch, 0, bytes)
            off += step
        }
    }

    @Throws(IOException::class)
    override fun writeFrom(src: FloatArray, n: Int) {
        val step = chunkValues(java.lang.Float.BYTES)
        var off = 0
        while (off < n) {
            val count = minOf(step, n - off)
            val bytes = count * java.lang.Float.BYTES
            val buf = scratch(bytes)
            buf.asFloatBuffer().put(src, off, count)
            out.write(scratch, 0, bytes)
            off += step
        }
    }

    private companion object {

        /**
         * Upper bound (in bytes) of the reusable scratch buffer for bulk writes. Large
         * runs are emitted in chunks of this size so a single column cannot force an
         * unbounded allocation.
         */
        private const val MAX_BULK_CHUNK_BYTES = 1 shl 16 // 64 KiB

        private val EMPTY_SCRATCH = ByteArray(0)
        private val EMPTY_SCRATCH_BUF: ByteBuffer =
            ByteBuffer.wrap(EMPTY_SCRATCH).order(ByteOrder.LITTLE_ENDIAN)

        /** Values per chunk for a run whose values are each [width] bytes. */
        private fun chunkValues(width: Int): Int {
            val v = MAX_BULK_CHUNK_BYTES / width
            return if (v < 1) 1 else v
        }
    }
}
