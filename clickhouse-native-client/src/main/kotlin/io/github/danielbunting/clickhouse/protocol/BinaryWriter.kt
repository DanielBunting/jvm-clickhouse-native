package io.github.danielbunting.clickhouse.protocol

import java.io.IOException

/**
 * Writes ClickHouse native-protocol primitives to an underlying byte sink.
 * Mirror of [BinaryReader]; see it for wire conventions.
 *
 * **Contract frozen in W0.2.** Concrete implementation is task W1.A1.
 * Unsigned writes accept a widened signed Java type and emit the correct byte width.
 */
public interface BinaryWriter {

    @Throws(IOException::class)
    public fun writeInt8(v: Byte)

    @Throws(IOException::class)
    public fun writeInt16(v: Short)

    @Throws(IOException::class)
    public fun writeInt32(v: Int)

    @Throws(IOException::class)
    public fun writeInt64(v: Long)

    /** Emits one byte; [v] expected in `[0, 255]`. */
    @Throws(IOException::class)
    public fun writeUInt8(v: Int)

    /** Emits two LE bytes; [v] expected in `[0, 65535]`. */
    @Throws(IOException::class)
    public fun writeUInt16(v: Int)

    /** Emits four LE bytes; [v] expected in `[0, 2^32-1]`. */
    @Throws(IOException::class)
    public fun writeUInt32(v: Long)

    /** Emits the raw 64 bits (caller supplies the unsigned value as a `long`). */
    @Throws(IOException::class)
    public fun writeUInt64(v: Long)

    @Throws(IOException::class)
    public fun writeFloat32(v: Float)

    @Throws(IOException::class)
    public fun writeFloat64(v: Double)

    /** Unsigned LEB128. */
    @Throws(IOException::class)
    public fun writeVarUInt(v: Long)

    /** Signed (zig-zag) LEB128. */
    @Throws(IOException::class)
    public fun writeVarInt(v: Long)

    /** `VarUInt` length prefix + UTF-8 payload. */
    @Throws(IOException::class)
    public fun writeString(v: String)

    @Throws(IOException::class)
    public fun writeBytes(src: ByteArray, offset: Int, length: Int)

    @Throws(IOException::class)
    public fun flush()

    // ----- Bulk fixed-width writes (improvement 04) -----
    //
    // Each writes the first `n` values of `src` as contiguous
    // little-endian fixed-width values. The bytes emitted are identical to calling
    // the matching per-value writer `n` times. Default-implemented as
    // per-value loops so non-bulk implementations and callers are unaffected;
    // DefaultBinaryWriter overrides them with a single bulk transfer.

    /** Writes `src[0..n)` as little-endian `Int64` values. */
    @Throws(IOException::class)
    public fun writeFrom(src: LongArray, n: Int) {
        for (i in 0 until n) {
            writeInt64(src[i])
        }
    }

    /** Writes `src[0..n)` as little-endian `Int32` values. */
    @Throws(IOException::class)
    public fun writeFrom(src: IntArray, n: Int) {
        for (i in 0 until n) {
            writeInt32(src[i])
        }
    }

    /** Writes `src[0..n)` as little-endian `Int16` values. */
    @Throws(IOException::class)
    public fun writeFrom(src: ShortArray, n: Int) {
        for (i in 0 until n) {
            writeInt16(src[i])
        }
    }

    /** Writes `src[0..n)` as little-endian `Float64` values. */
    @Throws(IOException::class)
    public fun writeFrom(src: DoubleArray, n: Int) {
        for (i in 0 until n) {
            writeFloat64(src[i])
        }
    }

    /** Writes `src[0..n)` as little-endian `Float32` values. */
    @Throws(IOException::class)
    public fun writeFrom(src: FloatArray, n: Int) {
        for (i in 0 until n) {
            writeFloat32(src[i])
        }
    }
}
