package io.github.danielbunting.clickhouse.protocol

import java.io.EOFException
import java.io.IOException

/**
 * Reads ClickHouse native-protocol primitives from an underlying byte source.
 *
 * Wire conventions (verify against the CH.Native .NET source before implementing):
 * - Multi-byte integers are **little-endian**.
 * - `VarUInt` is unsigned LEB128 (7 bits/byte, high bit = continuation).
 * - Strings are a `VarUInt` byte-length followed by that many UTF-8 bytes.
 *
 * **Contract frozen in W0.2.** Concrete implementation is task W1.A1.
 * Unsigned reads are widened to the next signed Java type so callers never see
 * sign artifacts; [readUInt64] returns the raw 64 bits to be interpreted
 * as unsigned by the caller.
 */
public interface BinaryReader {

    /** Reads one byte, returned as an unsigned value in `[0, 255]`. */
    @Throws(IOException::class)
    public fun readByteUnsigned(): Int

    /**
     * Reads one byte as an unsigned value in `[0, 255]`, or returns `-1`
     * at end-of-stream instead of throwing. Used to detect a clean stream end at a
     * frame boundary without consuming a partial header.
     *
     * The default implementation infers EOF from an [EOFException]
     * thrown by [readByteUnsigned]; [DefaultBinaryReader] overrides it
     * to consult the underlying stream directly.
     */
    @Throws(IOException::class)
    public fun readByteOrEof(): Int {
        return try {
            readByteUnsigned()
        } catch (eof: EOFException) {
            -1
        }
    }

    @Throws(IOException::class)
    public fun readInt8(): Byte

    @Throws(IOException::class)
    public fun readInt16(): Short

    @Throws(IOException::class)
    public fun readInt32(): Int

    @Throws(IOException::class)
    public fun readInt64(): Long

    /** UInt8 widened to `int` in `[0, 255]`. */
    @Throws(IOException::class)
    public fun readUInt8(): Int

    /** UInt16 widened to `int` in `[0, 65535]`. */
    @Throws(IOException::class)
    public fun readUInt16(): Int

    /** UInt32 widened to `long` in `[0, 2^32-1]`. */
    @Throws(IOException::class)
    public fun readUInt32(): Long

    /** Raw 64 bits; interpret as unsigned (use [java.lang.Long.toUnsignedString] etc.). */
    @Throws(IOException::class)
    public fun readUInt64(): Long

    @Throws(IOException::class)
    public fun readFloat32(): Float

    @Throws(IOException::class)
    public fun readFloat64(): Double

    /** Unsigned LEB128. */
    @Throws(IOException::class)
    public fun readVarUInt(): Long

    /** Signed (zig-zag) LEB128. */
    @Throws(IOException::class)
    public fun readVarInt(): Long

    /** `VarUInt` length prefix + UTF-8 payload. */
    @Throws(IOException::class)
    public fun readString(): String

    /** Reads exactly [count] bytes into a fresh array. */
    @Throws(IOException::class)
    public fun readBytes(count: Int): ByteArray

    /** Reads exactly [length] bytes into [dest] starting at [offset]. */
    @Throws(IOException::class)
    public fun readFully(dest: ByteArray, offset: Int, length: Int)

    // ----- Bulk fixed-width reads (improvement 04) -----
    //
    // Each reads `n` contiguous little-endian fixed-width values into the
    // first `n` slots of `dest`. The bytes consumed from the wire are
    // identical to calling the matching per-value reader `n` times. These
    // are default-implemented as per-value loops so that non-bulk implementations
    // and callers are unaffected; DefaultBinaryReader overrides them with a
    // single bulk transfer.

    /** Reads [n] little-endian `Int64` values into `dest[0..n)`. */
    @Throws(IOException::class)
    public fun readInto(dest: LongArray, n: Int) {
        for (i in 0 until n) {
            dest[i] = readInt64()
        }
    }

    /** Reads [n] little-endian `Int32` values into `dest[0..n)`. */
    @Throws(IOException::class)
    public fun readInto(dest: IntArray, n: Int) {
        for (i in 0 until n) {
            dest[i] = readInt32()
        }
    }

    /** Reads [n] little-endian `Int16` values into `dest[0..n)`. */
    @Throws(IOException::class)
    public fun readInto(dest: ShortArray, n: Int) {
        for (i in 0 until n) {
            dest[i] = readInt16()
        }
    }

    /** Reads [n] little-endian `Float64` values into `dest[0..n)`. */
    @Throws(IOException::class)
    public fun readInto(dest: DoubleArray, n: Int) {
        for (i in 0 until n) {
            dest[i] = readFloat64()
        }
    }

    /** Reads [n] little-endian `Float32` values into `dest[0..n)`. */
    @Throws(IOException::class)
    public fun readInto(dest: FloatArray, n: Int) {
        for (i in 0 until n) {
            dest[i] = readFloat32()
        }
    }
}
