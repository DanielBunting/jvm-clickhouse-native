package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `UInt64` (unsigned 64-bit integer).
 *
 * Backing array type: `long[]` — raw bits; the full 64-bit range
 * [0, 2^64-1] is preserved by treating the `long` as an unsigned value.
 * Callers that need the unsigned magnitude should use
 * [Long.toUnsignedString] or [java.math.BigInteger] arithmetic.
 *
 * [get] returns a [Long] containing the raw bits (same as
 * what [BinaryReader.readUInt64] produces). [set] accepts any
 * [Number]; for values that exceed `Long.MAX_VALUE` pass a
 * [Long] whose bits represent the unsigned value.
 *
 * Wire format: 8 bytes little-endian per value (unsigned, raw bits).
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class UInt64Codec : ColumnCodec<LongArray> {

    override fun typeName(): String {
        return "UInt64"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        // Wire width (8B) == long[] element width; the backing long holds the raw
        // 64-bit pattern, so the bulk little-endian transfer is byte-identical to
        // looping readUInt64().
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    /**
     * Returns the raw `long` bits boxed as [Long].
     * To obtain the unsigned decimal representation use
     * `Long.toUnsignedString((Long) get(array, row))`.
     */
    override fun get(array: LongArray, row: Int): Any {
        return array[row]
    }

    /**
     * Stores [value] as the raw 64-bit pattern. A [java.math.BigInteger]
     * is range-checked against `[0, 2^64-1]` and rejected if outside (rather than
     * silently wrapped via `longValue()`); any other [Number] is taken as its
     * raw `long` bits per the unsigned raw-bit contract.
     */
    override fun set(array: LongArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireUInt64(value)
    }

    /**
     * Returns the raw 64-bit pattern as-is (no boxing). For values above
     * `Long.MAX_VALUE` the result is negative; callers interpret the
     * unsignedness (e.g. [Long.toUnsignedString]).
     */
    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    /** Stores the raw 64-bit pattern as-is. */
    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return Long::class.javaObjectType
    }
}
