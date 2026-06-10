package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `UInt8` (unsigned 8-bit integer, range [0, 255]).
 *
 * Backing array type: `int[]` — widened to avoid sign artifacts, matching the
 * convention that [BinaryReader.readUInt8] returns an `int` in [0, 255].
 * [get] boxes to [Integer]; [set] accepts any [Number].
 *
 * Wire format: 1 byte per value (unsigned).
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class UInt8Codec : ColumnCodec<IntArray> {

    override fun typeName(): String {
        return "UInt8"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    // Left element-wise (improvement 04): the wire width is 1 byte but the backing
    // array is int[] (4 bytes/element) to hold the widened [0,255] value, so there
    // is no clean same-width bulk transfer into int[]. Widening one byte at a time
    // keeps correctness simple; a bulk byte[] read + widen loop would not reduce the
    // per-element work materially for this width.
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        for (i in 0 until rowCount) {
            dest[i] = input.readUInt8()
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeUInt8(src[i])
        }
    }

    override fun get(array: IntArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireBoxed(value, 0L, 0xFFL, "UInt8").toInt()
    }

    /** Backing `int[]` already holds [0,255]; widens cleanly to `long`. */
    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    override fun setLong(array: IntArray, row: Int, v: Long) {
        array[row] = IntegerRanges.require(v, 0L, 0xFFL, "UInt8").toInt()
    }

    override fun javaType(): Class<*> {
        return Int::class.javaObjectType
    }
}
