package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `UInt16` (unsigned 16-bit integer, range [0, 65535]).
 *
 * Backing array type: `int[]` — widened to avoid sign artifacts, matching the
 * convention that [BinaryReader.readUInt16] returns an `int` in [0, 65535].
 * [get] boxes to [Integer]; [set] accepts any [Number].
 *
 * Wire format: 2 bytes little-endian per value (unsigned).
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class UInt16Codec : ColumnCodec<IntArray> {

    override fun typeName(): String {
        return "UInt16"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    // Left element-wise (improvement 04): the wire width is 2 bytes but the backing
    // array is int[] (4 bytes/element) to hold the widened [0,65535] value, so the
    // bulk readInto(short[])/readInto(int[]) paths do not match this layout without
    // an extra widen pass. Element-wise widening keeps correctness simple.
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        for (i in 0 until rowCount) {
            dest[i] = input.readUInt16()
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeUInt16(src[i])
        }
    }

    override fun get(array: IntArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireBoxed(value, 0L, 0xFFFFL, "UInt16").toInt()
    }

    /** Backing `int[]` already holds [0,65535]; widens cleanly to `long`. */
    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    override fun setLong(array: IntArray, row: Int, v: Long) {
        array[row] = IntegerRanges.require(v, 0L, 0xFFFFL, "UInt16").toInt()
    }

    override fun javaType(): Class<*> {
        return Int::class.javaObjectType
    }
}
