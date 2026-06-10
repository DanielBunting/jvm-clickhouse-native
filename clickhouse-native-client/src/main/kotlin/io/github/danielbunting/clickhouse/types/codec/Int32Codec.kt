package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Int32` (signed 32-bit integer).
 *
 * Backing array type: `int[]` (four bytes per row, little-endian on the wire).
 * [get] boxes to [Integer]; [set] accepts any [Number].
 *
 * Wire format: 4 bytes little-endian per value.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class Int32Codec : ColumnCodec<IntArray> {

    override fun typeName(): String {
        return "Int32"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        // Wire width (4B) == int[] element width: bulk little-endian transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: IntArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireBoxed(
            value, Integer.MIN_VALUE.toLong(), Integer.MAX_VALUE.toLong(), "Int32"
        ).toInt()
    }

    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    override fun setLong(array: IntArray, row: Int, v: Long) {
        array[row] = IntegerRanges.require(
            v, Integer.MIN_VALUE.toLong(), Integer.MAX_VALUE.toLong(), "Int32"
        ).toInt()
    }

    override fun javaType(): Class<*> {
        return Int::class.javaObjectType
    }
}
