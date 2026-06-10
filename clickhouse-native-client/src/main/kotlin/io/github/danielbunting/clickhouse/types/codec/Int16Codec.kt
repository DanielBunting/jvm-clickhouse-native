package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Int16` (signed 16-bit integer).
 *
 * Backing array type: `short[]` (two bytes per row, little-endian on the wire).
 * [get] boxes to [Short]; [set] accepts any [Number].
 *
 * Wire format: 2 bytes little-endian per value.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class Int16Codec : ColumnCodec<ShortArray> {

    override fun typeName(): String {
        return "Int16"
    }

    override fun allocate(rowCount: Int): ShortArray {
        return ShortArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: ShortArray) {
        // Wire width (2B) == short[] element width: bulk little-endian transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: ShortArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: ShortArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: ShortArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireBoxed(
            value, Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong(), "Int16"
        ).toShort()
    }

    override fun getLong(array: ShortArray, row: Int): Long {
        return array[row].toLong()
    }

    override fun setLong(array: ShortArray, row: Int, v: Long) {
        array[row] = IntegerRanges.require(
            v, Short.MIN_VALUE.toLong(), Short.MAX_VALUE.toLong(), "Int16"
        ).toShort()
    }

    override fun javaType(): Class<*> {
        return Short::class.javaObjectType
    }
}
