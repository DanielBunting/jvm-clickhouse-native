package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Int64` (signed 64-bit integer).
 *
 * Backing array type: `long[]` (eight bytes per row, little-endian on the wire).
 * [get] boxes to [Long]; [set] accepts any [Number].
 *
 * Wire format: 8 bytes little-endian per value.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class Int64Codec : ColumnCodec<LongArray> {

    override fun typeName(): String {
        return "Int64"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        // Wire width (8B) == long[] element width: bulk little-endian transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: LongArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: LongArray, row: Int, value: Any?) {
        array[row] = (value as Number).toLong()
    }

    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return Long::class.javaObjectType
    }
}
