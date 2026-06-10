package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Int8` (signed 8-bit integer).
 *
 * Backing array type: `byte[]` (one byte per row, no padding).
 * [get] boxes to [Byte]; [set] accepts a [Byte] or
 * any [Number] that fits in a byte.
 *
 * Wire format: one byte per value, same representation as Java `byte`.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class Int8Codec : ColumnCodec<ByteArray> {

    override fun typeName(): String {
        return "Int8"
    }

    override fun allocate(rowCount: Int): ByteArray {
        return ByteArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: ByteArray) {
        // Backing byte[] == wire bytes (no endianness): one bulk readFully.
        input.readFully(dest, 0, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: ByteArray, rowCount: Int) {
        out.writeBytes(src, 0, rowCount)
    }

    override fun get(array: ByteArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: ByteArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireBoxed(
            value, Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong(), "Int8"
        ).toByte()
    }

    override fun getLong(array: ByteArray, row: Int): Long {
        return array[row].toLong()
    }

    override fun setLong(array: ByteArray, row: Int, v: Long) {
        array[row] = IntegerRanges.require(
            v, Byte.MIN_VALUE.toLong(), Byte.MAX_VALUE.toLong(), "Int8"
        ).toByte()
    }

    override fun javaType(): Class<*> {
        return Byte::class.javaObjectType
    }
}
