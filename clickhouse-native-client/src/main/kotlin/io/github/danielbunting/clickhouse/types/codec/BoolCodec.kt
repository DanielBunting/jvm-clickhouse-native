package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Bool`.
 *
 * Wire format: one byte per value, `0` (false) or `1` (true).
 * ClickHouse 25.6 reports the column type with the literal string `Bool`.
 *
 * Backing array type: `byte[]` (one byte per row, matching the wire
 * width). [get] boxes to [Boolean]; [set] accepts a
 * [Boolean], any [Number] (non-zero is `true`), or the
 * strings `"true"`/`"false"`.
 *
 * Java type: [Boolean].
 *
 * @constructor Public no-arg constructor required by the codec registry.
 */
public class BoolCodec : ColumnCodec<ByteArray> {

    override fun typeName(): String {
        return "Bool"
    }

    override fun allocate(rowCount: Int): ByteArray {
        return ByteArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: ByteArray) {
        for (i in 0 until rowCount) {
            dest[i] = (if (input.readUInt8() != 0) 1 else 0).toByte()
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: ByteArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeUInt8(if (src[i].toInt() != 0) 1 else 0)
        }
    }

    override fun get(array: ByteArray, row: Int): Any {
        return array[row].toInt() != 0
    }

    override fun set(array: ByteArray, row: Int, value: Any?) {
        array[row] = if (toBool(value)) 1.toByte() else 0.toByte()
    }

    override fun getLong(array: ByteArray, row: Int): Long {
        return if (array[row].toInt() != 0) 1L else 0L
    }

    override fun setLong(array: ByteArray, row: Int, v: Long) {
        array[row] = if (v != 0L) 1.toByte() else 0.toByte()
    }

    override fun javaType(): Class<*> {
        return Boolean::class.javaObjectType
    }

    public companion object {

        @JvmStatic
        private fun toBool(value: Any?): Boolean {
            if (value == null) {
                return false
            }
            if (value is Boolean) {
                return value
            }
            if (value is Number) {
                return value.toLong() != 0L
            }
            if (value is String) {
                return java.lang.Boolean.parseBoolean(value.trim { it <= ' ' })
            }
            throw IllegalArgumentException(
                "Cannot coerce " + value.javaClass.name + " to Bool"
            )
        }
    }
}
