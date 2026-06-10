package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Float32` (IEEE 754 single-precision).
 *
 * Backing array type: `float[]` (four bytes per row, little-endian IEEE 754).
 * [get] boxes to [Float]; [set] accepts any [Number].
 *
 * Wire format: 4 bytes little-endian IEEE 754 single-precision per value.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class Float32Codec : ColumnCodec<FloatArray> {

    override fun typeName(): String {
        return "Float32"
    }

    override fun allocate(rowCount: Int): FloatArray {
        return FloatArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: FloatArray) {
        // Wire width (4B IEEE-754) == float[] element width: bulk LE transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: FloatArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: FloatArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: FloatArray, row: Int, value: Any?) {
        array[row] = (value as Number).toFloat()
    }

    /** Widens the stored `float` to `double`. */
    override fun getDouble(array: FloatArray, row: Int): Double {
        return array[row].toDouble()
    }

    override fun setDouble(array: FloatArray, row: Int, v: Double) {
        array[row] = v.toFloat()
    }

    override fun javaType(): Class<*> {
        return Float::class.javaObjectType
    }
}
