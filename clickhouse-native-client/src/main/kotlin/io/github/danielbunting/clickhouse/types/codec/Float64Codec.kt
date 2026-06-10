package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `Float64` (IEEE 754 double-precision).
 *
 * Backing array type: `double[]` (eight bytes per row, little-endian IEEE 754).
 * [get] boxes to [Double]; [set] accepts any [Number].
 *
 * Wire format: 8 bytes little-endian IEEE 754 double-precision per value.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class Float64Codec : ColumnCodec<DoubleArray> {

    override fun typeName(): String {
        return "Float64"
    }

    override fun allocate(rowCount: Int): DoubleArray {
        return DoubleArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: DoubleArray) {
        // Wire width (8B IEEE-754) == double[] element width: bulk LE transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: DoubleArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: DoubleArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: DoubleArray, row: Int, value: Any?) {
        array[row] = (value as Number).toDouble()
    }

    override fun getDouble(array: DoubleArray, row: Int): Double {
        return array[row]
    }

    override fun setDouble(array: DoubleArray, row: Int, v: Double) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return Double::class.javaObjectType
    }
}
