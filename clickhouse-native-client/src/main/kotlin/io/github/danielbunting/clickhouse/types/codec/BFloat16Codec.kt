package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for the ClickHouse `BFloat16` type (experimental;
 * may require `SET allow_experimental_bfloat16_type = 1`).
 *
 * Wire format: two bytes per value (little-endian), holding the high 16 bits of
 * an IEEE-754 single-precision `float` (sign + 8 exponent + 7 mantissa bits).
 * This is a straight truncation of the `Float32` bit pattern.
 *
 * Backing array: `short[]` storing the raw 16-bit pattern.
 *
 * Java type: [Float]. [get] expands the 16 bits back into a
 * `float` via `Float.intBitsToFloat(bits << 16)`; [set] accepts
 * any [Number], truncating its `float` value to bfloat16.
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class BFloat16Codec : ColumnCodec<ShortArray> {

    override fun typeName(): String {
        return "BFloat16"
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
        return toFloat(array[row])
    }

    override fun set(array: ShortArray, row: Int, value: Any?) {
        val f = if (value == null) 0f else (value as Number).toFloat()
        array[row] = toBits(f)
    }

    override fun getDouble(array: ShortArray, row: Int): Double {
        return toFloat(array[row]).toDouble()
    }

    override fun setDouble(array: ShortArray, row: Int, v: Double) {
        array[row] = toBits(v.toFloat())
    }

    override fun javaType(): Class<*> {
        return Float::class.javaObjectType
    }

    public companion object {

        /** Expands a 16-bit bfloat16 pattern into a `float`. */
        @JvmStatic
        private fun toFloat(bits: Short): Float {
            // Place the 16 stored bits as the high half of a 32-bit float pattern.
            val intBits = (bits.toInt() and 0xFFFF) shl 16
            return java.lang.Float.intBitsToFloat(intBits)
        }

        /** Truncates a `float` to its high 16 bits (bfloat16). */
        @JvmStatic
        private fun toBits(f: Float): Short {
            return (java.lang.Float.floatToIntBits(f) ushr 16).toShort()
        }
    }
}
