package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.math.BigDecimal
import java.math.BigInteger

/**
 * Codec for the ClickHouse `Decimal(P, S)` type.
 *
 * ClickHouse stores decimals as fixed-width signed little-endian integers
 * scaled by `10^scale`:
 *
 *  - `Decimal32` (`P <= 9`): 4 bytes (Int32)
 *  - `Decimal64` (`P <= 18`): 8 bytes (Int64)
 *  - `Decimal128` (`P <= 38`): 16 bytes (Int128, little-endian)
 *  - `Decimal256` (`P <= 76`): 32 bytes (Int256, little-endian)
 *
 * The backing array is:
 *
 *  - `long[]` for `P <= 18` (Decimal32 and Decimal64)
 *  - `Object[]` (each element a [BigInteger]) for `P > 18`
 *
 * [get] always returns a [BigDecimal] with the correct scale applied.
 *
 * **Rounding contract:** [set] rescales the input to the column scale with
 * [java.math.RoundingMode.HALF_UP] (e.g. `Decimal(9,2)` stores `1.005` as
 * `1.01`). This is a deliberate, documented choice and differs from the temporal
 * sub-second codecs ([DateTime64Codec]/[Time64Codec]), which *truncate*
 * toward zero. A value whose magnitude needs more than `precision` significant digits
 * is rejected with an [IllegalArgumentException].
 *
 * // VERIFY against CH.Native: Int128/Int256 byte order is little-endian two's-complement.
 * // The .NET source reads/writes the low word then high word for Int128.
 */
public class DecimalCodec(precision: Int, scale: Int) : ColumnCodec<Any> {

    private val precision: Int
    private val scale: Int

    /** Byte width on the wire: 4, 8, 16, or 32. */
    private val byteWidth: Int

    /** True when the backing array is `long[]` (precision <= 18). */
    private val useLong: Boolean

    /** `10^precision`; a stored unscaled value must satisfy `|unscaled| < precisionLimit`. */
    private val precisionLimit: BigInteger

    /**
     * Constructs a `DecimalCodec`.
     *
     * @param precision the total number of significant digits (P)
     * @param scale     the number of digits after the decimal point (S)
     */
    init {
        if (precision < 1 || precision > 76) {
            throw IllegalArgumentException("Decimal precision must be 1–76, got: " + precision)
        }
        if (scale < 0 || scale > precision) {
            throw IllegalArgumentException(
                "Decimal scale must be 0–precision, got scale=" + scale + " precision=" + precision
            )
        }
        this.precision = precision
        this.scale = scale
        this.byteWidth = wireByteWidth(precision)
        this.useLong = precision <= MAX_PRECISION_LONG
        this.precisionLimit = BigInteger.TEN.pow(precision)
    }

    override fun typeName(): String {
        return "Decimal(" + precision + ", " + scale + ")"
    }

    override fun allocate(rowCount: Int): Any {
        return if (useLong) LongArray(rowCount) else arrayOfNulls<Any>(rowCount)
    }

    /**
     * The boxed backing array of a BigInteger-backed decimal column. Safe by
     * construction: the only arrays that reach the non-long paths are the
     * `arrayOfNulls<Any>` ones [allocate] created.
     */
    @Suppress("UNCHECKED_CAST")
    private fun boxedArray(array: Any): Array<Any?> = array as Array<Any?>

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: Any) {
        if (useLong) {
            val arr = dest as LongArray
            for (i in 0 until rowCount) {
                arr[i] = readLongValue(input)
            }
        } else {
            val arr = boxedArray(dest)
            for (i in 0 until rowCount) {
                arr[i] = readBigIntegerValue(input)
            }
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Any, rowCount: Int) {
        if (useLong) {
            val arr = src as LongArray
            for (i in 0 until rowCount) {
                writeLongValue(out, arr[i])
            }
        } else {
            val arr = boxedArray(src)
            for (i in 0 until rowCount) {
                val v = arr[i]
                val value = if (v != null) v as BigInteger else BigInteger.ZERO
                writeBigIntegerValue(out, value)
            }
        }
    }

    override fun get(array: Any, row: Int): Any {
        if (useLong) {
            val raw = (array as LongArray)[row]
            return BigDecimal.valueOf(raw, scale)
        } else {
            var raw = boxedArray(array)[row] as BigInteger?
            if (raw == null) raw = BigInteger.ZERO
            return BigDecimal(raw, scale)
        }
    }

    override fun set(array: Any, row: Int, value: Any?) {
        val bd: BigDecimal
        if (value is BigDecimal) {
            bd = value.setScale(scale, java.math.RoundingMode.HALF_UP)
        } else if (value is Number) {
            bd = BigDecimal(value.toString()).setScale(scale, java.math.RoundingMode.HALF_UP)
        } else if (value == null) {
            bd = BigDecimal.ZERO.setScale(scale)
        } else {
            throw IllegalArgumentException("Cannot set Decimal from: " + value.javaClass.name)
        }

        val unscaled = bd.unscaledValue()
        // A value with more than `precision` significant digits cannot be represented in
        // Decimal(P, S); reject it with a clear message rather than silently overflowing the
        // wire width (or throwing the opaque ArithmeticException that longValueExact would).
        if (unscaled.abs().compareTo(precisionLimit) >= 0) {
            throw IllegalArgumentException(
                typeName() + " value out of range: " + bd + " exceeds " + precision
                    + " significant digits"
            )
        }
        if (useLong) {
            (array as LongArray)[row] = unscaled.longValueExact()
        } else {
            boxedArray(array)[row] = unscaled
        }
    }

    override fun javaType(): Class<*> {
        return BigDecimal::class.java
    }

    // ---- private helpers ----

    @Throws(IOException::class)
    private fun readLongValue(input: BinaryReader): Long {
        if (byteWidth == 4) {
            return input.readInt32().toLong() // signed 4-byte LE
        } else {
            return input.readInt64() // signed 8-byte LE
        }
    }

    @Throws(IOException::class)
    private fun writeLongValue(out: BinaryWriter, v: Long) {
        if (byteWidth == 4) {
            out.writeInt32(v.toInt())
        } else {
            out.writeInt64(v)
        }
    }

    /**
     * Reads a signed little-endian integer of [byteWidth] bytes (16 or 32)
     * and converts it to a [BigInteger].
     *
     * // VERIFY against CH.Native: Int128 and Int256 are stored as little-endian
     * // two's-complement byte arrays; we reverse to big-endian for BigInteger.
     */
    @Throws(IOException::class)
    private fun readBigIntegerValue(input: BinaryReader): BigInteger {
        val leBytes = input.readBytes(byteWidth)
        // Reverse to big-endian (BigInteger constructor expects big-endian)
        val beBytes = ByteArray(byteWidth)
        for (i in 0 until byteWidth) {
            beBytes[i] = leBytes[byteWidth - 1 - i]
        }
        return BigInteger(beBytes) // signed two's-complement
    }

    /**
     * Writes a [BigInteger] as a signed little-endian integer of [byteWidth] bytes.
     *
     * // VERIFY against CH.Native: confirm Int128/Int256 are written little-endian.
     */
    @Throws(IOException::class)
    private fun writeBigIntegerValue(out: BinaryWriter, value: BigInteger) {
        val beBytes = value.toByteArray() // big-endian, sign-extended
        // Normalize to exactly byteWidth bytes (big-endian, sign-extended)
        val normalized = ByteArray(byteWidth)
        val signByte = if (value.signum() < 0) 0xFF.toByte() else 0x00.toByte()
        // Fill with sign extension first
        for (i in 0 until byteWidth) {
            normalized[i] = signByte
        }
        // Copy the actual bytes into the tail (most-significant bytes)
        val srcStart = Math.max(0, beBytes.size - byteWidth)
        val dstStart = byteWidth - (beBytes.size - srcStart)
        System.arraycopy(beBytes, srcStart, normalized, dstStart, beBytes.size - srcStart)

        // Write in little-endian order
        var i = byteWidth - 1
        while (i >= 0) {
            out.writeBytes(normalized, i, 1)
            i--
        }
    }

    /**
     * Returns the decimal precision (P).
     *
     * @return precision
     */
    public fun precision(): Int {
        return precision
    }

    /**
     * Returns the decimal scale (S).
     *
     * @return scale
     */
    public fun scale(): Int {
        return scale
    }

    /**
     * Returns the wire byte width (4, 8, 16, or 32).
     *
     * @return byte width
     */
    public fun byteWidth(): Int {
        return byteWidth
    }

    private companion object {

        private const val MAX_PRECISION_LONG = 18
        private const val MAX_PRECISION_INT = 9

        private fun wireByteWidth(precision: Int): Int {
            if (precision <= 9) return 4
            if (precision <= 18) return 8
            if (precision <= 38) return 16
            return 32
        }
    }
}
