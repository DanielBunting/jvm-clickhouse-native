package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.math.BigInteger

/**
 * Column-major codec for ClickHouse wide integer types: `Int128`,
 * `Int256`, `UInt128`, `UInt256`.
 *
 * Wire format: a fixed-width little-endian two's-complement integer of
 * `byteWidth` bytes (16 for the 128-bit types, 32 for the 256-bit types).
 * This mirrors `DecimalCodec.readBigIntegerValue`/`writeBigIntegerValue`:
 * the little-endian wire bytes are reversed to big-endian for the
 * [BigInteger] constructor, and on write the value is normalized to exactly
 * `byteWidth` sign-extended big-endian bytes then emitted little-endian.
 *
 * Signedness matters only on read: signed types use
 * `new BigInteger(beBytes)` (interpreting the high bit as a sign), while
 * unsigned types use `new BigInteger(1, beBytes)` so the value stays
 * non-negative (e.g. `UInt256` max = `2^256 - 1`). On write the two's
 * complement representation is identical for both, so writing is shared.
 *
 * Backing array: `BigInteger[]`. [get] returns a
 * [BigInteger]; [set] accepts a [BigInteger], any
 * [Number], or a numeric [String].
 *
 * @constructor Constructs a wide-integer codec.
 *
 * @param typeName  the canonical ClickHouse type name (e.g. `"Int128"`)
 * @param byteWidth the wire byte width: 16 or 32
 * @param unsigned  whether the type is unsigned
 */
public open class WideIntCodec protected constructor(
    private val typeName: String,
    /** Byte width on the wire: 16 (128-bit) or 32 (256-bit). */
    private val byteWidth: Int,
    /** True for unsigned types (`UInt128`/`UInt256`). */
    private val unsigned: Boolean,
) : ColumnCodec<Array<BigInteger?>> {

    /** Inclusive value range; a value outside it would silently truncate on the wire, so [set] rejects it. */
    private val min: BigInteger
    private val max: BigInteger

    init {
        val bits = byteWidth * 8
        if (unsigned) {
            min = BigInteger.ZERO
            max = BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE) // 2^bits - 1
        } else {
            max = BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE) // 2^(bits-1) - 1
            min = max.add(BigInteger.ONE).negate()                            // -2^(bits-1)
        }
    }

    override fun typeName(): String {
        return typeName
    }

    override fun allocate(rowCount: Int): Array<BigInteger?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: Array<BigInteger?>) {
        for (i in 0 until rowCount) {
            dest[i] = readValue(input)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<BigInteger?>, rowCount: Int) {
        for (i in 0 until rowCount) {
            val v = src[i] ?: BigInteger.ZERO
            writeValue(out, v)
        }
    }

    override fun get(array: Array<BigInteger?>, row: Int): Any {
        val v = array[row]
        return v ?: BigInteger.ZERO
    }

    override fun set(array: Array<BigInteger?>, row: Int, value: Any?) {
        array[row] = IntegerRanges.require(toBigInteger(value), min, max, typeName)
    }

    override fun javaType(): Class<*> {
        return BigInteger::class.java
    }

    // ---- wire helpers (mirror DecimalCodec.read/writeBigIntegerValue) ----

    /**
     * Reads a little-endian two's-complement integer of [byteWidth] bytes
     * and converts it to a [BigInteger], honouring [unsigned].
     */
    @Throws(IOException::class)
    private fun readValue(input: BinaryReader): BigInteger {
        val leBytes = input.readBytes(byteWidth)
        // Reverse to big-endian (BigInteger constructor expects big-endian).
        val beBytes = ByteArray(byteWidth)
        for (i in 0 until byteWidth) {
            beBytes[i] = leBytes[byteWidth - 1 - i]
        }
        // Signed: high bit is the sign. Unsigned: force non-negative magnitude.
        return if (unsigned) BigInteger(1, beBytes) else BigInteger(beBytes)
    }

    /**
     * Writes a [BigInteger] as a little-endian two's-complement integer of
     * [byteWidth] bytes. The two's-complement form is identical for signed
     * and unsigned types, so this is shared.
     */
    @Throws(IOException::class)
    private fun writeValue(out: BinaryWriter, value: BigInteger) {
        val beBytes = value.toByteArray() // big-endian, sign-extended
        val normalized = ByteArray(byteWidth)
        val signByte = if (value.signum() < 0) 0xFF.toByte() else 0x00.toByte()
        for (i in 0 until byteWidth) {
            normalized[i] = signByte
        }
        val srcStart = Math.max(0, beBytes.size - byteWidth)
        val dstStart = byteWidth - (beBytes.size - srcStart)
        System.arraycopy(beBytes, srcStart, normalized, dstStart, beBytes.size - srcStart)

        // Emit little-endian.
        var i = byteWidth - 1
        while (i >= 0) {
            out.writeBytes(normalized, i, 1)
            i--
        }
    }

    public companion object {

        @JvmStatic
        private fun toBigInteger(value: Any?): BigInteger {
            if (value == null) {
                return BigInteger.ZERO
            }
            if (value is BigInteger) {
                return value
            }
            if (value is java.math.BigDecimal) {
                return value.toBigIntegerExact()
            }
            if (value is Number) {
                return BigInteger.valueOf(value.toLong())
            }
            if (value is String) {
                return BigInteger(value.trim { it <= ' ' })
            }
            throw IllegalArgumentException(
                "Cannot set wide integer from: " + value.javaClass.name
            )
        }
    }
}
