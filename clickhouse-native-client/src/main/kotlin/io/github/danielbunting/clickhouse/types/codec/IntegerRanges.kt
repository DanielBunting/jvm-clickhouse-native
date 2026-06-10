package io.github.danielbunting.clickhouse.types.codec

import java.math.BigInteger

/**
 * Range validation for the fixed-width integer codecs' row-oriented `set`/`setLong`
 * paths. A bulk INSERT places caller-supplied values into a column one at a time through these
 * boxed/typed entry points; without a bounds check, a value outside the column's range was
 * silently narrowed (e.g. `9999 -> (byte) 9999 == 15` for `Int8`) and committed as a
 * clean, complete write — silent data corruption. These helpers reject such values up front with
 * a clear message instead.
 *
 * The column-major bulk `read`/`write` path is deliberately *not* guarded:
 * it moves whole primitive arrays on the hot path, and a caller using the raw-array API owns its
 * element semantics.
 */
internal object IntegerRanges {

    /** Largest `UInt64` value, `2^64 - 1`. */
    private val UINT64_MAX: BigInteger = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)

    /**
     * Coerces a boxed value to the raw 64-bit pattern for a `UInt64` column.
     *
     * A [BigInteger] is unambiguous about magnitude, so it is range-checked against
     * `[0, 2^64-1]` and rejected if outside — otherwise its low 64 bits would be silently
     * stored (e.g. `2^64` → `0`), the same silent-corruption the fixed-width codecs
     * guard against. A value in `[2^63, 2^64-1]` is in range and stored as its raw bit
     * pattern (a negative `long`). Any other [Number] is taken as its raw `long`
     * bits per the `UInt64` raw-bit contract (a caller passing `-1L` means `2^64-1`).
     */
    @JvmStatic
    fun requireUInt64(value: Any?): Long {
        if (value is BigInteger) {
            if (value.signum() < 0 || value.compareTo(UINT64_MAX) > 0) {
                throw IllegalArgumentException(
                    "UInt64 value out of range: $value is not in [0, $UINT64_MAX]"
                )
            }
            return value.toLong() // low 64 bits == correct unsigned raw pattern for in-range values
        }
        return (value as Number).toLong()
    }

    /** Returns [v] if it lies in `[min, max]`; otherwise throws [IllegalArgumentException]. */
    @JvmStatic
    fun require(v: Long, min: Long, max: Long, type: String): Long {
        if (v < min || v > max) {
            throw IllegalArgumentException(
                "$type value out of range: $v is not in [$min, $max]"
            )
        }
        return v
    }

    /**
     * Coerces a boxed numeric [value] to `long` and range-checks it against
     * `[min, max]`. A [BigInteger] too large to be a `long` is itself out of any
     * fixed-width range, so it is rejected rather than silently wrapped by `longValue()`.
     */
    @JvmStatic
    fun requireBoxed(value: Any?, min: Long, max: Long, type: String): Long {
        if (value is BigInteger) {
            if (value.bitLength() > 63) {
                throw IllegalArgumentException(
                    "$type value out of range: $value is not in [$min, $max]"
                )
            }
            return require(value.toLong(), min, max, type)
        }
        return require((value as Number).toLong(), min, max, type)
    }

    /** Returns [v] if it lies in `[min, max]` (wide-int range); otherwise throws. */
    @JvmStatic
    fun require(v: BigInteger, min: BigInteger, max: BigInteger, type: String): BigInteger {
        if (v.compareTo(min) < 0 || v.compareTo(max) > 0) {
            throw IllegalArgumentException(
                "$type value out of range: $v is not in [$min, $max]"
            )
        }
        return v
    }
}
