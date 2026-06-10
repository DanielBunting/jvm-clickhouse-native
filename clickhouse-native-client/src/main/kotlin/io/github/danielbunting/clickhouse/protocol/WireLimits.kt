package io.github.danielbunting.clickhouse.protocol

import io.github.danielbunting.clickhouse.ProtocolException

/**
 * Bounds checks for counts and lengths supplied by the server on the wire.
 *
 * A `VarUInt` row/column count or a `UInt64` array/map end-offset arrives
 * *untrusted*. Left unchecked, a value above [Integer.MAX_VALUE] silently
 * wraps when narrowed to `int` — potentially to a negative size that crashes the
 * decode with an unchecked [NegativeArraySizeException] or [ArithmeticException]
 * (from `Math.toIntExact`) — and an enormous-but-in-range value drives an unbounded
 * allocation. A buggy or hostile server should not be able to do either.
 *
 * These helpers reject such values up front with a [ProtocolException], consistent
 * with the client's reject-don't-silently-corrupt stance (mirroring
 * `types.codec.IntegerRanges` on the value path).
 */
public object WireLimits {

    /**
     * Validates that a wire-supplied count/length is a non-negative `int` and returns
     * it, throwing [ProtocolException] otherwise. A `long` whose high bit is set
     * (a `UInt64` at or above `2^63`) is negative here and is rejected, as is any
     * value above [Integer.MAX_VALUE].
     *
     * @param value the raw wire value (a `VarUInt` or `UInt64` widened to `long`)
     * @param what  a short description used in the error message (e.g. `"block row count"`)
     * @return [value] as an `int` when in `[0, Integer.MAX_VALUE]`
     */
    @JvmStatic
    public fun checkCount(value: Long, what: String): Int {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw ProtocolException(
                what + " out of range: " + java.lang.Long.toUnsignedString(value) +
                    " (must be 0.." + Integer.MAX_VALUE + ")"
            )
        }
        return value.toInt()
    }
}
