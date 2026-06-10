package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.types.ColumnCodec;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Locks the insert-time range validation on the fixed-width integer codecs. The row-oriented
 * {@code set}/{@code setLong} paths (used by the bulk-insert mapper) must accept boundary values
 * and REJECT out-of-range ones with {@link IllegalArgumentException} — never silently narrow them
 * (the old behaviour turned {@code 9999 -> (byte) 15} and committed it as a clean write).
 */
class IntegerRangeValidationTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertNarrowRange(ColumnCodec codec, long min, long max) {
        Object arr = codec.allocate(1);
        // Boundaries accepted via both entry points, stored exactly.
        codec.setLong(arr, 0, min);
        assertEquals(min, codec.getLong(arr, 0));
        codec.setLong(arr, 0, max);
        assertEquals(max, codec.getLong(arr, 0));
        codec.set(arr, 0, min);
        assertEquals(min, codec.getLong(arr, 0));
        codec.set(arr, 0, max);
        assertEquals(max, codec.getLong(arr, 0));

        // Out of range on either side is rejected, not wrapped.
        assertThrows(IllegalArgumentException.class, () -> codec.setLong(arr, 0, min - 1));
        assertThrows(IllegalArgumentException.class, () -> codec.setLong(arr, 0, max + 1));
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, Long.valueOf(min - 1)));
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, Long.valueOf(max + 1)));
        // A BigInteger far outside the long range is rejected, not silently truncated by longValue().
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, BigInteger.valueOf(max).add(BigInteger.TEN.pow(30))));
    }

    @Test void int8()   { assertNarrowRange(new Int8Codec(),   Byte.MIN_VALUE, Byte.MAX_VALUE); }
    @Test void uint8()  { assertNarrowRange(new UInt8Codec(),  0L, 0xFFL); }
    @Test void int16()  { assertNarrowRange(new Int16Codec(),  Short.MIN_VALUE, Short.MAX_VALUE); }
    @Test void uint16() { assertNarrowRange(new UInt16Codec(), 0L, 0xFFFFL); }
    @Test void int32()  { assertNarrowRange(new Int32Codec(),  Integer.MIN_VALUE, Integer.MAX_VALUE); }
    @Test void uint32() { assertNarrowRange(new UInt32Codec(), 0L, 0xFFFFFFFFL); }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertWideRange(ColumnCodec codec, BigInteger min, BigInteger max) {
        Object arr = codec.allocate(1);
        codec.set(arr, 0, min);
        assertEquals(min, codec.get(arr, 0));
        codec.set(arr, 0, max);
        assertEquals(max, codec.get(arr, 0));
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, min.subtract(BigInteger.ONE)));
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, max.add(BigInteger.ONE)));
    }

    private static BigInteger signedMin(int bits) { return BigInteger.ONE.shiftLeft(bits - 1).negate(); }
    private static BigInteger signedMax(int bits) { return BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE); }
    private static BigInteger unsignedMax(int bits) { return BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE); }

    @Test void int128()  { assertWideRange(new Int128Codec(),  signedMin(128), signedMax(128)); }
    @Test void uint128() { assertWideRange(new UInt128Codec(), BigInteger.ZERO, unsignedMax(128)); }
    @Test void int256()  { assertWideRange(new Int256Codec(),  signedMin(256), signedMax(256)); }
    @Test void uint256() { assertWideRange(new UInt256Codec(), BigInteger.ZERO, unsignedMax(256)); }
}
