package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Edge-value unit tests for {@link UInt64Codec}, focused on values above
 * {@code Long.MAX_VALUE}.
 *
 * <p>UInt64 has no Java equivalent: the raw 64-bit pattern is stored in a signed
 * {@code long}; the unsigned magnitude is recovered with
 * {@link Long#toUnsignedString(long)}. These tests lock that the high half of the
 * range round-trips bit-exactly and surfaces correctly as an unsigned decimal.
 */
class UInt64UnsignedTest {

    @Test
    void aboveLongMax_roundTripBitsAndUnsignedString() throws IOException {
        UInt64Codec codec = new UInt64Codec();

        // 2^63      = 0x8000_0000_0000_0000 (Long.MIN_VALUE bits) = "9223372036854775808"
        // 2^64 - 1  = 0xFFFF_FFFF_FFFF_FFFF (-1 bits)             = "18446744073709551615"
        // 2^63 + 1  = Long.MIN_VALUE + 1                          = "9223372036854775809"
        long[] src = {
            Long.MIN_VALUE,
            -1L,
            Long.MIN_VALUE + 1,
        };

        byte[] wire = Bytes.capture(w -> codec.write(w, src, src.length));
        assertEquals(src.length * 8, wire.length);

        long[] dest = codec.allocate(src.length);
        codec.read(Bytes.reader(wire), src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals("9223372036854775808", Long.toUnsignedString(dest[0]));
        assertEquals("18446744073709551615", Long.toUnsignedString(dest[1]));
        assertEquals("9223372036854775809", Long.toUnsignedString(dest[2]));

        // get() returns the raw bits boxed as Long (negative for the high half).
        assertEquals(Long.MIN_VALUE, (Long) codec.get(dest, 0));
        assertEquals(-1L, (Long) codec.get(dest, 1));
        // getLong() exposes the same bits without boxing.
        assertEquals(Long.MIN_VALUE, codec.getLong(dest, 0));
    }

    @Test
    void maxUnsignedWireIsAllFF() throws IOException {
        // 2^64 - 1 little-endian is eight 0xFF bytes.
        UInt64Codec codec = new UInt64Codec();
        long[] src = {-1L};
        byte[] wire = Bytes.capture(w -> codec.write(w, src, 1));
        byte[] expected = new byte[8];
        java.util.Arrays.fill(expected, (byte) 0xFF);
        assertArrayEquals(expected, wire);
    }

    @Test
    void twoPow63WireLayout() throws IOException {
        // 2^63 = 0x8000_0000_0000_0000 -> LE [0,0,0,0,0,0,0,0x80].
        UInt64Codec codec = new UInt64Codec();
        long[] src = {Long.MIN_VALUE};
        byte[] wire = Bytes.capture(w -> codec.write(w, src, 1));
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, (byte) 0x80}, wire);
    }

    @Test
    void setLong_storesRawBits() {
        UInt64Codec codec = new UInt64Codec();
        long[] arr = codec.allocate(1);
        codec.setLong(arr, 0, -1L);
        assertEquals(-1L, codec.getLong(arr, 0));
        assertEquals("18446744073709551615", Long.toUnsignedString(codec.getLong(arr, 0)));
    }

    @Test
    void set_bigIntegerInHighRange_storesRawBits() {
        // A BigInteger in [2^63, 2^64-1] is in range; its low 64 bits are the correct
        // unsigned raw pattern (a negative long).
        UInt64Codec codec = new UInt64Codec();
        long[] arr = codec.allocate(2);
        codec.set(arr, 0, BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)); // 2^64-1
        codec.set(arr, 1, BigInteger.ONE.shiftLeft(63));                          // 2^63
        assertEquals(-1L, codec.getLong(arr, 0));
        assertEquals(Long.MIN_VALUE, codec.getLong(arr, 1));
        assertEquals("18446744073709551615", Long.toUnsignedString(codec.getLong(arr, 0)));
        assertEquals("9223372036854775808", Long.toUnsignedString(codec.getLong(arr, 1)));
    }

    @Test
    void set_bigIntegerOutOfRange_rejected() {
        // The codec range-checks BigInteger inputs (unambiguous magnitude) rather than
        // silently wrapping via longValue() — consistent with the fixed-width int codecs.
        UInt64Codec codec = new UInt64Codec();
        long[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, BigInteger.ONE.shiftLeft(64)));        // 2^64
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, BigInteger.valueOf(-1)));              // negative
    }

    @Test
    void set_negativeBoxedLong_treatedAsRawBits() {
        // A plain Long is ambiguous, so it is taken as raw bits per the documented contract:
        // -1L means 2^64-1, NOT an out-of-range error.
        UInt64Codec codec = new UInt64Codec();
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, -1L);
        assertEquals("18446744073709551615", Long.toUnsignedString(codec.getLong(arr, 0)));
    }
}
