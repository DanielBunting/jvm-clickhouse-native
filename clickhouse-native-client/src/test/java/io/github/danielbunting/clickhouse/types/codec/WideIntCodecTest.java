package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Edge-value unit tests for the wide-integer codecs {@code Int128}/{@code Int256}/
 * {@code UInt128}/{@code UInt256} (all proxy to {@link WideIntCodec}).
 *
 * <p>Focus areas (vs. the happy-path integration round-trips):
 * <ul>
 *   <li>negative two's-complement values</li>
 *   <li>MIN / MAX for each width and signedness</li>
 *   <li>the signed↔unsigned boundary (high bit set)</li>
 *   <li>a byte-order-revealing value (asymmetric LE byte pattern)</li>
 *   <li>exact wire length (16 or 32 bytes) and little-endian layout</li>
 * </ul>
 *
 * <p>All I/O is in-memory via {@link Bytes}; no server required.
 */
class WideIntCodecTest {

    // ---- range helpers (mirror IntegerRangeValidationTest) ----
    private static BigInteger signedMin(int bits)   { return BigInteger.ONE.shiftLeft(bits - 1).negate(); }
    private static BigInteger signedMax(int bits)   { return BigInteger.ONE.shiftLeft(bits - 1).subtract(BigInteger.ONE); }
    private static BigInteger unsignedMax(int bits) { return BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE); }

    // ---- shared round-trip + wire-length helper ----
    private static byte[] writeOne(WideIntCodec codec, BigInteger v) {
        BigInteger[] src = codec.allocate(1);
        codec.set(src, 0, v);
        return Bytes.capture(w -> codec.write(w, src, 1));
    }

    private static BigInteger readOne(WideIntCodec codec, byte[] wire) throws IOException {
        BigInteger[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        return (BigInteger) codec.get(dest, 0);
    }

    /** Round-trips {@code v} through the codec and asserts the wire is {@code byteWidth} long. */
    private static void assertRoundTrip(WideIntCodec codec, BigInteger v, int byteWidth) throws IOException {
        byte[] wire = writeOne(codec, v);
        assertEquals(byteWidth, wire.length, "wire byte width");
        assertEquals(v, readOne(codec, wire), "round-trip value");
    }

    // =========================================================================
    // Int128 (16 bytes, signed)
    // =========================================================================

    @Test
    void int128_metadata() {
        Int128Codec codec = new Int128Codec();
        assertEquals("Int128", codec.typeName());
        assertEquals(BigInteger.class, codec.javaType());
    }

    @Test
    void int128_minMaxAndZeroRoundTrip() throws IOException {
        Int128Codec codec = new Int128Codec();
        assertRoundTrip(codec, signedMin(128), 16);   // -2^127
        assertRoundTrip(codec, signedMax(128), 16);   //  2^127 - 1
        assertRoundTrip(codec, BigInteger.ZERO, 16);
        assertRoundTrip(codec, BigInteger.valueOf(-1), 16);
    }

    @Test
    void int128_negativeOneIsAllFFLittleEndian() throws IOException {
        Int128Codec codec = new Int128Codec();
        byte[] wire = writeOne(codec, BigInteger.valueOf(-1));
        byte[] expected = new byte[16];
        java.util.Arrays.fill(expected, (byte) 0xFF); // two's complement -1 = all ones
        assertArrayEquals(expected, wire);
        assertEquals(BigInteger.valueOf(-1), readOne(codec, wire));
    }

    @Test
    void int128_minValueWireLayout() throws IOException {
        // -2^127 in two's complement = 0x80 followed by zeros (big-endian);
        // little-endian on the wire => 15 zero bytes then 0x80 as the last (MS) byte.
        Int128Codec codec = new Int128Codec();
        byte[] wire = writeOne(codec, signedMin(128));
        for (int i = 0; i < 15; i++) {
            assertEquals(0x00, wire[i] & 0xFF, "byte " + i);
        }
        assertEquals(0x80, wire[15] & 0xFF);
        assertEquals(signedMin(128), readOne(codec, wire));
    }

    @Test
    void int128_byteOrderRevealingValue() throws IOException {
        // BigInteger.valueOf(1) occupies only the lowest byte. In little-endian the
        // first wire byte must be 0x01 and the rest zero — this fails loudly if the
        // codec ever emits big-endian.
        Int128Codec codec = new Int128Codec();
        byte[] wire = writeOne(codec, BigInteger.ONE);
        assertEquals(0x01, wire[0] & 0xFF, "LE: value 1 -> first byte 0x01");
        for (int i = 1; i < 16; i++) {
            assertEquals(0x00, wire[i] & 0xFF, "byte " + i);
        }
        assertEquals(BigInteger.ONE, readOne(codec, wire));
    }

    @Test
    void int128_outOfRangeRejected() {
        Int128Codec codec = new Int128Codec();
        BigInteger[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, signedMax(128).add(BigInteger.ONE)));
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, signedMin(128).subtract(BigInteger.ONE)));
    }

    // =========================================================================
    // UInt128 (16 bytes, unsigned)
    // =========================================================================

    @Test
    void uint128_metadata() {
        assertEquals("UInt128", new UInt128Codec().typeName());
    }

    @Test
    void uint128_maxRoundTrips() throws IOException {
        // 2^128 - 1 must stay non-negative (uses BigInteger(1, beBytes) on read).
        UInt128Codec codec = new UInt128Codec();
        assertRoundTrip(codec, unsignedMax(128), 16);
        assertRoundTrip(codec, BigInteger.ZERO, 16);
    }

    @Test
    void uint128_signedUnsignedBoundary() throws IOException {
        // 2^127: high bit set. As Int128 this would read back negative (-2^127);
        // as UInt128 it must stay positive. This is THE signed↔unsigned divergence.
        UInt128Codec uCodec = new UInt128Codec();
        BigInteger twoPow127 = BigInteger.ONE.shiftLeft(127);
        byte[] wire = writeOne(uCodec, twoPow127);
        assertEquals(twoPow127, readOne(uCodec, wire), "UInt128 keeps 2^127 positive");

        // Same bytes through a signed Int128 read back as -2^127.
        Int128Codec sCodec = new Int128Codec();
        assertEquals(twoPow127.negate(), readOne(sCodec, wire),
                "identical bytes read as Int128 are negative");
    }

    @Test
    void uint128_outOfRangeRejected() {
        UInt128Codec codec = new UInt128Codec();
        BigInteger[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, BigInteger.valueOf(-1)));   // negative invalid for unsigned
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, unsignedMax(128).add(BigInteger.ONE)));
    }

    // =========================================================================
    // Int256 (32 bytes, signed)
    // =========================================================================

    @Test
    void int256_minMaxRoundTrip() throws IOException {
        Int256Codec codec = new Int256Codec();
        assertRoundTrip(codec, signedMin(256), 32);
        assertRoundTrip(codec, signedMax(256), 32);
        assertRoundTrip(codec, BigInteger.valueOf(-1), 32);
    }

    @Test
    void int256_byteOrderRevealingValue() throws IOException {
        // 0x0102030405060708 occupies only the low 8 bytes; little-endian => the first
        // 8 wire bytes ascend 08..01 and the remaining 24 bytes are zero.
        Int256Codec codec = new Int256Codec();
        byte[] wire = writeOne(codec, BigInteger.valueOf(0x0102030405060708L));
        byte[] expectedLow = {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01};
        for (int i = 0; i < 8; i++) {
            assertEquals(expectedLow[i], wire[i], "low byte " + i);
        }
        for (int i = 8; i < 32; i++) {
            assertEquals(0x00, wire[i] & 0xFF, "high byte " + i);
        }
        assertEquals(BigInteger.valueOf(0x0102030405060708L), readOne(codec, wire));
    }

    @Test
    void int256_negativeWireLayout() throws IOException {
        Int256Codec codec = new Int256Codec();

        // -1 in two's complement is all ones.
        byte[] negOne = writeOne(codec, BigInteger.valueOf(-1));
        byte[] allFF = new byte[32];
        java.util.Arrays.fill(allFF, (byte) 0xFF);
        assertArrayEquals(allFF, negOne);

        // -2^255 = 0x80 followed by zeros (big-endian); little-endian => 31 zeros then 0x80.
        byte[] min = writeOne(codec, signedMin(256));
        for (int i = 0; i < 31; i++) {
            assertEquals(0x00, min[i] & 0xFF, "byte " + i);
        }
        assertEquals(0x80, min[31] & 0xFF);
        assertEquals(signedMin(256), readOne(codec, min));
    }

    // =========================================================================
    // UInt256 (32 bytes, unsigned)
    // =========================================================================

    @Test
    void uint256_maxRoundTrips() throws IOException {
        UInt256Codec codec = new UInt256Codec();
        assertRoundTrip(codec, unsignedMax(256), 32);   // 2^256 - 1
        assertRoundTrip(codec, BigInteger.ZERO, 32);
    }

    @Test
    void uint256_signedUnsignedBoundary() throws IOException {
        // 2^255: high bit set. Stays positive under UInt256; the same bytes read back as
        // -2^255 under a signed Int256. Mirrors uint128_signedUnsignedBoundary.
        UInt256Codec uCodec = new UInt256Codec();
        BigInteger twoPow255 = BigInteger.ONE.shiftLeft(255);
        byte[] wire = writeOne(uCodec, twoPow255);
        assertEquals(twoPow255, readOne(uCodec, wire), "UInt256 keeps 2^255 positive");

        Int256Codec sCodec = new Int256Codec();
        assertEquals(twoPow255.negate(), readOne(sCodec, wire),
                "identical bytes read as Int256 are negative");
    }

    @Test
    void uint256_outOfRangeRejected() {
        UInt256Codec codec = new UInt256Codec();
        BigInteger[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, BigInteger.valueOf(-1)));            // negative invalid
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, unsignedMax(256).add(BigInteger.ONE))); // 2^256
    }
}
