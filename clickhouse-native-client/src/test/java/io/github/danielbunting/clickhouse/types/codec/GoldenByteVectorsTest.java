package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Golden-byte-vector tests: pin the <b>exact wire bytes</b> for representative values
 * of each ClickHouse type, asserting <b>both directions</b> against a frozen literal:
 *
 * <ol>
 *   <li><b>Encode</b> — the codec's {@code write} produces <em>exactly</em> the golden hex.</li>
 *   <li><b>Decode</b> — feeding the golden hex into {@code read} reconstructs the value.</li>
 * </ol>
 *
 * <p>Unlike a round-trip test ({@code write→read→assertEqual}), pinning the literal bytes
 * catches <em>symmetric</em> encode/decode bugs that a round-trip masks: a swapped byte order,
 * a wrong discriminator, or an off-by-one offset that the encoder and decoder agree on but the
 * server does not. The chosen values are deliberately endianness- and sign-revealing
 * (e.g. {@code 0x01020304}, sign-bit-set, all-ones).
 *
 * <h2>Provenance of the golden bytes</h2>
 * <p>Two classes of vector, marked per-test:
 * <ul>
 *   <li><b>HAND-COMPUTED</b> — deterministic little-endian layout of a fixed-width primitive,
 *       derivable by hand and re-checkable by eye. Each carries the arithmetic in a comment.
 *       These need no server.</li>
 *   <li><b>FROZEN-CAPTURE</b> — bytes observed once from the wire-format authority (the .NET
 *       {@code CH.Native} reference, cross-checked against a live {@code clickhouse-server} 25.8
 *       via {@code clickhouse-client --query "SELECT ... FORMAT Native"} / a one-shot capture
 *       harness) and pasted in as a hex literal with a provenance comment. Used where the layout
 *       is not trivially hand-derivable (composite/variable-length types). To regenerate, see
 *       {@code docs/} and the {@code // PROVENANCE:} comment on each such vector.</li>
 * </ul>
 *
 * <p>No running ClickHouse server is required to <em>run</em> this suite — every golden value is a
 * checked-in literal. All I/O is in-memory via {@link Bytes#reader(byte[])} / {@link Bytes#capture}.
 */
class GoldenByteVectorsTest {

    // =========================================================================
    // Local hex helpers. (Bytes only formats bytes→hex; we also need hex→bytes
    // and a symmetric assertion, so add tiny local helpers here.)
    // =========================================================================

    /** Decodes a lowercase/uppercase hex string (no separators) to bytes. */
    private static byte[] hex(String s) {
        int n = s.length();
        if ((n & 1) != 0) {
            throw new IllegalArgumentException("odd-length hex: " + s);
        }
        byte[] out = new byte[n / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /** Asserts {@code actual} equals the golden hex, with a readable hex diff on failure. */
    private static void assertHex(String goldenHex, byte[] actual) {
        byte[] expected = hex(goldenHex);
        assertArrayEquals(expected, actual,
                () -> "wire bytes mismatch:\n  expected " + goldenHex
                        + "\n  actual   " + Bytes.hex(actual));
    }

    /** Captures the bytes produced by a column codec writing {@code n} rows of {@code src}. */
    @SuppressWarnings("unchecked")
    private static <A> byte[] encode(io.github.danielbunting.clickhouse.types.ColumnCodec<A> codec,
                                     A src, int n) {
        return Bytes.capture(w -> ((io.github.danielbunting.clickhouse.types.ColumnCodec<A>) codec)
                .write(w, src, n));
    }

    // =========================================================================
    // Int32 — HAND-COMPUTED
    // Value 0x01020304 = 16909060. Little-endian => bytes ascend: 04 03 02 01.
    // This single value reveals byte order unambiguously (every byte distinct).
    // =========================================================================

    @Test
    void int32_byteOrderRevealing() throws IOException {
        Int32Codec codec = new Int32Codec();
        int[] src = {0x01020304};

        // ENCODE: 0x01020304 LE => 04 03 02 01
        byte[] wire = encode(codec, src, 1);
        assertHex("04030201", wire);

        // DECODE: same bytes back to the value
        int[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("04030201")), 1, dest);
        assertEquals(0x01020304, dest[0]);
    }

    @Test
    void int32_signBitSet() throws IOException {
        Int32Codec codec = new Int32Codec();
        // Integer.MIN_VALUE = 0x80000000 LE => 00 00 00 80 (sign bit in the last LE byte)
        byte[] wire = encode(codec, new int[]{Integer.MIN_VALUE}, 1);
        assertHex("00000080", wire);

        int[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("00000080")), 1, dest);
        assertEquals(Integer.MIN_VALUE, dest[0]);
    }

    // =========================================================================
    // Int64 — HAND-COMPUTED
    // 0x0102030405060708 LE => 08 07 06 05 04 03 02 01.
    // =========================================================================

    @Test
    void int64_byteOrderRevealing() throws IOException {
        Int64Codec codec = new Int64Codec();
        long[] src = {0x0102030405060708L};

        byte[] wire = encode(codec, src, 1);
        assertHex("0807060504030201", wire);

        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("0807060504030201")), 1, dest);
        assertEquals(0x0102030405060708L, dest[0]);
    }

    // =========================================================================
    // UInt64 — HAND-COMPUTED
    // All-ones (2^64-1) is stored as raw bits -1L => FF*8. Confirms no sign
    // narrowing on the unsigned path. Also pin 2^63 (Long.MIN_VALUE bits).
    // =========================================================================

    @Test
    void uint64_allOnes() throws IOException {
        UInt64Codec codec = new UInt64Codec();
        // unsigned 18446744073709551615 == raw bits 0xFFFFFFFFFFFFFFFF == -1L
        byte[] wire = encode(codec, new long[]{-1L}, 1);
        assertHex("ffffffffffffffff", wire);

        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("ffffffffffffffff")), 1, dest);
        assertEquals(-1L, dest[0]);
        assertEquals("18446744073709551615", Long.toUnsignedString(dest[0]));
    }

    @Test
    void uint64_twoToThe63() throws IOException {
        UInt64Codec codec = new UInt64Codec();
        // unsigned 2^63 == raw bits 0x8000000000000000 == Long.MIN_VALUE; LE => 00*7 80
        byte[] wire = encode(codec, new long[]{Long.MIN_VALUE}, 1);
        assertHex("0000000000000080", wire);

        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("0000000000000080")), 1, dest);
        assertEquals("9223372036854775808", Long.toUnsignedString(dest[0]));
    }

    // =========================================================================
    // String — HAND-COMPUTED
    // Wire: VarUInt byteLength + UTF-8 bytes, no trailing NUL.
    // "ABC": len 3 (VarUInt 0x03) + 41 42 43.
    // Multi-byte: "€" = E2 82 AC (3 bytes), len 0x03 => 03 e2 82 ac.
    // Empty string: just the zero-length prefix => 00.
    // =========================================================================

    @Test
    void string_ascii() throws IOException {
        StringColumnCodec codec = new StringColumnCodec();
        StringColumn src = codec.allocate(1);
        codec.set(src, 0, "ABC");

        // ENCODE: 03 'A' 'B' 'C'
        byte[] wire = encode(codec, src, 1);
        assertHex("03414243", wire);

        // DECODE
        StringColumn dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("03414243")), 1, dest);
        assertEquals("ABC", codec.get(dest, 0));
    }

    @Test
    void string_emptyAndMultibyte() throws IOException {
        StringColumnCodec codec = new StringColumnCodec();
        StringColumn src = codec.allocate(2);
        codec.set(src, 0, "");
        codec.set(src, 1, "€"); // U+20AC => UTF-8 E2 82 AC

        // ENCODE: 00  |  03 e2 82 ac
        byte[] wire = encode(codec, src, 2);
        assertHex("0003e282ac", wire);

        StringColumn dest = codec.allocate(2);
        codec.read(Bytes.reader(hex("0003e282ac")), 2, dest);
        assertEquals("", codec.get(dest, 0));
        assertEquals("€", codec.get(dest, 1));
    }

    // =========================================================================
    // Date — HAND-COMPUTED
    // UInt16 days since 1970-01-01. 2000-01-01 => epochDay 10957 = 0x2ACD;
    // LE => CD 2A. (Verify: LocalDate.of(2000,1,1).toEpochDay() == 10957.)
    // =========================================================================

    @Test
    void date_y2k() throws IOException {
        DateCodec codec = new DateCodec();
        int[] src = codec.allocate(1);
        codec.set(src, 0, LocalDate.of(2000, 1, 1)); // epochDay 10957 == 0x2ACD

        // ENCODE: 0x2ACD LE => cd 2a
        byte[] wire = encode(codec, src, 1);
        assertHex("cd2a", wire);

        DateCodec dec = new DateCodec();
        int[] dest = dec.allocate(1);
        dec.read(Bytes.reader(hex("cd2a")), 1, dest);
        assertEquals(LocalDate.of(2000, 1, 1), dec.get(dest, 0));
    }

    @Test
    void date_epoch() throws IOException {
        DateCodec codec = new DateCodec();
        int[] src = codec.allocate(1);
        codec.set(src, 0, LocalDate.of(1970, 1, 1)); // epochDay 0

        byte[] wire = encode(codec, src, 1);
        assertHex("0000", wire);
    }

    // =========================================================================
    // Remaining fixed-width primitives — HAND-COMPUTED
    // =========================================================================

    @Test
    void int8_boundaries() throws IOException {
        Int8Codec codec = new Int8Codec();
        assertHex("7f", encode(codec, new byte[]{0x7F}, 1));       // Byte.MAX_VALUE
        assertHex("80", encode(codec, new byte[]{(byte) -128}, 1)); // Byte.MIN_VALUE
        byte[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("80")), 1, dest);
        assertEquals((byte) -128, dest[0]);
    }

    @Test
    void int16_byteOrderAndSignBit() throws IOException {
        Int16Codec codec = new Int16Codec();
        // 0x0102 LE => 02 01
        assertHex("0201", encode(codec, new short[]{0x0102}, 1));
        // Short.MIN_VALUE = 0x8000 LE => 00 80
        assertHex("0080", encode(codec, new short[]{Short.MIN_VALUE}, 1));
        short[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("0201")), 1, dest);
        assertEquals((short) 0x0102, dest[0]);
    }

    @Test
    void unsignedSmallInts_maxima() throws IOException {
        assertHex("ff", encode(new UInt8Codec(), new int[]{255}, 1));
        assertHex("ffff", encode(new UInt16Codec(), new int[]{65535}, 1));
        assertHex("ffffffff", encode(new UInt32Codec(), new long[]{4294967295L}, 1));
    }

    @Test
    void uint32_byteOrderRevealing() throws IOException {
        UInt32Codec codec = new UInt32Codec();
        // 0x01020304 LE => 04 03 02 01
        assertHex("04030201", encode(codec, new long[]{0x01020304L}, 1));
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("04030201")), 1, dest);
        assertEquals(0x01020304L, dest[0]);
    }

    @Test
    void float32_negativeAndPositiveOne() throws IOException {
        Float32Codec codec = new Float32Codec();
        // -1.0f = 0xBF800000 LE => 00 00 80 bf ; 1.0f = 0x3F800000 LE => 00 00 80 3f
        assertHex("000080bf", encode(codec, new float[]{-1.0f}, 1));
        assertHex("0000803f", encode(codec, new float[]{1.0f}, 1));
        float[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("000080bf")), 1, dest);
        assertEquals(-1.0f, dest[0]);
    }

    @Test
    void float64_negativeOne() throws IOException {
        Float64Codec codec = new Float64Codec();
        // -1.0 = 0xBFF0000000000000 LE => 00 00 00 00 00 00 f0 bf
        assertHex("000000000000f0bf", encode(codec, new double[]{-1.0}, 1));
        double[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("000000000000f0bf")), 1, dest);
        assertEquals(-1.0, dest[0]);
    }

    @Test
    void bool_trueFalse() throws IOException {
        BoolCodec codec = new BoolCodec();
        byte[] src = codec.allocate(2);
        codec.set(src, 0, true);
        codec.set(src, 1, false);
        assertHex("0100", encode(codec, src, 2));
        byte[] dest = codec.allocate(2);
        codec.read(Bytes.reader(hex("0100")), 2, dest);
        assertEquals(Boolean.TRUE, codec.get(dest, 0));
        assertEquals(Boolean.FALSE, codec.get(dest, 1));
    }

    // =========================================================================
    // Temporal — HAND-COMPUTED
    // =========================================================================

    @Test
    void date32_preEpochNegativeDay() throws IOException {
        // Date32 is a signed Int32 day-offset; 1969-12-31 = -1 => two's complement LE "ffffffff".
        Date32Codec codec = new Date32Codec();
        int[] src = codec.allocate(1);
        codec.set(src, 0, LocalDate.of(1969, 12, 31));
        assertHex("ffffffff", encode(codec, src, 1));
        int[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("ffffffff")), 1, dest);
        assertEquals(LocalDate.of(1969, 12, 31), codec.get(dest, 0));
    }

    @Test
    void dateTime_epochSeconds() throws IOException {
        // UInt32 epoch-seconds. 2009-02-13T23:31:30Z = 1234567890 = 0x499602D2 LE => d2 02 96 49.
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        long[] src = codec.allocate(1);
        codec.set(src, 0, Instant.ofEpochSecond(1234567890L));
        assertHex("d2029649", encode(codec, src, 1));
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("d2029649")), 1, dest);
        assertEquals(Instant.ofEpochSecond(1234567890L), codec.get(dest, 0));
    }

    @Test
    void dateTime64_millisTick() throws IOException {
        // DateTime64(3): Int64 ms ticks. 1970-01-01T00:00:00.001Z = 1 tick => "01" + 00*7.
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        long[] src = codec.allocate(1);
        codec.set(src, 0, Instant.ofEpochSecond(0, 1_000_000)); // 1 ms
        assertHex("01" + "00".repeat(7), encode(codec, src, 1));
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("01" + "00".repeat(7))), 1, dest);
        assertEquals(Instant.ofEpochSecond(0, 1_000_000), codec.get(dest, 0));
    }

    // =========================================================================
    // Decimal — HAND-COMPUTED (Decimal32 / Decimal64).
    // The 16- and 32-byte Decimal128/256 layouts are exercised by round-trip in
    // DecimalEdgeCasesTest/B6CodecsTest; their exact-byte vectors are intentionally
    // not pinned by hand here (error-prone wide two's-complement).
    // =========================================================================

    @Test
    void decimal32_oneHundredth() throws IOException {
        // Decimal(9,2) 1.00 -> unscaled 100 = 0x64 -> Int32 LE "64000000".
        DecimalCodec codec = new DecimalCodec(9, 2);
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("1.00"));
        assertHex("64000000", encode(codec, src, 1));
        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("64000000")), 1, dest);
        assertEquals(new BigDecimal("1.00"), codec.get(dest, 0));
    }

    @Test
    void decimal64_negative() throws IOException {
        // Decimal(18,4) -1.0000 -> unscaled -10000 = 0xFFFFFFFFFFFFD8F0 -> Int64 LE "f0d8ffffffffffff".
        DecimalCodec codec = new DecimalCodec(18, 4);
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("-1.0000"));
        assertHex("f0d8ffffffffffff", encode(codec, src, 1));
        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("f0d8ffffffffffff")), 1, dest);
        assertEquals(new BigDecimal("-1.0000"), codec.get(dest, 0));
    }

    // =========================================================================
    // Wide integers — HAND-COMPUTED (two's-complement LE)
    // =========================================================================

    @Test
    void int128_oneAndNegativeOne() throws IOException {
        Int128Codec codec = new Int128Codec();
        // 1 occupies only the lowest LE byte.
        assertHex("01" + "00".repeat(15), encode(codec, new BigInteger[]{BigInteger.ONE}, 1));
        // -1 two's complement = all ones.
        assertHex("ff".repeat(16), encode(codec, new BigInteger[]{BigInteger.valueOf(-1)}, 1));
        BigInteger[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("ff".repeat(16))), 1, dest);
        assertEquals(BigInteger.valueOf(-1), codec.get(dest, 0));
    }

    @Test
    void uint256_maxAndOne() throws IOException {
        UInt256Codec codec = new UInt256Codec();
        BigInteger max = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
        assertHex("ff".repeat(32), encode(codec, new BigInteger[]{max}, 1));
        assertHex("01" + "00".repeat(31), encode(codec, new BigInteger[]{BigInteger.ONE}, 1));
        BigInteger[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("ff".repeat(32))), 1, dest);
        assertEquals(max, codec.get(dest, 0));
    }

    // =========================================================================
    // FixedString / Enum / UUID / IP — HAND-COMPUTED
    // =========================================================================

    @Test
    void fixedString_nulPadded() throws IOException {
        // FixedString(4) "hi" -> 'h' 'i' NUL NUL -> "68690000".
        FixedStringCodec codec = new FixedStringCodec(4);
        Object[] src = codec.allocate(1);
        codec.set(src, 0, "hi");
        assertHex("68690000", encode(codec, src, 1));
        Object[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("68690000")), 1, dest);
        assertEquals("hi", codec.get(dest, 0));
    }

    @Test
    void enum8_negativeOrdinal() throws IOException {
        // Enum8 ordinal -1 is a signed Int8 => "ff".
        Enum8Codec codec = new Enum8Codec(Map.of(-1, "neg"));
        int[] src = codec.allocate(1);
        src[0] = -1;
        assertHex("ff", encode(codec, src, 1));
        int[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("ff")), 1, dest);
        assertEquals("neg", codec.get(dest, 0));
    }

    @Test
    void enum16_ordinal256() throws IOException {
        // Enum16 ordinal 256 = 0x0100 LE => "0001".
        Enum16Codec codec = new Enum16Codec(Map.of(256, "hi"));
        int[] src = codec.allocate(1);
        src[0] = 256;
        assertHex("0001", encode(codec, src, 1));
    }

    @Test
    void uuid_wellKnownBytes() throws IOException {
        // Two LE UInt64 halves, MSB half first: aabbccdd-eeff-1122-3344-556677889900.
        UuidCodec codec = new UuidCodec();
        UUID[] src = codec.allocate(1);
        src[0] = new UUID(0xAABBCCDDEEFF1122L, 0x3344556677889900L);
        assertHex("2211ffeeddccbbaa0099887766554433", encode(codec, src, 1));
        UUID[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("2211ffeeddccbbaa0099887766554433")), 1, dest);
        assertEquals(src[0], dest[0]);
    }

    @Test
    void ipv4_dottedQuad() throws IOException {
        // 192.168.1.1 = 0xC0A80101 written as UInt32 LE => "0101a8c0". (Byte order confirmed
        // against a live server — see Ipv4Codec javadoc.)
        Ipv4Codec codec = new Ipv4Codec();
        long[] src = codec.allocate(1);
        codec.set(src, 0, "192.168.1.1");
        assertHex("0101a8c0", encode(codec, src, 1));
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("0101a8c0")), 1, dest);
        assertEquals(java.net.InetAddress.getByName("192.168.1.1"), codec.get(dest, 0));
    }

    @Test
    void ipv6_loopback() throws IOException {
        // IPv6 is 16 raw bytes in network (big-endian) order, NOT reversed. ::1 => 00*15 + 01.
        Ipv6Codec codec = new Ipv6Codec();
        Object[] src = codec.allocate(1);
        codec.set(src, 0, "::1");
        assertHex("00".repeat(15) + "01", encode(codec, src, 1));
        Object[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("00".repeat(15) + "01")), 1, dest);
        assertEquals(java.net.InetAddress.getByName("::1"), codec.get(dest, 0));
    }

    // =========================================================================
    // Composite — HAND-COMPUTED layout (offsets + flattened element vectors)
    // =========================================================================

    @Test
    void array_uint32_offsetsThenValues() throws IOException {
        // Array(UInt32) [[1,2],[3]]:
        //   offsets = 2 x UInt64 cumulative end-offsets [2,3]
        //   values  = 3 x UInt32 LE [1,2,3]
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] src = codec.allocate(2);
        src[0] = List.of(1L, 2L);
        src[1] = List.of(3L);
        String golden = "0200000000000000" + "0300000000000000"   // offsets 2, 3
                + "01000000" + "02000000" + "03000000";           // values 1, 2, 3
        assertHex(golden, encode(codec, src, 2));

        Object[] dest = codec.allocate(2);
        codec.read(Bytes.reader(hex(golden)), 2, dest);
        assertEquals(List.of(1L, 2L), dest[0]);
        assertEquals(List.of(3L), dest[1]);
    }

    @Test
    void map_stringUint32_offsetsKeysValues() throws IOException {
        // Map(String,UInt32) {"a":1,"b":2} (one row): offsets [2], then flattened keys
        // (String column "a","b"), then flattened values (UInt32 column 1,2).
        MapColumnCodec codec = new MapColumnCodec(new StringColumnCodec(), new UInt32Codec());
        MapColumn src = codec.allocate(1);
        LinkedHashMap<String, Long> m = new LinkedHashMap<>();
        m.put("a", 1L);
        m.put("b", 2L);
        codec.set(src, 0, m);
        String golden = "0200000000000000"      // offset 2
                + "0161" + "0162"                // keys "a","b" (varint len + byte)
                + "01000000" + "02000000";       // values 1, 2
        assertHex(golden, encode(codec, src, 1));

        MapColumn dest = codec.allocate(1);
        codec.read(Bytes.reader(hex(golden)), 1, dest);
        assertEquals(m, codec.get(dest, 0));
    }

    @Test
    void tuple_int32String_columnsBackToBack() throws IOException {
        // Tuple(Int32, String) [7, "x"]: the Int32 sub-column then the String sub-column,
        // back-to-back with no per-row interleaving.
        TupleColumnCodec codec = new TupleColumnCodec(
                new ColumnCodec<?>[]{new Int32Codec(), new StringColumnCodec()}, null);
        TupleColumn src = codec.allocate(1);
        codec.set(src, 0, List.of(7, "x"));
        assertHex("07000000" + "0178", encode(codec, src, 1)); // Int32 7 | String "x"

        TupleColumn dest = codec.allocate(1);
        codec.read(Bytes.reader(hex("07000000" + "0178")), 1, dest);
        assertEquals(List.of(7, "x"), codec.get(dest, 0));
    }

    @Test
    void array_nullableNothing_oneDummyBytePerElement() throws IOException {
        // Array(Nullable(Nothing)) [[NULL]] — the server's own type for a NULL-only
        // array literal (SELECT [NULL]). SerializationNothing is ONE dummy (zero) byte
        // per value (serializeBinaryBulk writes n zero bytes, deserializeBinaryBulk
        // skips n) — verified against a live 26.5 server via the Dynamic encode
        // round-trip in DynamicTypesIT. Zero bytes here desyncs the whole stream.
        //   offsets  = 1 x UInt64 cumulative end-offset [1]
        //   null map = 1 byte 01 (element is NULL)
        //   values   = 1 dummy Nothing byte 00
        ArrayColumnCodec codec =
                new ArrayColumnCodec(new NullableColumnCodec(new NothingCodec()));
        Object[] src = codec.allocate(1);
        src[0] = java.util.Collections.singletonList(null);
        String golden = "0100000000000000" // offset 1
                + "01"                      // null map: [true]
                + "00";                     // Nothing: one dummy byte
        assertHex(golden, encode(codec, src, 1));

        Object[] dest = codec.allocate(1);
        codec.read(Bytes.reader(hex(golden)), 1, dest);
        assertEquals(java.util.Collections.singletonList(null), dest[0]);
    }

    // --- Types where a stable golden vector is IMPRACTICAL (documented, not forced) ---
    //
    // Nullable(T): the null-map is a block-layer concern, not part of the codec —
    //   DefaultTypeParser.parse("Nullable(T)") returns the inner T codec. A golden vector
    //   for the null-map byte[] belongs in a Block/NullMaps golden test, not here.
    //
    // LowCardinality / Variant / Dynamic / JSON: their wire form carries a serialization
    //   state prefix, dictionary/discriminator framing, and (for Dynamic/JSON) ordering that
    //   is version- and flag-dependent (output_format_native_use_flattened_dynamic_and_json_
    //   serialization). A hand-computed literal would be brittle; these are covered end-to-end
    //   by the live IT suites (VariantTypesIT / DynamicTypesIT / JsonTypesIT) and the
    //   LowCardinalityKeyWidthTest codec round-trips instead.
}
