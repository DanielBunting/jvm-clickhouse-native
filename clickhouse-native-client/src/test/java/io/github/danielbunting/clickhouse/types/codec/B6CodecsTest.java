package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * Round-trip and wire-format tests for the B6 leaf codecs:
 * {@link FixedStringCodec}, {@link Enum8Codec}, {@link Enum16Codec},
 * {@link UuidCodec}, and {@link DecimalCodec}.
 *
 * <p>No running ClickHouse server is required; all tests operate on in-memory
 * byte vectors produced and consumed by the {@link Bytes} test utility.
 */
class B6CodecsTest {

    // ==========================================================================
    // FixedStringCodec
    // ==========================================================================

    @Test
    void fixedString_typeName() {
        assertEquals("FixedString(8)", new FixedStringCodec(8).typeName());
        assertEquals("FixedString(1)", new FixedStringCodec(1).typeName());
    }

    @Test
    void fixedString_javaType() {
        assertEquals(String.class, new FixedStringCodec(4).javaType());
    }

    @Test
    void fixedString_allocate() {
        Object[] arr = new FixedStringCodec(4).allocate(3);
        assertNotNull(arr);
        assertEquals(3, arr.length);
    }

    @Test
    void fixedString_illegalLength() {
        assertThrows(IllegalArgumentException.class, () -> new FixedStringCodec(0));
        assertThrows(IllegalArgumentException.class, () -> new FixedStringCodec(-1));
    }

    @Test
    void fixedString_roundTripAscii() throws IOException {
        FixedStringCodec codec = new FixedStringCodec(8);
        Object[] src = codec.allocate(3);
        codec.set(src, 0, "hello");
        codec.set(src, 1, "hi");
        codec.set(src, 2, "world!!!");

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Each value is exactly 8 bytes on the wire
        assertEquals(24, wire.length);

        Object[] dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);

        assertEquals("hello", dest[0]);
        assertEquals("hi", dest[1]);
        assertEquals("world!!!", dest[2]);
    }

    @Test
    void fixedString_wireLayoutZeroPadded() throws IOException {
        // "hi" in FixedString(4) should be padded to [0x68, 0x69, 0x00, 0x00]
        FixedStringCodec codec = new FixedStringCodec(4);
        Object[] src = codec.allocate(1);
        codec.set(src, 0, "hi");

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(4, wire.length);
        assertEquals(0x68, wire[0] & 0xFF);  // 'h'
        assertEquals(0x69, wire[1] & 0xFF);  // 'i'
        assertEquals(0x00, wire[2] & 0xFF);  // NUL pad
        assertEquals(0x00, wire[3] & 0xFF);  // NUL pad
    }

    @Test
    void fixedString_readStripsTrailingNuls() throws IOException {
        // Wire bytes: "AB\x00\x00" should read back as "AB"
        byte[] wire = new byte[]{'A', 'B', 0x00, 0x00};
        FixedStringCodec codec = new FixedStringCodec(4);
        Object[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals("AB", dest[0]);
    }

    @Test
    void fixedString_emptyStringIsAllNuls() throws IOException {
        FixedStringCodec codec = new FixedStringCodec(4);
        Object[] src = codec.allocate(1);
        codec.set(src, 0, "");

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertArrayEquals(new byte[]{0x00, 0x00, 0x00, 0x00}, wire);

        Object[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals("", dest[0]);
    }

    @Test
    void fixedString_emptyColumn() throws IOException {
        FixedStringCodec codec = new FixedStringCodec(4);
        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, codec.allocate(0), 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertArrayEquals(new byte[0], wire);
    }

    @Test
    void fixedString_getAndSet() {
        FixedStringCodec codec = new FixedStringCodec(8);
        Object[] arr = codec.allocate(2);
        codec.set(arr, 0, "alpha");
        codec.set(arr, 1, "beta");
        assertEquals("alpha", codec.get(arr, 0));
        assertEquals("beta", codec.get(arr, 1));
    }

    @Test
    void fixedString_multibyteUtf8() throws IOException {
        // "€" is 3 bytes in UTF-8; FixedString(4) can hold it with 1 NUL pad
        FixedStringCodec codec = new FixedStringCodec(4);
        Object[] src = codec.allocate(1);
        codec.set(src, 0, "€");

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(4, wire.length);
        // 0xE2 0x82 0xAC 0x00
        assertEquals(0xE2, wire[0] & 0xFF);
        assertEquals(0x82, wire[1] & 0xFF);
        assertEquals(0xAC, wire[2] & 0xFF);
        assertEquals(0x00, wire[3] & 0xFF);

        Object[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals("€", dest[0]);
    }

    // ==========================================================================
    // Enum8Codec
    // ==========================================================================

    private static final Map<Integer, String> ENUM8_MAP =
            Map.of(1, "active", 2, "inactive", 3, "pending");

    @Test
    void enum8_typeName() {
        assertEquals("Enum8", new Enum8Codec(ENUM8_MAP).typeName());
    }

    @Test
    void enum8_javaType() {
        assertEquals(String.class, new Enum8Codec(ENUM8_MAP).javaType());
    }

    @Test
    void enum8_allocate() {
        int[] arr = new Enum8Codec(ENUM8_MAP).allocate(5);
        assertEquals(5, arr.length);
    }

    @Test
    void enum8_roundTrip() throws IOException {
        Enum8Codec codec = new Enum8Codec(ENUM8_MAP);
        int[] src = codec.allocate(3);
        src[0] = 1;
        src[1] = 3;
        src[2] = 2;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // 3 rows, 1 byte each
        assertEquals(3, wire.length);
        assertEquals(1, wire[0]);
        assertEquals(3, wire[1]);
        assertEquals(2, wire[2]);

        int[] dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);
        assertArrayEquals(src, dest);
    }

    @Test
    void enum8_getReturnsStringName() {
        Enum8Codec codec = new Enum8Codec(ENUM8_MAP);
        int[] arr = codec.allocate(3);
        arr[0] = 1;
        arr[1] = 2;
        arr[2] = 3;
        assertEquals("active", codec.get(arr, 0));
        assertEquals("inactive", codec.get(arr, 1));
        assertEquals("pending", codec.get(arr, 2));
    }

    @Test
    void enum8_setByName() {
        Enum8Codec codec = new Enum8Codec(ENUM8_MAP);
        int[] arr = codec.allocate(3);
        codec.set(arr, 0, "active");
        codec.set(arr, 1, "inactive");
        codec.set(arr, 2, "pending");
        assertEquals(1, arr[0]);
        assertEquals(2, arr[1]);
        assertEquals(3, arr[2]);
    }

    @Test
    void enum8_setByNumber() {
        Enum8Codec codec = new Enum8Codec(ENUM8_MAP);
        int[] arr = codec.allocate(1);
        codec.set(arr, 0, 2);
        assertEquals(2, arr[0]);
    }

    @Test
    void enum8_setUnknownNameThrows() {
        Enum8Codec codec = new Enum8Codec(ENUM8_MAP);
        int[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, "unknown"));
    }

    @Test
    void enum8_wireFormatNegativeOrdinal() throws IOException {
        // Enum8 allows negative Int8 ordinals (signed byte)
        Map<Integer, String> map = Map.of(-1, "neg", 0, "zero");
        Enum8Codec codec = new Enum8Codec(map);
        int[] src = codec.allocate(2);
        src[0] = -1;
        src[1] = 0;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(2, wire.length);
        assertEquals((byte) -1, wire[0]);
        assertEquals((byte) 0, wire[1]);

        int[] dest = codec.allocate(2);
        codec.read(Bytes.reader(wire), 2, dest);
        assertEquals(-1, dest[0]);
        assertEquals(0, dest[1]);
        assertEquals("neg", codec.get(dest, 0));
        assertEquals("zero", codec.get(dest, 1));
    }

    // ==========================================================================
    // Enum16Codec
    // ==========================================================================

    private static final Map<Integer, String> ENUM16_MAP =
            Map.of(100, "alpha", 200, "beta", 300, "gamma", -32768, "min");

    @Test
    void enum16_typeName() {
        assertEquals("Enum16", new Enum16Codec(ENUM16_MAP).typeName());
    }

    @Test
    void enum16_javaType() {
        assertEquals(String.class, new Enum16Codec(ENUM16_MAP).javaType());
    }

    @Test
    void enum16_roundTrip() throws IOException {
        Enum16Codec codec = new Enum16Codec(ENUM16_MAP);
        int[] src = codec.allocate(3);
        src[0] = 100;
        src[1] = 300;
        src[2] = -32768;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // 3 rows × 2 bytes each = 6 bytes
        assertEquals(6, wire.length);

        int[] dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);
        assertArrayEquals(src, dest);
    }

    @Test
    void enum16_getAndSetByName() {
        Enum16Codec codec = new Enum16Codec(ENUM16_MAP);
        int[] arr = codec.allocate(2);
        codec.set(arr, 0, "alpha");
        codec.set(arr, 1, "gamma");
        assertEquals(100, arr[0]);
        assertEquals(300, arr[1]);
        assertEquals("alpha", codec.get(arr, 0));
        assertEquals("gamma", codec.get(arr, 1));
    }

    @Test
    void enum16_wireLayout() throws IOException {
        // Value 256 in LE Int16 = [0x00, 0x01]
        Map<Integer, String> map = Map.of(256, "high");
        Enum16Codec codec = new Enum16Codec(map);
        int[] src = codec.allocate(1);
        src[0] = 256;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(2, wire.length);
        assertEquals(0x00, wire[0] & 0xFF);  // low byte
        assertEquals(0x01, wire[1] & 0xFF);  // high byte
    }

    @Test
    void enum16_minOrdinal() throws IOException {
        // -32768 in LE Int16 = [0x00, 0x80]
        Map<Integer, String> map = Map.of(-32768, "min");
        Enum16Codec codec = new Enum16Codec(map);
        int[] src = codec.allocate(1);
        src[0] = -32768;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(2, wire.length);
        assertEquals(0x00, wire[0] & 0xFF);
        assertEquals(0x80, wire[1] & 0xFF);

        int[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(-32768, dest[0]);
    }

    @Test
    void enum16_setUnknownNameThrows() {
        Enum16Codec codec = new Enum16Codec(ENUM16_MAP);
        int[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, "nope"));
    }

    // ==========================================================================
    // UuidCodec
    // ==========================================================================

    @Test
    void uuid_typeName() {
        assertEquals("UUID", new UuidCodec().typeName());
    }

    @Test
    void uuid_javaType() {
        assertEquals(UUID.class, new UuidCodec().javaType());
    }

    @Test
    void uuid_allocate() {
        UUID[] arr = new UuidCodec().allocate(4);
        assertEquals(4, arr.length);
    }

    @Test
    void uuid_roundTrip() throws IOException {
        UuidCodec codec = new UuidCodec();
        UUID[] src = codec.allocate(3);
        src[0] = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        src[1] = UUID.fromString("00000000-0000-0000-0000-000000000000");
        src[2] = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // 3 UUIDs × 16 bytes each = 48 bytes
        assertEquals(48, wire.length);

        UUID[] dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);

        assertEquals(src[0], dest[0]);
        assertEquals(src[1], dest[1]);
        assertEquals(src[2], dest[2]);
    }

    @Test
    void uuid_wireLayoutMsbFirst() throws IOException {
        // UUID 00000000-0000-0001-0000-000000000002
        // MSB = 0x0000000000000001L, LSB = 0x0000000000000002L
        UUID uuid = new UUID(0x0000000000000001L, 0x0000000000000002L);
        UuidCodec codec = new UuidCodec();
        UUID[] src = codec.allocate(1);
        src[0] = uuid;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(16, wire.length);

        // MSB half written first as LE UInt64: 1L -> [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        ByteBuffer bb = ByteBuffer.wrap(wire).order(ByteOrder.LITTLE_ENDIAN);
        long readMsb = bb.getLong(0);
        long readLsb = bb.getLong(8);
        assertEquals(0x0000000000000001L, readMsb);
        assertEquals(0x0000000000000002L, readLsb);
    }

    @Test
    void uuid_wellKnownWireBytes() throws IOException {
        // UUID aabbccdd-eeff-1122-3344-556677889900
        // MSB = 0xAABBCCDDEEFF1122L
        // LSB = 0x3344556677889900L
        UUID uuid = new UUID(0xAABBCCDDEEFF1122L, 0x3344556677889900L);
        UuidCodec codec = new UuidCodec();
        UUID[] src = codec.allocate(1);
        src[0] = uuid;

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Read back via reader
        BinaryReader reader = Bytes.reader(wire);
        long msb = reader.readUInt64();
        long lsb = reader.readUInt64();
        assertEquals(0xAABBCCDDEEFF1122L, msb);
        assertEquals(0x3344556677889900L, lsb);

        UUID[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(uuid, dest[0]);
    }

    @Test
    void uuid_setFromString() {
        UuidCodec codec = new UuidCodec();
        UUID[] arr = codec.allocate(1);
        codec.set(arr, 0, "550e8400-e29b-41d4-a716-446655440000");
        assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), arr[0]);
    }

    @Test
    void uuid_setInvalidTypeThrows() {
        UuidCodec codec = new UuidCodec();
        UUID[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, 42));
    }

    @Test
    void uuid_emptyColumn() throws IOException {
        UuidCodec codec = new UuidCodec();
        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, codec.allocate(0), 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertArrayEquals(new byte[0], wire);
    }

    @Test
    void uuid_randomUuidRoundTrip() throws IOException {
        UuidCodec codec = new UuidCodec();
        UUID[] src = codec.allocate(10);
        for (int i = 0; i < 10; i++) {
            src[i] = UUID.randomUUID();
        }

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 10);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(160, wire.length);

        UUID[] dest = codec.allocate(10);
        codec.read(Bytes.reader(wire), 10, dest);
        for (int i = 0; i < 10; i++) {
            assertEquals(src[i], dest[i]);
        }
    }

    // ==========================================================================
    // DecimalCodec
    // ==========================================================================

    @Test
    void decimal_typeNames() {
        assertEquals("Decimal(9, 2)", new DecimalCodec(9, 2).typeName());
        assertEquals("Decimal(18, 6)", new DecimalCodec(18, 6).typeName());
        assertEquals("Decimal(38, 10)", new DecimalCodec(38, 10).typeName());
    }

    @Test
    void decimal_javaType() {
        assertEquals(BigDecimal.class, new DecimalCodec(9, 2).javaType());
    }

    @Test
    void decimal_illegalArgs() {
        assertThrows(IllegalArgumentException.class, () -> new DecimalCodec(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new DecimalCodec(77, 0));
        assertThrows(IllegalArgumentException.class, () -> new DecimalCodec(9, 10));  // scale > precision
        assertThrows(IllegalArgumentException.class, () -> new DecimalCodec(9, -1));
    }

    @Test
    void decimal_byteWidths() {
        assertEquals(4,  new DecimalCodec(9, 2).byteWidth());
        assertEquals(8,  new DecimalCodec(18, 6).byteWidth());
        assertEquals(16, new DecimalCodec(38, 10).byteWidth());
        assertEquals(32, new DecimalCodec(76, 20).byteWidth());
    }

    // --- Decimal32 (P<=9, 4 bytes) ---

    @Test
    void decimal32_roundTripBasic() throws IOException {
        DecimalCodec codec = new DecimalCodec(9, 2);
        // Unscaled value for 123.45 = 12345
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("123.45"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(4, wire.length);  // Int32 on wire

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(new BigDecimal("123.45"), codec.get(dest, 0));
    }

    @Test
    void decimal32_negativeValue() throws IOException {
        DecimalCodec codec = new DecimalCodec(9, 3);
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("-999.999"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(new BigDecimal("-999.999"), codec.get(dest, 0));
    }

    @Test
    void decimal32_zeroValue() throws IOException {
        DecimalCodec codec = new DecimalCodec(9, 4);
        Object src = codec.allocate(1);
        codec.set(src, 0, BigDecimal.ZERO);

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertArrayEquals(new byte[]{0x00, 0x00, 0x00, 0x00}, wire);

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0)).compareTo(BigDecimal.ZERO));
    }

    @Test
    void decimal32_wireLayout() throws IOException {
        // 1.00 with scale 2 => unscaled=100 => LE Int32 [0x64, 0x00, 0x00, 0x00]
        DecimalCodec codec = new DecimalCodec(9, 2);
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("1.00"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertArrayEquals(new byte[]{0x64, 0x00, 0x00, 0x00}, wire);
    }

    @Test
    void decimal32_scaleEdgeCase_scaleZero() throws IOException {
        // Scale=0, integer values only
        DecimalCodec codec = new DecimalCodec(9, 0);
        Object src = codec.allocate(2);
        codec.set(src, 0, new BigDecimal("42"));
        codec.set(src, 1, new BigDecimal("-1"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Object dest = codec.allocate(2);
        codec.read(Bytes.reader(wire), 2, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0)).compareTo(new BigDecimal("42")));
        assertEquals(0, ((BigDecimal) codec.get(dest, 1)).compareTo(new BigDecimal("-1")));
    }

    // --- Decimal64 (10 <= P <= 18, 8 bytes) ---

    @Test
    void decimal64_roundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(18, 6);
        Object src = codec.allocate(2);
        codec.set(src, 0, new BigDecimal("123456789.123456"));
        codec.set(src, 1, new BigDecimal("-0.000001"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(16, wire.length);  // 2 × 8 bytes

        Object dest = codec.allocate(2);
        codec.read(Bytes.reader(wire), 2, dest);
        assertEquals(new BigDecimal("123456789.123456"), codec.get(dest, 0));
        assertEquals(new BigDecimal("-0.000001"), codec.get(dest, 1));
    }

    @Test
    void decimal64_maxPrecision() throws IOException {
        DecimalCodec codec = new DecimalCodec(18, 0);
        // Max Int64 ~ 9.2e18, fits in P=18 int representation
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("999999999999999999"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0)).compareTo(new BigDecimal("999999999999999999")));
    }

    // --- Decimal128 (19 <= P <= 38, 16 bytes) ---

    @Test
    void decimal128_roundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(38, 10);
        Object src = codec.allocate(2);
        // A value requiring more than 64 bits
        codec.set(src, 0, new BigDecimal("99999999999999999999.1234567890"));
        codec.set(src, 1, new BigDecimal("-1.0000000001"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(32, wire.length);  // 2 × 16 bytes

        Object dest = codec.allocate(2);
        codec.read(Bytes.reader(wire), 2, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0)).compareTo(new BigDecimal("99999999999999999999.1234567890")));
        assertEquals(0, ((BigDecimal) codec.get(dest, 1)).compareTo(new BigDecimal("-1.0000000001")));
    }

    @Test
    void decimal128_zeroRoundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(38, 10);
        Object src = codec.allocate(1);
        codec.set(src, 0, BigDecimal.ZERO);

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(16, wire.length);
        // All zeros
        for (byte b : wire) {
            assertEquals(0, b);
        }

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0)).compareTo(BigDecimal.ZERO));
    }

    @Test
    void decimal128_negativeRoundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(38, 5);
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("-12345678901234567890.12345"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0))
                .compareTo(new BigDecimal("-12345678901234567890.12345")));
    }

    // --- Decimal256 (39 <= P <= 76, 32 bytes) ---

    @Test
    void decimal256_roundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(76, 20);
        Object src = codec.allocate(1);
        // A very large BigDecimal
        BigDecimal val = new BigDecimal("12345678901234567890123456789012345678901234567890.12345678901234567890");
        codec.set(src, 0, val);

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(32, wire.length);  // 32 bytes

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(0, ((BigDecimal) codec.get(dest, 0)).compareTo(val));
    }

    // --- Scale edge cases ---

    @Test
    void decimal_scalePreservedOnGet() throws IOException {
        // scale=4, value 1.5 should return as BigDecimal with scale 4: 1.5000
        DecimalCodec codec = new DecimalCodec(9, 4);
        Object src = codec.allocate(1);
        codec.set(src, 0, new BigDecimal("1.5"));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        BigDecimal result = (BigDecimal) codec.get(dest, 0);
        assertEquals(4, result.scale());
        assertEquals(0, result.compareTo(new BigDecimal("1.5")));
    }

    @Test
    void decimal_multipleRowsRoundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(9, 2);
        Object src = codec.allocate(5);
        BigDecimal[] vals = {
            new BigDecimal("0.00"),
            // Decimal(9,2) allows 7 integer digits + 2 fractional = 9 significant digits.
            new BigDecimal("9999999.99"),
            new BigDecimal("-9999999.99"),
            new BigDecimal("1.23"),
            new BigDecimal("-0.01")
        };
        for (int i = 0; i < vals.length; i++) {
            codec.set(src, i, vals[i]);
        }

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, vals.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(20, wire.length);  // 5 × 4 bytes

        Object dest = codec.allocate(5);
        codec.read(Bytes.reader(wire), 5, dest);
        for (int i = 0; i < vals.length; i++) {
            assertEquals(0, vals[i].compareTo((BigDecimal) codec.get(dest, i)),
                    "mismatch at index " + i);
        }
    }

    @Test
    void decimal32_emptyColumn() throws IOException {
        DecimalCodec codec = new DecimalCodec(9, 2);
        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, codec.allocate(0), 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertArrayEquals(new byte[0], wire);
    }
}
