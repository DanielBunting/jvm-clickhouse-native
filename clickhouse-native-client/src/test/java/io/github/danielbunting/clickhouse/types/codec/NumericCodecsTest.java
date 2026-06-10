package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip byte-vector tests for all fixed-width numeric codecs (B1).
 *
 * <p>Each test verifies:
 * <ol>
 *   <li>Writing a filled array produces the expected little-endian byte sequence.</li>
 *   <li>Reading those same bytes back produces an array equal to the original.</li>
 *   <li>Boundary/sign-bit-set values are handled correctly.</li>
 * </ol>
 *
 * <p>No running ClickHouse server is required; all I/O is in-memory via
 * {@link Bytes#reader(byte[])} and {@link Bytes#capture(java.util.function.Consumer)}.
 */
class NumericCodecsTest {

    // -----------------------------------------------------------------------
    // Int8
    // -----------------------------------------------------------------------

    @Test
    void int8_roundTrip() throws IOException {
        Int8Codec codec = new Int8Codec();
        assertEquals("Int8", codec.typeName());
        assertEquals(Byte.class, codec.javaType());

        byte[] src = {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        // Wire: one byte per value, exact Java byte representation
        assertArrayEquals(new byte[]{(byte) -128, (byte) -1, 0, 1, 127}, wire);

        BinaryReader reader = Bytes.reader(wire);
        byte[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        // get/set bridge
        assertEquals((byte) -128, codec.get(dest, 0));
        codec.set(dest, 0, (byte) 42);
        assertEquals((byte) 42, dest[0]);
    }

    @Test
    void int8_signBitSet() throws IOException {
        Int8Codec codec = new Int8Codec();
        // 0x80 = -128 in signed interpretation
        byte[] wire = {(byte) 0x80};
        byte[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(Byte.MIN_VALUE, dest[0]);
    }

    // -----------------------------------------------------------------------
    // Int16
    // -----------------------------------------------------------------------

    @Test
    void int16_roundTrip() throws IOException {
        Int16Codec codec = new Int16Codec();
        assertEquals("Int16", codec.typeName());
        assertEquals(Short.class, codec.javaType());

        short[] src = {Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        // 10 bytes total: 5 x 2 bytes LE
        assertEquals(10, wire.length);
        // Short.MIN_VALUE = 0x8000 -> LE bytes [0x00, 0x80]
        assertEquals((byte) 0x00, wire[0]);
        assertEquals((byte) 0x80, wire[1]);
        // -1 = 0xFFFF -> LE [0xFF, 0xFF]
        assertEquals((byte) 0xFF, wire[2]);
        assertEquals((byte) 0xFF, wire[3]);

        BinaryReader reader = Bytes.reader(wire);
        short[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals(Short.MIN_VALUE, codec.get(dest, 0));
    }

    @Test
    void int16_signBitSet() throws IOException {
        Int16Codec codec = new Int16Codec();
        // 0x8000 LE -> [0x00, 0x80]
        byte[] wire = {0x00, (byte) 0x80};
        short[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(Short.MIN_VALUE, dest[0]);
    }

    // -----------------------------------------------------------------------
    // Int32
    // -----------------------------------------------------------------------

    @Test
    void int32_roundTrip() throws IOException {
        Int32Codec codec = new Int32Codec();
        assertEquals("Int32", codec.typeName());
        assertEquals(Integer.class, codec.javaType());

        int[] src = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(20, wire.length);
        // Integer.MIN_VALUE = 0x80000000 -> LE [0x00, 0x00, 0x00, 0x80]
        assertEquals((byte) 0x00, wire[0]);
        assertEquals((byte) 0x00, wire[1]);
        assertEquals((byte) 0x00, wire[2]);
        assertEquals((byte) 0x80, wire[3]);

        BinaryReader reader = Bytes.reader(wire);
        int[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals(Integer.MIN_VALUE, codec.get(dest, 0));
        codec.set(dest, 1, 99);
        assertEquals(99, dest[1]);
    }

    @Test
    void int32_signBitSet() throws IOException {
        Int32Codec codec = new Int32Codec();
        // 0x80000000 LE
        byte[] wire = {0x00, 0x00, 0x00, (byte) 0x80};
        int[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(Integer.MIN_VALUE, dest[0]);
    }

    // -----------------------------------------------------------------------
    // Int64
    // -----------------------------------------------------------------------

    @Test
    void int64_roundTrip() throws IOException {
        Int64Codec codec = new Int64Codec();
        assertEquals("Int64", codec.typeName());
        assertEquals(Long.class, codec.javaType());

        long[] src = {Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(40, wire.length);
        // Long.MIN_VALUE = 0x8000000000000000 -> LE [0,0,0,0,0,0,0,0x80]
        for (int i = 0; i < 7; i++) assertEquals((byte) 0x00, wire[i]);
        assertEquals((byte) 0x80, wire[7]);

        BinaryReader reader = Bytes.reader(wire);
        long[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals(Long.MIN_VALUE, codec.get(dest, 0));
    }

    @Test
    void int64_signBitSet() throws IOException {
        Int64Codec codec = new Int64Codec();
        byte[] wire = {0,0,0,0,0,0,0,(byte)0x80};
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(Long.MIN_VALUE, dest[0]);
    }

    // -----------------------------------------------------------------------
    // UInt8
    // -----------------------------------------------------------------------

    @Test
    void uint8_roundTrip() throws IOException {
        UInt8Codec codec = new UInt8Codec();
        assertEquals("UInt8", codec.typeName());
        assertEquals(Integer.class, codec.javaType());

        int[] src = {0, 1, 127, 128, 255};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(5, wire.length);
        assertEquals((byte) 0x00, wire[0]);
        assertEquals((byte) 0x01, wire[1]);
        assertEquals((byte) 0x7F, wire[2]);
        assertEquals((byte) 0x80, wire[3]); // 128 -> 0x80
        assertEquals((byte) 0xFF, wire[4]); // 255 -> 0xFF

        BinaryReader reader = Bytes.reader(wire);
        int[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals(255, codec.get(dest, 4));
        // A value outside [0, 255] is rejected (no silent high-bit stripping) so a bulk INSERT
        // can never commit a corrupted value.
        assertThrows(IllegalArgumentException.class, () -> codec.set(dest, 0, 0x1FF));
        assertThrows(IllegalArgumentException.class, () -> codec.setLong(dest, 0, -1));
        codec.set(dest, 0, 255); // boundary still accepted
        assertEquals(255, dest[0]);
    }

    // -----------------------------------------------------------------------
    // UInt16
    // -----------------------------------------------------------------------

    @Test
    void uint16_roundTrip() throws IOException {
        UInt16Codec codec = new UInt16Codec();
        assertEquals("UInt16", codec.typeName());
        assertEquals(Integer.class, codec.javaType());

        int[] src = {0, 1, 32767, 32768, 65535};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(10, wire.length);
        // 65535 = 0xFFFF -> LE [0xFF, 0xFF]
        assertEquals((byte) 0xFF, wire[8]);
        assertEquals((byte) 0xFF, wire[9]);
        // 32768 = 0x8000 -> LE [0x00, 0x80]
        assertEquals((byte) 0x00, wire[6]);
        assertEquals((byte) 0x80, wire[7]);

        BinaryReader reader = Bytes.reader(wire);
        int[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals(65535, codec.get(dest, 4));
    }

    // -----------------------------------------------------------------------
    // UInt32
    // -----------------------------------------------------------------------

    @Test
    void uint32_roundTrip() throws IOException {
        UInt32Codec codec = new UInt32Codec();
        assertEquals("UInt32", codec.typeName());
        assertEquals(Long.class, codec.javaType());

        long[] src = {0L, 1L, Integer.MAX_VALUE, 2147483648L, 4294967295L};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(20, wire.length);
        // 4294967295 = 0xFFFFFFFF -> LE [0xFF,0xFF,0xFF,0xFF]
        assertEquals((byte) 0xFF, wire[16]);
        assertEquals((byte) 0xFF, wire[17]);
        assertEquals((byte) 0xFF, wire[18]);
        assertEquals((byte) 0xFF, wire[19]);
        // 2147483648 = 0x80000000 -> LE [0x00,0x00,0x00,0x80]
        assertEquals((byte) 0x00, wire[12]);
        assertEquals((byte) 0x00, wire[13]);
        assertEquals((byte) 0x00, wire[14]);
        assertEquals((byte) 0x80, wire[15]);

        BinaryReader reader = Bytes.reader(wire);
        long[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        assertEquals(4294967295L, codec.get(dest, 4));
    }

    @Test
    void uint32_rejectsOutOfRange() {
        UInt32Codec codec = new UInt32Codec();
        long[] arr = codec.allocate(1);
        // A 33-bit value would silently lose its high bits; reject it instead of corrupting.
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, 0x1_FFFFFFFFL));
        assertThrows(IllegalArgumentException.class, () -> codec.setLong(arr, 0, -1));
        codec.set(arr, 0, 0xFFFFFFFFL); // boundary (2^32-1) still accepted
        assertEquals(0xFFFFFFFFL, arr[0]);
    }

    // -----------------------------------------------------------------------
    // UInt64
    // -----------------------------------------------------------------------

    @Test
    void uint64_roundTrip() throws IOException {
        UInt64Codec codec = new UInt64Codec();
        assertEquals("UInt64", codec.typeName());
        assertEquals(Long.class, codec.javaType());

        // Raw bits: 0, 1, Long.MAX_VALUE, sign-bit set (represents 2^63), all-ones (2^64-1)
        long[] src = {
            0L,
            1L,
            Long.MAX_VALUE,
            Long.MIN_VALUE,        // raw bits = 0x8000000000000000, unsigned = 2^63
            -1L                    // raw bits = 0xFFFFFFFFFFFFFFFF, unsigned = 2^64-1
        };
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(40, wire.length);
        // -1L (0xFFFFFFFFFFFFFFFF) -> LE: all 0xFF
        for (int i = 32; i < 40; i++) assertEquals((byte) 0xFF, wire[i]);
        // Long.MIN_VALUE (0x8000000000000000) -> LE: [0,0,0,0,0,0,0,0x80]
        for (int i = 24; i < 31; i++) assertEquals((byte) 0x00, wire[i]);
        assertEquals((byte) 0x80, wire[31]);

        BinaryReader reader = Bytes.reader(wire);
        long[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);
        assertArrayEquals(src, dest);

        // get returns raw Long bits
        assertEquals(-1L, (Long) codec.get(dest, 4));
        assertEquals(Long.MIN_VALUE, (Long) codec.get(dest, 3));

        // Unsigned interpretation via Long.toUnsignedString
        assertEquals("18446744073709551615", Long.toUnsignedString((Long) codec.get(dest, 4)));
        assertEquals("9223372036854775808", Long.toUnsignedString((Long) codec.get(dest, 3)));
    }

    // -----------------------------------------------------------------------
    // Float32
    // -----------------------------------------------------------------------

    @Test
    void float32_roundTrip() throws IOException {
        Float32Codec codec = new Float32Codec();
        assertEquals("Float32", codec.typeName());
        assertEquals(Float.class, codec.javaType());

        float[] src = {Float.MIN_VALUE, -1.0f, 0.0f, 1.0f, Float.MAX_VALUE, Float.NaN, Float.NEGATIVE_INFINITY};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(src.length * 4, wire.length);

        BinaryReader reader = Bytes.reader(wire);
        float[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);

        for (int i = 0; i < src.length; i++) {
            // NaN != NaN, so use bit comparison
            assertEquals(Float.floatToRawIntBits(src[i]), Float.floatToRawIntBits(dest[i]),
                "Mismatch at index " + i);
        }

        assertEquals(-1.0f, (Float) codec.get(dest, 1));
    }

    @Test
    void float32_negativeOneBytes() throws IOException {
        Float32Codec codec = new Float32Codec();
        // -1.0f = 0xBF800000 -> LE [0x00, 0x00, 0x80, 0xBF]
        float[] src = {-1.0f};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, 1); } catch (IOException e) { throw new RuntimeException(e); }
        });
        assertEquals((byte) 0x00, wire[0]);
        assertEquals((byte) 0x00, wire[1]);
        assertEquals((byte) 0x80, wire[2]);
        assertEquals((byte) 0xBF, wire[3]);
    }

    // -----------------------------------------------------------------------
    // Float64
    // -----------------------------------------------------------------------

    @Test
    void float64_roundTrip() throws IOException {
        Float64Codec codec = new Float64Codec();
        assertEquals("Float64", codec.typeName());
        assertEquals(Double.class, codec.javaType());

        double[] src = {Double.MIN_VALUE, -1.0, 0.0, 1.0, Double.MAX_VALUE, Double.NaN, Double.NEGATIVE_INFINITY};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, src.length); } catch (IOException e) { throw new RuntimeException(e); }
        });

        assertEquals(src.length * 8, wire.length);

        BinaryReader reader = Bytes.reader(wire);
        double[] dest = codec.allocate(src.length);
        codec.read(reader, src.length, dest);

        for (int i = 0; i < src.length; i++) {
            assertEquals(Double.doubleToRawLongBits(src[i]), Double.doubleToRawLongBits(dest[i]),
                "Mismatch at index " + i);
        }

        assertEquals(-1.0, (Double) codec.get(dest, 1));
    }

    @Test
    void float64_negativeOneBytes() throws IOException {
        Float64Codec codec = new Float64Codec();
        // -1.0 = 0xBFF0000000000000 -> LE [0,0,0,0,0,0,0xF0,0xBF]
        double[] src = {-1.0};
        byte[] wire = Bytes.capture(w -> {
            try { codec.write(w, src, 1); } catch (IOException e) { throw new RuntimeException(e); }
        });
        assertEquals((byte) 0x00, wire[0]);
        assertEquals((byte) 0x00, wire[1]);
        assertEquals((byte) 0x00, wire[2]);
        assertEquals((byte) 0x00, wire[3]);
        assertEquals((byte) 0x00, wire[4]);
        assertEquals((byte) 0x00, wire[5]);
        assertEquals((byte) 0xF0, wire[6]);
        assertEquals((byte) 0xBF, wire[7]);
    }

    // -----------------------------------------------------------------------
    // allocate + typeName sanity checks for all codecs
    // -----------------------------------------------------------------------

    @Test
    void allCodecs_allocateCorrectTypes() {
        assertInstanceOf(byte[].class,   new Int8Codec().allocate(3));
        assertInstanceOf(short[].class,  new Int16Codec().allocate(3));
        assertInstanceOf(int[].class,    new Int32Codec().allocate(3));
        assertInstanceOf(long[].class,   new Int64Codec().allocate(3));
        assertInstanceOf(int[].class,    new UInt8Codec().allocate(3));
        assertInstanceOf(int[].class,    new UInt16Codec().allocate(3));
        assertInstanceOf(long[].class,   new UInt32Codec().allocate(3));
        assertInstanceOf(long[].class,   new UInt64Codec().allocate(3));
        assertInstanceOf(float[].class,  new Float32Codec().allocate(3));
        assertInstanceOf(double[].class, new Float64Codec().allocate(3));
    }

    @Test
    void allCodecs_typeNames() {
        assertEquals("Int8",    new Int8Codec().typeName());
        assertEquals("Int16",   new Int16Codec().typeName());
        assertEquals("Int32",   new Int32Codec().typeName());
        assertEquals("Int64",   new Int64Codec().typeName());
        assertEquals("UInt8",   new UInt8Codec().typeName());
        assertEquals("UInt16",  new UInt16Codec().typeName());
        assertEquals("UInt32",  new UInt32Codec().typeName());
        assertEquals("UInt64",  new UInt64Codec().typeName());
        assertEquals("Float32", new Float32Codec().typeName());
        assertEquals("Float64", new Float64Codec().typeName());
    }
}
