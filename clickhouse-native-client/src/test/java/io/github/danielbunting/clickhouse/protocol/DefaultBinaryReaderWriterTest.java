package io.github.danielbunting.clickhouse.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Byte-vector and round-trip tests for {@link DefaultBinaryReader} and
 * {@link DefaultBinaryWriter}.
 */
final class DefaultBinaryReaderWriterTest {

    private static byte[] write(WriterAction action) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultBinaryWriter w = new DefaultBinaryWriter(baos);
        action.run(w);
        w.flush();
        return baos.toByteArray();
    }

    private static DefaultBinaryReader reader(byte[] bytes) {
        return new DefaultBinaryReader(new ByteArrayInputStream(bytes));
    }

    @FunctionalInterface
    private interface WriterAction {
        void run(DefaultBinaryWriter w) throws IOException;
    }

    // ----- Fixed-width little-endian byte vectors -----

    @Test
    void int16IsLittleEndian() throws IOException {
        byte[] bytes = write(w -> w.writeInt16((short) 0x0102));
        assertArrayEquals(new byte[] {0x02, 0x01}, bytes);
        assertEquals((short) 0x0102, reader(bytes).readInt16());
    }

    @Test
    void int32IsLittleEndian() throws IOException {
        byte[] bytes = write(w -> w.writeInt32(0x01020304));
        assertArrayEquals(new byte[] {0x04, 0x03, 0x02, 0x01}, bytes);
        assertEquals(0x01020304, reader(bytes).readInt32());
    }

    @Test
    void int64IsLittleEndian() throws IOException {
        byte[] bytes = write(w -> w.writeInt64(0x0102030405060708L));
        assertArrayEquals(
                new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, bytes);
        assertEquals(0x0102030405060708L, reader(bytes).readInt64());
    }

    @Test
    void uInt8RoundTripIncludingMax() throws IOException {
        for (int v : new int[] {0, 1, 127, 128, 255}) {
            byte[] bytes = write(w -> w.writeUInt8(v));
            assertEquals(1, bytes.length);
            assertEquals(v, reader(bytes).readUInt8());
        }
    }

    @Test
    void uInt16MaxValue() throws IOException {
        byte[] bytes = write(w -> w.writeUInt16(0xFFFF));
        assertArrayEquals(new byte[] {(byte) 0xFF, (byte) 0xFF}, bytes);
        assertEquals(0xFFFF, reader(bytes).readUInt16());
    }

    @Test
    void uInt32MaxValue() throws IOException {
        long max = 0xFFFFFFFFL;
        byte[] bytes = write(w -> w.writeUInt32(max));
        assertEquals(max, reader(bytes).readUInt32());
    }

    @Test
    void uInt64RawBitsRoundTrip() throws IOException {
        long raw = -1L; // all bits set -> 2^64-1 unsigned
        byte[] bytes = write(w -> w.writeUInt64(raw));
        assertEquals(raw, reader(bytes).readUInt64());
        assertEquals("18446744073709551615", Long.toUnsignedString(reader(bytes).readUInt64()));
    }

    @Test
    void int8NegativeRoundTrip() throws IOException {
        byte[] bytes = write(w -> w.writeInt8((byte) -128));
        assertEquals((byte) -128, reader(bytes).readInt8());
        assertEquals(0x80, reader(bytes).readByteUnsigned());
    }

    @Test
    void floatRoundTrip() throws IOException {
        byte[] f = write(w -> w.writeFloat32(3.14159f));
        assertEquals(3.14159f, reader(f).readFloat32());
        byte[] d = write(w -> w.writeFloat64(2.718281828459045));
        assertEquals(2.718281828459045, reader(d).readFloat64());
    }

    // ----- VarUInt (unsigned LEB128) -----

    @Test
    void varUIntZero() throws IOException {
        byte[] bytes = write(w -> w.writeVarUInt(0));
        assertArrayEquals(new byte[] {0x00}, bytes);
        assertEquals(0, reader(bytes).readVarUInt());
    }

    @Test
    void varUIntSingleByteBoundary() throws IOException {
        byte[] bytes = write(w -> w.writeVarUInt(127));
        assertArrayEquals(new byte[] {0x7F}, bytes);
        assertEquals(127, reader(bytes).readVarUInt());
    }

    @Test
    void varUIntTwoBytes() throws IOException {
        byte[] bytes = write(w -> w.writeVarUInt(128));
        assertArrayEquals(new byte[] {(byte) 0x80, 0x01}, bytes);
        assertEquals(128, reader(bytes).readVarUInt());
    }

    @Test
    void varUIntMultiByte300() throws IOException {
        byte[] bytes = write(w -> w.writeVarUInt(300));
        assertArrayEquals(new byte[] {(byte) 0xAC, 0x02}, bytes);
        assertEquals(300, reader(bytes).readVarUInt());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 127, 128, 16383, 16384, 2097151, 1L << 32, Long.MAX_VALUE, -1L})
    void varUIntRoundTrip(long v) throws IOException {
        byte[] bytes = write(w -> w.writeVarUInt(v));
        assertEquals(v, reader(bytes).readVarUInt());
    }

    @Test
    void varUIntMaxUnsignedUsesTenBytes() throws IOException {
        byte[] bytes = write(w -> w.writeVarUInt(-1L));
        assertEquals(10, bytes.length);
        assertEquals(-1L, reader(bytes).readVarUInt());
    }

    // ----- VarInt (zig-zag LEB128) -----

    @ParameterizedTest
    @ValueSource(longs = {0, -1, 1, -2, 2, 63, -64, 64, -65, Long.MIN_VALUE, Long.MAX_VALUE, 12345678901L})
    void varIntRoundTrip(long v) throws IOException {
        byte[] bytes = write(w -> w.writeVarInt(v));
        assertEquals(v, reader(bytes).readVarInt());
    }

    @Test
    void varIntZigZagEncoding() throws IOException {
        assertArrayEquals(new byte[] {0x00}, write(w -> w.writeVarInt(0)));
        assertArrayEquals(new byte[] {0x01}, write(w -> w.writeVarInt(-1)));
        assertArrayEquals(new byte[] {0x02}, write(w -> w.writeVarInt(1)));
        assertArrayEquals(new byte[] {0x03}, write(w -> w.writeVarInt(-2)));
    }

    // ----- Strings -----

    @Test
    void emptyString() throws IOException {
        byte[] bytes = write(w -> w.writeString(""));
        assertArrayEquals(new byte[] {0x00}, bytes);
        assertEquals("", reader(bytes).readString());
    }

    @Test
    void asciiString() throws IOException {
        byte[] bytes = write(w -> w.writeString("hello"));
        assertArrayEquals(
                new byte[] {0x05, 'h', 'e', 'l', 'l', 'o'}, bytes);
        assertEquals("hello", reader(bytes).readString());
    }

    @Test
    void utf8MultiByteString() throws IOException {
        String s = "héllo 世界 😀"; // accented, CJK, emoji
        byte[] bytes = write(w -> w.writeString(s));
        int payloadLen = s.getBytes(StandardCharsets.UTF_8).length;
        assertEquals(payloadLen + 1, bytes.length); // single-byte length prefix
        assertEquals(s, reader(bytes).readString());
    }

    @Test
    void longStringMultiByteLengthPrefix() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append('x');
        }
        String s = sb.toString();
        byte[] bytes = write(w -> w.writeString(s));
        // 200 -> VarUInt = 0xC8 0x01 (2 bytes) + 200 payload
        assertEquals(202, bytes.length);
        assertEquals(s, reader(bytes).readString());
    }

    // ----- Bytes -----

    @Test
    void bytesRoundTrip() throws IOException {
        byte[] payload = {1, 2, 3, 4, 5};
        byte[] bytes = write(w -> w.writeBytes(payload, 0, payload.length));
        assertArrayEquals(payload, reader(bytes).readBytes(5));
    }

    @Test
    void bytesWithOffset() throws IOException {
        byte[] payload = {9, 1, 2, 3, 9};
        byte[] bytes = write(w -> w.writeBytes(payload, 1, 3));
        assertArrayEquals(new byte[] {1, 2, 3}, bytes);
    }

    @Test
    void readBytesZeroLength() throws IOException {
        assertArrayEquals(new byte[0], reader(new byte[0]).readBytes(0));
    }

    @Test
    void readFullyIntoOffset() throws IOException {
        byte[] src = {10, 20, 30};
        DefaultBinaryReader r = reader(src);
        byte[] dest = new byte[5];
        r.readFully(dest, 1, 3);
        assertArrayEquals(new byte[] {0, 10, 20, 30, 0}, dest);
    }

    // ----- EOF / error handling -----

    @Test
    void readByteOnEmptyStreamThrowsEof() {
        assertThrows(EOFException.class, () -> reader(new byte[0]).readByteUnsigned());
    }

    @Test
    void readFullyOnShortStreamThrowsEof() {
        DefaultBinaryReader r = reader(new byte[] {1, 2});
        assertThrows(EOFException.class, () -> r.readBytes(4));
    }

    @Test
    void readInt32OnTruncatedStreamThrowsEof() {
        DefaultBinaryReader r = reader(new byte[] {1, 2, 3});
        assertThrows(EOFException.class, r::readInt32);
    }

    @Test
    void varUIntTruncatedContinuationThrowsEof() {
        // high bit set but no following byte
        DefaultBinaryReader r = reader(new byte[] {(byte) 0x80});
        assertThrows(EOFException.class, r::readVarUInt);
    }

    @Test
    void varUIntOverlongThrows() {
        byte[] tooLong = new byte[11];
        for (int i = 0; i < 11; i++) {
            tooLong[i] = (byte) 0x80;
        }
        DefaultBinaryReader r = reader(tooLong);
        assertThrows(IOException.class, r::readVarUInt);
    }

    @Test
    void writeNullStringThrows() {
        assertThrows(NullPointerException.class, () -> write(w -> w.writeString(null)));
    }

    // ----- Composite sequence round-trip -----

    @Test
    void mixedSequenceRoundTrip() throws IOException {
        byte[] bytes = write(w -> {
            w.writeUInt8(200);
            w.writeInt32(-12345);
            w.writeVarUInt(99999);
            w.writeString("ClickHouse");
            w.writeFloat64(1.5);
            w.writeVarInt(-77);
            w.writeUInt64(-1L);
        });
        DefaultBinaryReader r = reader(bytes);
        assertEquals(200, r.readUInt8());
        assertEquals(-12345, r.readInt32());
        assertEquals(99999, r.readVarUInt());
        assertEquals("ClickHouse", r.readString());
        assertEquals(1.5, r.readFloat64());
        assertEquals(-77, r.readVarInt());
        assertEquals(-1L, r.readUInt64());
    }
}
