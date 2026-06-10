package io.github.danielbunting.clickhouse.testutil;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Self-tests for {@link Bytes} test utilities.
 *
 * <p>These tests verify that {@code reader()}, {@code capture()}, and
 * {@code hex()} work correctly and that the reader/writer agree on a
 * round-trip. No running ClickHouse server is required.
 */
final class BytesTest {

    // ----- reader() -----

    @Test
    void readerWrapsDataCorrectly() throws IOException {
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        BinaryReader r = Bytes.reader(data);
        assertEquals(0x01, r.readByteUnsigned());
        assertEquals(0x02, r.readByteUnsigned());
        assertEquals(0x03, r.readByteUnsigned());
        assertEquals(0x04, r.readByteUnsigned());
    }

    @Test
    void readerOnEmptyArrayReturnsValidReader() {
        // should not throw; only reading would throw
        BinaryReader r = Bytes.reader(new byte[0]);
        assertThrows(IOException.class, r::readByteUnsigned);
    }

    // ----- capture() -----

    @Test
    void captureWritesSingleByte() {
        byte[] bytes = Bytes.capture(w -> w.writeUInt8(0xAB));
        assertArrayEquals(new byte[]{(byte) 0xAB}, bytes);
    }

    @Test
    void captureWritesLittleEndianInt32() {
        byte[] bytes = Bytes.capture(w -> w.writeInt32(0x01020304));
        assertArrayEquals(new byte[]{0x04, 0x03, 0x02, 0x01}, bytes);
    }

    @Test
    void captureEmptyConsumerReturnsEmptyArray() {
        byte[] bytes = Bytes.capture(w -> {/* nothing */});
        assertArrayEquals(new byte[0], bytes);
    }

    // ----- round-trip via reader + capture -----

    @Test
    void captureAndReaderRoundTripVarUInt() throws IOException {
        byte[] bytes = Bytes.capture(w -> w.writeVarUInt(300));
        BinaryReader r = Bytes.reader(bytes);
        assertEquals(300L, r.readVarUInt());
    }

    @Test
    void captureAndReaderRoundTripString() throws IOException {
        String value = "ClickHouse native";
        byte[] bytes = Bytes.capture(w -> w.writeString(value));
        BinaryReader r = Bytes.reader(bytes);
        assertEquals(value, r.readString());
    }

    @Test
    void captureAndReaderRoundTripInt64() throws IOException {
        long value = Long.MIN_VALUE;
        byte[] bytes = Bytes.capture(w -> w.writeInt64(value));
        BinaryReader r = Bytes.reader(bytes);
        assertEquals(value, r.readInt64());
    }

    // ----- hex() -----

    @Test
    void hexEmptyArray() {
        assertEquals("", Bytes.hex(new byte[0]));
    }

    @Test
    void hexSingleByte() {
        assertEquals("00", Bytes.hex(new byte[]{0x00}));
        assertEquals("ff", Bytes.hex(new byte[]{(byte) 0xFF}));
        assertEquals("0a", Bytes.hex(new byte[]{0x0A}));
    }

    @Test
    void hexMultipleBytes() {
        assertEquals("0aff1b", Bytes.hex(new byte[]{0x0A, (byte) 0xFF, 0x1B}));
    }

    @Test
    void hexAllNibbles() {
        byte[] all = new byte[16];
        for (int i = 0; i < 16; i++) {
            all[i] = (byte) (i * 0x11); // 0x00, 0x11, 0x22, ..., 0xff
        }
        assertEquals("00112233445566778899aabbccddeeff", Bytes.hex(all));
    }

    @Test
    void hexNullThrows() {
        assertThrows(NullPointerException.class, () -> Bytes.hex(null));
    }

    @Test
    void hexMatchesCapturedBytes() {
        // capture a known LE int32 and verify hex matches expected wire bytes
        byte[] bytes = Bytes.capture(w -> w.writeInt32(0xDEADBEEF));
        // 0xEF 0xBE 0xAD 0xDE in LE order
        assertEquals("efbeadde", Bytes.hex(bytes));
    }
}
