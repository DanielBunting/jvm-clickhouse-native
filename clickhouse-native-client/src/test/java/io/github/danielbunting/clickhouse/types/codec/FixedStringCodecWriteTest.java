package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.protocol.DefaultBinaryWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the write half of {@link FixedStringCodec}, which accepts raw bytes (e.g.
 * an Arrow {@code FixedSizeBinary} cell fed through the ADBC ingest bridge) as well as Strings:
 * both zero-pad to the declared width, nulls write as all-NUL, and anything else fails clearly
 * rather than corrupting the fixed-width wire frame.
 */
class FixedStringCodecWriteTest {

    /** Serializes {@code values} through the codec and returns the raw wire bytes. */
    private static byte[] wireBytes(int width, Object... values) throws IOException {
        FixedStringCodec codec = new FixedStringCodec(width);
        Object[] backing = codec.allocate(values.length);
        for (int i = 0; i < values.length; i++) {
            codec.set(backing, i, values[i]);
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultBinaryWriter writer = new DefaultBinaryWriter(baos);
        codec.write(writer, backing, values.length);
        writer.flush();
        return baos.toByteArray();
    }

    @Test
    @DisplayName("String values encode as UTF-8, zero-padded to the declared width")
    void stringValuesZeroPadded() throws IOException {
        assertArrayEquals(
                new byte[] {'a', 'b', 0, 0, 'w', 'i', 'd', 'e'},
                wireBytes(4, "ab", "wide"));
    }

    @Test
    @DisplayName("raw byte[] values (the Arrow FixedSizeBinary ingest shape) write verbatim")
    void byteArrayValuesWriteVerbatim() throws IOException {
        assertArrayEquals(
                new byte[] {1, 2, 3, 4, 9, 0, 0, 0},
                wireBytes(4, new byte[] {1, 2, 3, 4}, new byte[] {9}));
    }

    @Test
    @DisplayName("a null cell writes the all-NUL frame")
    void nullWritesAllNulFrame() throws IOException {
        assertArrayEquals(new byte[] {0, 0, 0, 0}, wireBytes(4, (Object) null));
    }

    @Test
    @DisplayName("an unconvertible value type fails clearly instead of corrupting the frame")
    void unconvertibleValueFailsClearly() {
        assertThrows(IllegalArgumentException.class, () -> wireBytes(4, 42),
                "a numeric value has no FixedString byte form");
    }

    @Test
    @DisplayName("fixedLength reports the declared width")
    void fixedLengthReportsWidth() {
        org.junit.jupiter.api.Assertions.assertEquals(7, new FixedStringCodec(7).fixedLength());
    }

    @Test
    @DisplayName("UTF-8 multi-byte content pads by BYTES, not characters")
    void multiByteContentPadsByBytes() throws IOException {
        byte[] expected = new byte[6];
        byte[] utf8 = "héllo".getBytes(StandardCharsets.UTF_8); // 6 bytes
        System.arraycopy(utf8, 0, expected, 0, utf8.length);
        assertArrayEquals(expected, wireBytes(6, "héllo"));
    }
}
