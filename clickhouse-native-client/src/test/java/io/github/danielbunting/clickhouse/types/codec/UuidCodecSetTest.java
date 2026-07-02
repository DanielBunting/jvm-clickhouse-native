package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the byte-array arm of {@link UuidCodec#set}: a big-endian 16-byte value
 * (e.g. an Arrow {@code FixedSizeBinary(16)} cell fed through the ADBC ingest bridge) decodes
 * to the same UUID, and wrong widths are rejected instead of silently mis-decoding.
 */
class UuidCodecSetTest {

    @Test
    @DisplayName("a big-endian 16-byte value decodes to the equivalent UUID")
    void bigEndianBytesDecode() {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        byte[] bytes = ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        UuidCodec codec = new UuidCodec();
        UUID[] backing = codec.allocate(1);
        codec.set(backing, 0, bytes);
        assertEquals(uuid, codec.get(backing, 0));
    }

    @Test
    @DisplayName("a byte value of the wrong width is rejected")
    void wrongWidthRejected() {
        UuidCodec codec = new UuidCodec();
        UUID[] backing = codec.allocate(1);
        assertThrows(IllegalArgumentException.class, () -> codec.set(backing, 0, new byte[15]));
        assertThrows(IllegalArgumentException.class, () -> codec.set(backing, 0, new byte[17]));
    }
}
