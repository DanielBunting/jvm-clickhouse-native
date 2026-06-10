package io.github.danielbunting.clickhouse.compress;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.EOFException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Robustness tests for {@link CompressedBlockCodec} frame-header validation.
 *
 * <p>Checksum tampering is already covered by {@code CompressedBlockCodecTest}
 * ({@code readThrowsOnChecksumMismatch} / {@code readThrowsOnCorruptedChecksumBytes}).
 * This suite covers the header size fields and a truncated payload — a malformed
 * {@code compressed_size}/{@code uncompressed_size} must be rejected with a
 * {@link ClickHouseException} (the size checks run before the checksum is recomputed,
 * so a junk checksum here is irrelevant), and a payload shorter than {@code compressed_size}
 * claims must surface as an {@link EOFException}.
 */
class CompressedFrameRobustnessTest {

    private static final int HEADER = 9; // method (1) + compressed_size (4) + uncompressed_size (4)

    /** Builds a frame: 16-byte (junk) checksum, method, compressed_size, uncompressed_size, payload. */
    private static byte[] frame(long compressedSize, long uncompressedSize, byte[] payload) {
        return Bytes.capture(w -> {
            w.writeBytes(new byte[16], 0, 16); // checksum — size checks run before checksum verification
            w.writeUInt8(CompressionMethod.LZ4.marker & 0xFF);
            w.writeUInt32(compressedSize);
            w.writeUInt32(uncompressedSize);
            if (payload.length > 0) {
                w.writeBytes(payload, 0, payload.length);
            }
        });
    }

    @Test
    void compressedSizeBelowHeader_isRejected() {
        // compressed_size must be at least the 9-byte header; 5 is impossible.
        byte[] wire = frame(5, 0, new byte[0]);
        assertThrows(ClickHouseException.class, () -> CompressedBlockCodec.read(Bytes.reader(wire)));
    }

    @Test
    void uncompressedSizeAboveIntMax_isRejected() {
        // A u32 of 0xFFFFFFFF (4_294_967_295) exceeds Integer.MAX_VALUE and would drive an
        // unbounded decompression buffer.
        byte[] wire = frame(HEADER, 0xFFFFFFFFL, new byte[0]);
        assertThrows(ClickHouseException.class, () -> CompressedBlockCodec.read(Bytes.reader(wire)));
    }

    @Test
    void truncatedPayload_throwsEof() {
        // compressed_size claims 100 payload bytes (109 - 9 header) but only 10 are present.
        byte[] wire = frame(HEADER + 100, 50, new byte[10]);
        assertThrows(EOFException.class, () -> CompressedBlockCodec.read(Bytes.reader(wire)));
    }
}
