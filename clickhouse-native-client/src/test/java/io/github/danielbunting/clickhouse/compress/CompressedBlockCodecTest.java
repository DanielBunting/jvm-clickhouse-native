package io.github.danielbunting.clickhouse.compress;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link CompressedBlockCodec} framing and checksum verification.
 *
 * <p>Uses a copy-only stub {@link Compressor} (LZ4 marker) so the {@code read}
 * path routes to a matching copy-only stub {@link Decompressor} via the
 * checksum-verified frame, exercising the framing layer independently of the real
 * LZ4/ZSTD codecs. The full end-to-end test uses the real LZ4 codec.
 */
class CompressedBlockCodecTest {

    private static final int HEADER = 9;     // method (1) + compressed_size (4) + uncompressed_size (4)
    private static final int CHECKSUM = 16;

    /** Copy-only compressor that emits the LZ4 marker so the codec frames it as LZ4. */
    private static final class CopyCompressor implements Compressor {
        @Override public CompressionMethod method() { return CompressionMethod.LZ4; }
        @Override public byte[] compress(byte[] src, int offset, int length) {
            return Arrays.copyOfRange(src, offset, offset + length);
        }
    }

    private static byte[] frame(byte[] raw) {
        return frame(raw, new CopyCompressor());
    }

    private static byte[] frame(byte[] raw, Compressor compressor) {
        return Bytes.capture(w -> {
            try {
                CompressedBlockCodec.write(w, raw, compressor);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static long leU32(byte[] b, int off) {
        return (b[off] & 0xFFL)
                | ((b[off + 1] & 0xFFL) << 8)
                | ((b[off + 2] & 0xFFL) << 16)
                | ((b[off + 3] & 0xFFL) << 24);
    }

    @Test
    void writeProducesExpectedHeaderLayout() {
        byte[] raw = "hello clickhouse".getBytes(StandardCharsets.UTF_8);
        byte[] framed = frame(raw);

        int payloadLen = raw.length;            // copy stub => payload == raw
        int compressedSize = HEADER + payloadLen;
        assertEquals(CHECKSUM + compressedSize, framed.length, "total frame length");

        // method byte directly after the 16-byte checksum
        assertEquals(CompressionMethod.LZ4.marker, framed[CHECKSUM] & 0xFF, "method marker");
        // compressed_size includes the 9-byte header (ClickHouse semantics)
        assertEquals(compressedSize, leU32(framed, CHECKSUM + 1), "compressed_size");
        assertEquals(raw.length, leU32(framed, CHECKSUM + 5), "uncompressed_size");

        // payload bytes follow the header unchanged
        byte[] payload = Arrays.copyOfRange(framed, CHECKSUM + HEADER, framed.length);
        assertArrayEquals(raw, payload, "payload == raw for copy stub");
    }

    @Test
    void checksumCoversHeaderPlusPayload() {
        byte[] raw = "checksum me".getBytes(StandardCharsets.UTF_8);
        byte[] framed = frame(raw);

        byte[] storedChecksum = Arrays.copyOfRange(framed, 0, CHECKSUM);
        byte[] checksummed = Arrays.copyOfRange(framed, CHECKSUM, framed.length);

        // Recompute over exactly [method | compressed_size | uncompressed_size | payload].
        byte[] recomputed = CityHash128.hash128(checksummed, 0, checksummed.length);
        assertArrayEquals(recomputed, storedChecksum, "checksum covers header + payload");
    }

    @Test
    void roundTripThroughWriteThenRead() {
        // read() routes to the real LZ4 decompressor via the marker, so the
        // payload must be valid LZ4 -> use the real LZ4 compressor here.
        byte[] raw = "round trip payload bytes round trip payload bytes".getBytes(StandardCharsets.UTF_8);
        byte[] framed = frame(raw, new Lz4Compressor());

        BinaryReader reader = Bytes.reader(framed);
        byte[] decoded;
        try {
            decoded = CompressedBlockCodec.read(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertArrayEquals(raw, decoded, "round-trip restores the raw block");
    }

    @Test
    void readThrowsOnChecksumMismatch() {
        byte[] raw = "tamper target".getBytes(StandardCharsets.UTF_8);
        byte[] framed = frame(raw);

        // Corrupt a payload byte without touching the stored checksum.
        framed[CHECKSUM + HEADER] ^= 0xFF;

        BinaryReader reader = Bytes.reader(framed);
        assertThrows(ClickHouseException.class, () -> CompressedBlockCodec.read(reader));
    }

    @Test
    void readThrowsOnCorruptedChecksumBytes() {
        byte[] raw = "checksum corruption".getBytes(StandardCharsets.UTF_8);
        byte[] framed = frame(raw);

        // Flip the first checksum byte.
        framed[0] ^= 0x01;

        BinaryReader reader = Bytes.reader(framed);
        assertThrows(ClickHouseException.class, () -> CompressedBlockCodec.read(reader));
    }

    @Test
    void offsetLengthOverloadFramesOnlyTheRequestedRegion() {
        // A reused staging buffer holds extra trailing bytes from a previous (larger)
        // block. The offset/length overload must frame only [off, off+len) and ignore
        // the stale tail, producing the same wire bytes as a trimmed copy.
        byte[] payload = "region-under-test".getBytes(StandardCharsets.UTF_8);
        byte[] staging = new byte[payload.length + 32];
        System.arraycopy(payload, 0, staging, 0, payload.length);
        Arrays.fill(staging, payload.length, staging.length, (byte) 0x7E); // stale tail

        byte[] viaOverload = Bytes.capture(w -> {
            try {
                CompressedBlockCodec.write(w, staging, 0, payload.length, new Lz4Compressor());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        byte[] viaTrimmedCopy = frame(payload, new Lz4Compressor());
        assertArrayEquals(viaTrimmedCopy, viaOverload,
                "offset/length overload yields identical wire bytes to a trimmed-copy write");

        byte[] decoded;
        try {
            decoded = CompressedBlockCodec.read(Bytes.reader(viaOverload));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertArrayEquals(payload, decoded, "overload round-trips the requested region only");
    }

    @Test
    void multipleBlocksThroughOneReusedBufferRoundTrip() {
        // Simulate the NativeClientImpl compressed path: ONE reused growable staging
        // buffer + ONE reused compressor, serializing/compressing several blocks in a
        // row with reset() between them. Each framed block must independently round-trip.
        java.io.ByteArrayOutputStream staging = new java.io.ByteArrayOutputStream();
        Lz4Compressor reusedCompressor = new Lz4Compressor();

        byte[][] blocks = {
            "first block payload".getBytes(StandardCharsets.UTF_8),
            "second, somewhat longer block payload with more bytes".getBytes(StandardCharsets.UTF_8),
            new byte[0],
            "tiny".getBytes(StandardCharsets.UTF_8),
        };

        for (byte[] raw : blocks) {
            staging.reset();
            staging.write(raw, 0, raw.length); // stage the "serialized block"
            byte[] backing = staging.toByteArray(); // capture current contents
            int len = staging.size();

            byte[] framed = Bytes.capture(w -> {
                try {
                    CompressedBlockCodec.write(w, backing, 0, len, reusedCompressor);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            byte[] decoded;
            try {
                decoded = CompressedBlockCodec.read(Bytes.reader(framed));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            assertArrayEquals(raw, decoded,
                    "block round-trips through the reused buffer + reused compressor");
        }
    }

    @Test
    void emptyRawBlockRoundTrips() {
        byte[] raw = new byte[0];
        byte[] framed = frame(raw);
        assertEquals(CHECKSUM + HEADER, framed.length, "empty block frame is checksum + header only");

        BinaryReader reader = Bytes.reader(framed);
        byte[] decoded;
        try {
            decoded = CompressedBlockCodec.read(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertEquals(0, decoded.length, "empty block round-trips to empty");
    }
}
