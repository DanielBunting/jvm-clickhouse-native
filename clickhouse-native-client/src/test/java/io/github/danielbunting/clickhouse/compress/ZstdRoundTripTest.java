package io.github.danielbunting.clickhouse.compress;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and byte-vector tests for {@link ZstdCompressor} and
 * {@link ZstdDecompressor}. No running ClickHouse server is required.
 */
class ZstdRoundTripTest {

    private final ZstdCompressor compressor = new ZstdCompressor();
    private final ZstdDecompressor decompressor = new ZstdDecompressor();

    // ---- method() contracts ------------------------------------------------

    @Test
    void compressorMethodIsZstd() {
        assertEquals(CompressionMethod.ZSTD, compressor.method());
    }

    @Test
    void decompressorMethodIsZstd() {
        assertEquals(CompressionMethod.ZSTD, decompressor.method());
    }

    // ---- empty buffer ------------------------------------------------------

    @Test
    void roundTripEmptyBuffer() {
        byte[] original = new byte[0];
        byte[] compressed = compressor.compress(original, 0, 0);
        assertNotNull(compressed);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, 0);
        assertArrayEquals(original, recovered);
    }

    // ---- small ASCII string ------------------------------------------------

    @Test
    void roundTripSmallString() {
        byte[] original = "Hello, ClickHouse!".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- highly-compressible all-zeros block --------------------------------

    @Test
    void roundTripAllZeros() {
        byte[] original = new byte[64 * 1024]; // 64 KB of zeros
        byte[] compressed = compressor.compress(original, 0, original.length);
        // Zstd should compress zeros well; compressed must be smaller than original.
        assertTrue(compressed.length < original.length,
                "Expected compressed size < original; got compressed=" + compressed.length);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- pseudo-random (incompressible) data --------------------------------

    @Test
    void roundTripRandomData() {
        byte[] original = new byte[16 * 1024];
        new Random(0xDEADBEEFL).nextBytes(original);
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- single byte --------------------------------------------------------

    @Test
    void roundTripSingleByte() {
        byte[] original = new byte[]{(byte) 0xAB};
        byte[] compressed = compressor.compress(original, 0, 1);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, 1);
        assertArrayEquals(original, recovered);
    }

    // ---- larger realistic block (1 MB) -------------------------------------

    @Test
    void roundTripOneMegabyte() {
        // Alternating pattern: somewhat compressible, realistic column data.
        byte[] original = new byte[1024 * 1024];
        Random rnd = new Random(42);
        for (int i = 0; i < original.length; i++) {
            // mix of repeated and random bytes
            original[i] = (i % 64 == 0) ? (byte) rnd.nextInt(256) : (byte) (i & 0xFF);
        }
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- non-zero offset in source array -----------------------------------

    @Test
    void roundTripWithOffset() {
        byte[] padded = new byte[32 + 256];
        // fill prefix with noise so accidental offset=0 would produce wrong data
        Arrays.fill(padded, 0, 32, (byte) 0xFF);
        byte[] payload = new byte[256];
        new Random(99).nextBytes(payload);
        System.arraycopy(payload, 0, padded, 32, 256);

        byte[] compressed = compressor.compress(padded, 32, 256);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, 256);
        assertArrayEquals(payload, recovered);
    }

    // ---- decompress with non-zero offset -----------------------------------

    @Test
    void decompressWithOffset() {
        byte[] original = "offset decompression test".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressor.compress(original, 0, original.length);

        // Prepend 8 bytes of garbage, then decompress from offset 8.
        byte[] padded = new byte[8 + compressed.length];
        Arrays.fill(padded, 0, 8, (byte) 0x00);
        System.arraycopy(compressed, 0, padded, 8, compressed.length);

        byte[] recovered = decompressor.decompress(padded, 8, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- repeated pattern (high repetition) --------------------------------

    @Test
    void roundTripRepeatedPattern() {
        byte[] pattern = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        byte[] original = new byte[4096];
        for (int i = 0; i < original.length; i++) {
            original[i] = pattern[i % pattern.length];
        }
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- binary data with all byte values ----------------------------------

    @Test
    void roundTripAllByteValues() {
        byte[] original = new byte[256];
        for (int i = 0; i < 256; i++) {
            original[i] = (byte) i;
        }
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] recovered = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, recovered);
    }

    // ---- error: wrong uncompressedSize -------------------------------------

    @Test
    void decompressWithWrongSizeThrows() {
        byte[] original = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        byte[] compressed = compressor.compress(original, 0, original.length);
        // Claim the uncompressed size is larger than reality — Zstd should report an error
        // or we detect the mismatch.
        assertThrows(IllegalArgumentException.class,
                () -> decompressor.decompress(compressed, 0, compressed.length, original.length + 100));
    }

    // ---- compressed output is a valid Zstd frame (magic bytes check) -------

    @Test
    void compressedOutputStartsWithZstdMagic() {
        // Zstd frame magic number: 0xFD2FB528 (little-endian: 28 B5 2F FD)
        byte[] original = "frame magic test".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressor.compress(original, 0, original.length);
        assertTrue(compressed.length >= 4, "Compressed output too short for magic check");
        int magic = (compressed[0] & 0xFF)
                | ((compressed[1] & 0xFF) << 8)
                | ((compressed[2] & 0xFF) << 16)
                | ((compressed[3] & 0xFF) << 24);
        assertEquals(0xFD2FB528, magic, "Expected Zstd frame magic number");
    }
}
