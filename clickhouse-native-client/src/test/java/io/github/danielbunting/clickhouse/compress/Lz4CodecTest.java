package io.github.danielbunting.clickhouse.compress;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Round-trip and byte-vector tests for {@link Lz4Compressor} and
 * {@link Lz4Decompressor}. No running ClickHouse server is required.
 */
final class Lz4CodecTest {

    private final Lz4Compressor compressor = new Lz4Compressor();
    private final Lz4Decompressor decompressor = new Lz4Decompressor();

    // ----- method() contract -----

    @Test
    void compressorMethodIsLz4() {
        assertEquals(CompressionMethod.LZ4, compressor.method());
    }

    @Test
    void decompressorMethodIsLz4() {
        assertEquals(CompressionMethod.LZ4, decompressor.method());
    }

    // ----- Empty input -----

    @Test
    void compressEmptyInputReturnsEmptyArray() {
        byte[] result = compressor.compress(new byte[0], 0, 0);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void decompressEmptyInputReturnsEmptyArray() {
        byte[] result = decompressor.decompress(new byte[0], 0, 0, 0);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void roundTripEmpty() {
        byte[] original = new byte[0];
        byte[] compressed = compressor.compress(original, 0, 0);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, 0);
        assertArrayEquals(original, restored);
    }

    // ----- Single-byte input -----

    @Test
    void roundTripSingleByte() {
        byte[] original = {(byte) 0xAB};
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored);
    }

    // ----- Highly compressible data (repeated pattern) -----

    @Test
    void roundTripAllZeros() {
        byte[] original = new byte[65536]; // 64 KiB of zeros
        byte[] compressed = compressor.compress(original, 0, original.length);
        // Highly compressible — compressed must be smaller than original
        assertTrue(compressed.length < original.length,
                "Expected compressed size " + compressed.length
                        + " < original size " + original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored);
    }

    @Test
    void roundTripRepeatedPattern() {
        // Create a buffer with a short repeating pattern
        byte[] original = new byte[32768];
        byte[] pattern = "ClickHouseNative".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < original.length; i++) {
            original[i] = pattern[i % pattern.length];
        }
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored);
    }

    // ----- Incompressible data (random bytes) -----

    @Test
    void roundTripRandomData() {
        byte[] original = new byte[16384];
        new Random(0xDEADBEEFL).nextBytes(original);
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 13, 255, 256, 1023, 4096, 65535})
    void roundTripVariedSizes(int size) {
        byte[] original = new byte[size];
        // Fill with a mix of random and structured bytes to exercise both paths
        Random rng = new Random(size);
        rng.nextBytes(original);
        // Overwrite the first quarter with a compressible pattern
        Arrays.fill(original, 0, size / 4, (byte) 0x42);

        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored,
                "Round-trip failed for size=" + size);
    }

    // ----- Offset / partial-array compress -----

    @Test
    void compressWithOffset() {
        // Prepend guard bytes; compress only the payload portion
        byte[] guard = {(byte) 0xFF, (byte) 0xFE};
        byte[] payload = "Hello, ClickHouse!".getBytes(StandardCharsets.UTF_8);
        byte[] src = new byte[guard.length + payload.length];
        System.arraycopy(guard, 0, src, 0, guard.length);
        System.arraycopy(payload, 0, src, guard.length, payload.length);

        byte[] compressed = compressor.compress(src, guard.length, payload.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, payload.length);
        assertArrayEquals(payload, restored);
    }

    @Test
    void decompressWithOffset() {
        byte[] original = "decompressOffset".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressor.compress(original, 0, original.length);

        // Prepend a guard byte to the compressed data
        byte[] withGuard = new byte[1 + compressed.length];
        withGuard[0] = (byte) 0xDE;
        System.arraycopy(compressed, 0, withGuard, 1, compressed.length);

        byte[] restored = decompressor.decompress(withGuard, 1, compressed.length, original.length);
        assertArrayEquals(original, restored);
    }

    // ----- Large block (matches typical ClickHouse block size ~1 MiB) -----

    @Test
    void roundTripOneMegabyte() {
        byte[] original = new byte[1 << 20]; // 1 MiB
        Random rng = new Random(42L);
        // Fill with random data first, then overwrite first 75% with zeros (compressible)
        rng.nextBytes(original);
        Arrays.fill(original, 0, (original.length * 3) / 4, (byte) 0x00);

        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored);
    }

    // ----- Output is sized exactly (no extra trailing bytes) -----

    @Test
    void compressedOutputIsExactlySized() {
        byte[] original = "exact size check".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressor.compress(original, 0, original.length);
        // Decompressor must produce exactly original.length bytes — no more, no less
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertEquals(original.length, restored.length);
        assertArrayEquals(original, restored);
    }

    @Test
    void decompressedOutputIsExactlySized() {
        byte[] original = new byte[512];
        Arrays.fill(original, (byte) 0x7F);
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertEquals(512, restored.length);
    }

    // ----- UTF-8 text payload -----

    @Test
    void roundTripUtf8Text() {
        String text = "SELECT 1; -- ClickHouse native protocol test\n"
                + "数据库 データベース 데이터베이스\n"
                + "Ünïcödé 😀🎉\n";
        byte[] original = text.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressor.compress(original, 0, original.length);
        byte[] restored = decompressor.decompress(compressed, 0, compressed.length, original.length);
        assertArrayEquals(original, restored);
        assertEquals(text, new String(restored, StandardCharsets.UTF_8));
    }

}
