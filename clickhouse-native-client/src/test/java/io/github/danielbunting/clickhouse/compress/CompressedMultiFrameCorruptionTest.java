package io.github.danielbunting.clickhouse.compress;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

/**
 * Robustness of {@link CompressedFrameInputStream} across a frame <em>boundary</em>.
 *
 * <p>{@code CompressedBlockCodecTest} / {@code CompressedFrameRobustnessTest} cover a
 * single frame's checksum and header validation, and {@code CompressionRoundTripIT}
 * covers the happy multi-frame reassembly against a live server. The remaining gap —
 * exercised here — is a multi-frame stream whose <em>second</em> frame is corrupt: the
 * first frame must decode cleanly and the failure must surface only when the reader
 * advances across the boundary into the tampered frame.
 */
class CompressedMultiFrameCorruptionTest {

    /** A byte pattern of {@code len} bytes seeded from {@code seed}, distinct per frame. */
    private static byte[] pattern(int len, int seed) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) ((i * 31 + seed) & 0xFF);
        }
        return b;
    }

    private static byte[] frame(byte[] raw) {
        return Bytes.capture(w -> CompressedBlockCodec.write(w, raw, new Lz4Compressor()));
    }

    @Test
    void twoValidFrames_reassembleToConcatenatedBlocks() throws IOException {
        byte[] raw1 = pattern(200, 1);
        byte[] raw2 = pattern(150, 2);
        byte[] wire = concat(frame(raw1), frame(raw2));

        InputStream in = new CompressedFrameInputStream(Bytes.reader(wire));
        byte[] all = in.readAllBytes();

        assertArrayEquals(concat(raw1, raw2), all,
                "multi-frame stream yields the concatenation of both decompressed blocks");
    }

    @Test
    void corruptSecondFrame_firstFrameReadsThenBoundaryCrossingThrows() throws IOException {
        byte[] raw1 = pattern(200, 1);
        byte[] raw2 = pattern(150, 2);
        byte[] frame1 = frame(raw1);
        byte[] wire = concat(frame1, frame(raw2));
        // Flip a byte in the second frame's CityHash128 checksum (its first byte sits exactly
        // at the end of frame 1). The first frame stays byte-for-byte valid.
        wire[frame1.length] ^= (byte) 0xFF;

        InputStream in = new CompressedFrameInputStream(Bytes.reader(wire));

        // Frame 1 decodes cleanly and serves all of raw1's bytes...
        byte[] firstBlock = in.readNBytes(raw1.length);
        assertArrayEquals(raw1, firstBlock, "the intact first frame is fully readable");

        // ...and only crossing into the tampered second frame surfaces the checksum failure.
        assertThrows(ClickHouseException.class, in::read,
                "advancing into the corrupt second frame fails the checksum");
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
