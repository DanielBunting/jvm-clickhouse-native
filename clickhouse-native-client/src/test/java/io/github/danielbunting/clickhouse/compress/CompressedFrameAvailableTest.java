package io.github.danielbunting.clickhouse.compress;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.testutil.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * {@link CompressedFrameInputStream#available()} contract across LZ4 frame boundaries
 * (reference: Lz4InputStreamTest#testReadByteWithAvailable / #testReadWithAvailable /
 * #testReadBytesWithAvailable — read loops gated on {@code available()}).
 *
 * <p>{@code available()} reports the undelivered remainder of the CURRENT decompressed
 * frame only ({@code limit - pos}); it is 0 before the first read and at each frame
 * boundary, and never looks ahead into the next frame (the stream must not prefetch —
 * see the class KDoc). So a consumer loop gated purely on {@code available()} must probe
 * with a read at each boundary; these tests pin exactly that interplay.
 */
class CompressedFrameAvailableTest {

    private static byte[] pattern(int len, int seed) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) ((i * 31 + seed) & 0xFF);
        }
        return b;
    }

    private static byte[] wire(byte[]... blocks) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] raw : blocks) {
            out.writeBytes(Bytes.capture(w -> CompressedBlockCodec.write(w, raw, new Lz4Compressor())));
        }
        return out.toByteArray();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    @Test
    void availableIsZeroBeforeFirstRead_thenTracksCurrentFrameRemainder() throws IOException {
        byte[] raw = pattern(100, 7);
        CompressedFrameInputStream in = new CompressedFrameInputStream(Bytes.reader(wire(raw)));

        assertEquals(0, in.available(), "no frame loaded before the first read");
        assertEquals(raw[0] & 0xFF, in.read(), "first byte");
        assertEquals(99, in.available(), "remainder of the current frame after one byte");

        byte[] rest = in.readNBytes(99);
        assertArrayEquals(java.util.Arrays.copyOfRange(raw, 1, 100), rest);
        assertEquals(0, in.available(), "frame fully served");
        assertEquals(-1, in.read(), "EOF after the only frame");
    }

    @Test
    void byteAtATimeReadGatedOnAvailable_reassemblesAcrossFrames() throws IOException {
        byte[] raw1 = pattern(64, 1);
        byte[] raw2 = pattern(48, 2);
        CompressedFrameInputStream in =
                new CompressedFrameInputStream(Bytes.reader(wire(raw1, raw2)));

        ByteArrayOutputStream got = new ByteArrayOutputStream();
        // available() is 0 at every frame boundary, so the loop probes with one read()
        // and then drains exactly available() bytes one at a time.
        int probe;
        while ((probe = in.read()) != -1) {
            got.write(probe);
            for (int remaining = in.available(); remaining > 0; remaining--) {
                got.write(in.read());
            }
            assertEquals(0, in.available(), "frame drained before the next probe");
        }
        assertArrayEquals(concat(raw1, raw2), got.toByteArray());
    }

    @Test
    void bufferedReadsSizedByAvailable_reassembleAcrossFrames() throws IOException {
        byte[] raw1 = pattern(200, 3);
        byte[] raw2 = pattern(150, 4);
        CompressedFrameInputStream in =
                new CompressedFrameInputStream(Bytes.reader(wire(raw1, raw2)));

        ByteArrayOutputStream got = new ByteArrayOutputStream();
        byte[] probe = new byte[1];
        while (in.read(probe, 0, 1) != -1) {
            got.write(probe[0]);
            int remaining = in.available();
            if (remaining > 0) {
                byte[] buf = new byte[remaining];
                int n = in.read(buf, 0, buf.length);
                assertEquals(remaining, n,
                        "a read sized by available() is served in full from the current frame");
                got.write(buf, 0, n);
            }
        }
        assertArrayEquals(concat(raw1, raw2), got.toByteArray());
    }

    @Test
    void readNeverServesAcrossAFrameBoundaryInOneCall() throws IOException {
        byte[] raw1 = pattern(30, 5);
        byte[] raw2 = pattern(30, 6);
        CompressedFrameInputStream in =
                new CompressedFrameInputStream(Bytes.reader(wire(raw1, raw2)));

        // Ask for more than frame 1 holds: the call returns only frame 1's bytes.
        byte[] buf = new byte[60];
        int n = in.read(buf, 0, 60);
        assertEquals(30, n, "a single read() stops at the frame boundary");
        assertArrayEquals(raw1, java.util.Arrays.copyOf(buf, 30));
        assertEquals(0, in.available(), "boundary reached");

        int n2 = in.read(buf, 0, 60);
        assertEquals(30, n2, "next call loads and serves frame 2");
        assertArrayEquals(raw2, java.util.Arrays.copyOf(buf, 30));
        assertTrue(in.read() == -1, "clean EOF after both frames");
    }
}
