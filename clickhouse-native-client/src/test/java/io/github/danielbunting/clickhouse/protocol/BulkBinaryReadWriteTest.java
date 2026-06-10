package io.github.danielbunting.clickhouse.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

/**
 * Verifies the bulk {@code readInto}/{@code writeFrom} paths (improvement 04)
 * produce byte-identical output to the per-element path and decode back exactly,
 * including explicit little-endian byte order and high-bit / extreme edge values.
 *
 * <p>The {@code default} interface methods on {@link BinaryReader}/{@link BinaryWriter}
 * loop the per-value methods; {@link DefaultBinaryReader}/{@link DefaultBinaryWriter}
 * override them with a single ByteBuffer transfer. Asserting that the bytes a bulk
 * write produces match a per-element write — and that a bulk read of those bytes
 * matches the source array — proves the two paths are wire-compatible.
 */
final class BulkBinaryReadWriteTest {

    private static byte[] bulkWrite(WriterAction action) throws IOException {
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

    // ----- long[] / Int64 / UInt64 -----

    @Test
    void longBulkIsLittleEndianAndRoundTrips() throws IOException {
        long[] src = {0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE,
                0x0102030405060708L, 0x80000000_00000000L, 0xFFFFFFFF_00000000L};

        byte[] bulk = bulkWrite(w -> w.writeFrom(src, src.length));

        // Identical to per-element writes.
        byte[] perElem = bulkWrite(w -> {
            for (long v : src) {
                w.writeInt64(v);
            }
        });
        assertArrayEquals(perElem, bulk);

        // Explicit little-endian byte layout for a known value.
        assertArrayEquals(
                new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
                slice(bulk, 5 * 8, 8));
        assertEquals(0x0102030405060708L,
                ByteBuffer.wrap(slice(bulk, 5 * 8, 8)).order(ByteOrder.LITTLE_ENDIAN).getLong());

        // Bulk read reproduces the source exactly.
        long[] dst = new long[src.length];
        reader(bulk).readInto(dst, src.length);
        assertArrayEquals(src, dst);
    }

    // ----- int[] / Int32 -----

    @Test
    void intBulkIsLittleEndianAndRoundTrips() throws IOException {
        int[] src = {0, -1, 1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0x01020304, 0x80000000};

        byte[] bulk = bulkWrite(w -> w.writeFrom(src, src.length));
        byte[] perElem = bulkWrite(w -> {
            for (int v : src) {
                w.writeInt32(v);
            }
        });
        assertArrayEquals(perElem, bulk);
        assertArrayEquals(new byte[] {0x04, 0x03, 0x02, 0x01}, slice(bulk, 5 * 4, 4));

        int[] dst = new int[src.length];
        reader(bulk).readInto(dst, src.length);
        assertArrayEquals(src, dst);
    }

    // ----- short[] / Int16 -----

    @Test
    void shortBulkIsLittleEndianAndRoundTrips() throws IOException {
        short[] src = {0, -1, 1, Short.MIN_VALUE, Short.MAX_VALUE, 0x0102, (short) 0x8000};

        byte[] bulk = bulkWrite(w -> w.writeFrom(src, src.length));
        byte[] perElem = bulkWrite(w -> {
            for (short v : src) {
                w.writeInt16(v);
            }
        });
        assertArrayEquals(perElem, bulk);
        assertArrayEquals(new byte[] {0x02, 0x01}, slice(bulk, 5 * 2, 2));

        short[] dst = new short[src.length];
        reader(bulk).readInto(dst, src.length);
        assertArrayEquals(src, dst);
    }

    // ----- double[] / Float64 -----

    @Test
    void doubleBulkIsLittleEndianAndRoundTrips() throws IOException {
        double[] src = {0.0, -0.0, 1.5, -1.5, Double.MIN_VALUE, Double.MAX_VALUE,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN};

        byte[] bulk = bulkWrite(w -> w.writeFrom(src, src.length));
        byte[] perElem = bulkWrite(w -> {
            for (double v : src) {
                w.writeFloat64(v);
            }
        });
        assertArrayEquals(perElem, bulk);

        // 1.5 as little-endian IEEE-754.
        byte[] expected15 = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putDouble(1.5).array();
        assertArrayEquals(expected15, slice(bulk, 2 * 8, 8));

        double[] dst = new double[src.length];
        reader(bulk).readInto(dst, src.length);
        assertArrayEquals(src, dst);
    }

    // ----- float[] / Float32 -----

    @Test
    void floatBulkIsLittleEndianAndRoundTrips() throws IOException {
        float[] src = {0.0f, -0.0f, 1.5f, -1.5f, Float.MIN_VALUE, Float.MAX_VALUE,
                Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN};

        byte[] bulk = bulkWrite(w -> w.writeFrom(src, src.length));
        byte[] perElem = bulkWrite(w -> {
            for (float v : src) {
                w.writeFloat32(v);
            }
        });
        assertArrayEquals(perElem, bulk);

        byte[] expected15 = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
                .putFloat(1.5f).array();
        assertArrayEquals(expected15, slice(bulk, 2 * 4, 4));

        float[] dst = new float[src.length];
        reader(bulk).readInto(dst, src.length);
        assertArrayEquals(src, dst);
    }

    // ----- Large run crossing the internal chunk boundary -----

    @Test
    void largeLongRunCrossesChunkBoundary() throws IOException {
        // > 64 KiB worth of longs (8 KiB values) to exercise the chunked path.
        int n = 20_000;
        long[] src = new long[n];
        for (int i = 0; i < n; i++) {
            src[i] = ((long) i << 40) ^ (i * 0x9E3779B97F4A7C15L) ^ -(i & 1L);
        }

        byte[] bulk = bulkWrite(w -> w.writeFrom(src, n));
        byte[] perElem = bulkWrite(w -> {
            for (long v : src) {
                w.writeInt64(v);
            }
        });
        assertArrayEquals(perElem, bulk);

        long[] dst = new long[n];
        reader(bulk).readInto(dst, n);
        assertArrayEquals(src, dst);
    }

    private static byte[] slice(byte[] src, int off, int len) {
        byte[] out = new byte[len];
        System.arraycopy(src, off, out, 0, len);
        return out;
    }
}
