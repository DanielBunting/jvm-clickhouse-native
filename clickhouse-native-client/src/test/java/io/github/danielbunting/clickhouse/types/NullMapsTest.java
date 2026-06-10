package io.github.danielbunting.clickhouse.types;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.protocol.BinaryWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Byte-vector and round-trip tests for {@link NullMaps}.
 *
 * <p>Uses minimal in-test {@link BinaryReader}/{@link BinaryWriter} implementations
 * backed by byte streams so the suite does not depend on classes produced by other
 * Wave 1 agents (DefaultBinaryReader/Writer, testutil.Bytes). Only the methods
 * exercised by {@link NullMaps} are implemented; the rest throw.
 */
class NullMapsTest {

    @Test
    void writeProducesOneBytePerRowWithExpectedValues() throws IOException {
        boolean[] nulls = {false, true, false, true, true};
        byte[] out = capture(w -> NullMaps.write(w, nulls, nulls.length));
        assertArrayEquals(new byte[] {0, 1, 0, 1, 1}, out);
    }

    @Test
    void readInterpretsOneBytePerRow() throws IOException {
        BinaryReader r = reader(new byte[] {0, 1, 0, 1, 1});
        boolean[] nulls = NullMaps.read(r, 5);
        assertArrayEquals(new boolean[] {false, true, false, true, true}, nulls);
    }

    @Test
    void readTreatsAnyNonZeroByteAsNull() throws IOException {
        BinaryReader r = reader(new byte[] {0, 2, (byte) 0xFF, 0});
        boolean[] nulls = NullMaps.read(r, 4);
        assertArrayEquals(new boolean[] {false, true, true, false}, nulls);
    }

    @Test
    void roundTripMixed() throws IOException {
        boolean[] in = {true, false, false, true, false, true, true, false};
        byte[] wire = capture(w -> NullMaps.write(w, in, in.length));
        assertEquals(in.length, wire.length);
        boolean[] out = NullMaps.read(reader(wire), in.length);
        assertArrayEquals(in, out);
    }

    @Test
    void roundTripAllNull() throws IOException {
        boolean[] in = new boolean[16];
        java.util.Arrays.fill(in, true);
        byte[] wire = capture(w -> NullMaps.write(w, in, in.length));
        for (byte b : wire) {
            assertEquals(1, b);
        }
        boolean[] out = NullMaps.read(reader(wire), in.length);
        for (boolean v : out) {
            assertTrue(v);
        }
    }

    @Test
    void roundTripNoneNull() throws IOException {
        boolean[] in = new boolean[16];
        byte[] wire = capture(w -> NullMaps.write(w, in, in.length));
        for (byte b : wire) {
            assertEquals(0, b);
        }
        boolean[] out = NullMaps.read(reader(wire), in.length);
        for (boolean v : out) {
            assertFalse(v);
        }
    }

    @Test
    void roundTripEmpty() throws IOException {
        byte[] wire = capture(w -> NullMaps.write(w, new boolean[0], 0));
        assertEquals(0, wire.length);
        boolean[] out = NullMaps.read(reader(wire), 0);
        assertEquals(0, out.length);
    }

    @Test
    void roundTripLarge() throws IOException {
        int n = 100_000;
        boolean[] in = new boolean[n];
        Random rnd = new Random(42);
        for (int i = 0; i < n; i++) {
            in[i] = rnd.nextBoolean();
        }
        byte[] wire = capture(w -> NullMaps.write(w, in, n));
        assertEquals(n, wire.length);
        boolean[] out = NullMaps.read(reader(wire), n);
        assertArrayEquals(in, out);
    }

    @Test
    void writeHonorsRowCountSmallerThanArray() throws IOException {
        boolean[] nulls = {true, true, true, true};
        byte[] out = capture(w -> NullMaps.write(w, nulls, 2));
        assertArrayEquals(new byte[] {1, 1}, out);
    }

    @Test
    void negativeRowCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> NullMaps.read(reader(new byte[0]), -1));
        assertThrows(IllegalArgumentException.class,
                () -> NullMaps.write(new ByteSinkWriter(new ByteArrayOutputStream()), new boolean[0], -1));
    }

    @Test
    void writeRejectsShortArray() {
        assertThrows(IllegalArgumentException.class,
                () -> NullMaps.write(new ByteSinkWriter(new ByteArrayOutputStream()), new boolean[2], 3));
    }

    // ---- minimal in-test helpers ----

    private static BinaryReader reader(byte[] bytes) {
        return new ByteSourceReader(new ByteArrayInputStream(bytes));
    }

    private interface WriterConsumer {
        void accept(BinaryWriter w) throws IOException;
    }

    private static byte[] capture(WriterConsumer fn) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteSinkWriter w = new ByteSinkWriter(baos);
        fn.accept(w);
        w.flush();
        return baos.toByteArray();
    }

    /** Reader backing only the methods {@link NullMaps} touches. */
    private static final class ByteSourceReader implements BinaryReader {
        private final ByteArrayInputStream in;

        ByteSourceReader(ByteArrayInputStream in) {
            this.in = in;
        }

        @Override
        public byte[] readBytes(int count) throws IOException {
            byte[] buf = new byte[count];
            readFully(buf, 0, count);
            return buf;
        }

        @Override
        public void readFully(byte[] dest, int offset, int length) throws IOException {
            int n = 0;
            while (n < length) {
                int r = in.read(dest, offset + n, length - n);
                if (r < 0) {
                    throw new IOException("unexpected EOF");
                }
                n += r;
            }
        }

        @Override public int readByteUnsigned() { throw new UnsupportedOperationException(); }
        @Override public byte readInt8() { throw new UnsupportedOperationException(); }
        @Override public short readInt16() { throw new UnsupportedOperationException(); }
        @Override public int readInt32() { throw new UnsupportedOperationException(); }
        @Override public long readInt64() { throw new UnsupportedOperationException(); }
        @Override public int readUInt8() { throw new UnsupportedOperationException(); }
        @Override public int readUInt16() { throw new UnsupportedOperationException(); }
        @Override public long readUInt32() { throw new UnsupportedOperationException(); }
        @Override public long readUInt64() { throw new UnsupportedOperationException(); }
        @Override public float readFloat32() { throw new UnsupportedOperationException(); }
        @Override public double readFloat64() { throw new UnsupportedOperationException(); }
        @Override public long readVarUInt() { throw new UnsupportedOperationException(); }
        @Override public long readVarInt() { throw new UnsupportedOperationException(); }
        @Override public String readString() { throw new UnsupportedOperationException(); }
    }

    /** Writer backing only the methods {@link NullMaps} touches. */
    private static final class ByteSinkWriter implements BinaryWriter {
        private final ByteArrayOutputStream out;

        ByteSinkWriter(ByteArrayOutputStream out) {
            this.out = out;
        }

        @Override
        public void writeBytes(byte[] src, int offset, int length) {
            out.write(src, offset, length);
        }

        @Override
        public void flush() {
            // ByteArrayOutputStream needs no flush.
        }

        @Override public void writeInt8(byte v) { throw new UnsupportedOperationException(); }
        @Override public void writeInt16(short v) { throw new UnsupportedOperationException(); }
        @Override public void writeInt32(int v) { throw new UnsupportedOperationException(); }
        @Override public void writeInt64(long v) { throw new UnsupportedOperationException(); }
        @Override public void writeUInt8(int v) { throw new UnsupportedOperationException(); }
        @Override public void writeUInt16(int v) { throw new UnsupportedOperationException(); }
        @Override public void writeUInt32(long v) { throw new UnsupportedOperationException(); }
        @Override public void writeUInt64(long v) { throw new UnsupportedOperationException(); }
        @Override public void writeFloat32(float v) { throw new UnsupportedOperationException(); }
        @Override public void writeFloat64(double v) { throw new UnsupportedOperationException(); }
        @Override public void writeVarUInt(long v) { throw new UnsupportedOperationException(); }
        @Override public void writeVarInt(long v) { throw new UnsupportedOperationException(); }
        @Override public void writeString(String v) { throw new UnsupportedOperationException(); }
    }
}
