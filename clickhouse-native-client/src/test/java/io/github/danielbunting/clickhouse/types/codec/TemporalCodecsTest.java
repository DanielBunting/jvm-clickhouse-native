package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.protocol.BinaryWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Byte-vector and round-trip tests for {@link DateCodec}, {@link DateTimeCodec},
 * and {@link DateTime64Codec}.
 *
 * <p>Uses minimal in-test {@link BinaryReader}/{@link BinaryWriter} implementations
 * backed by byte streams so this suite does not depend on classes produced by other
 * Wave 1 agents (DefaultBinaryReader/Writer, testutil.Bytes).
 */
class TemporalCodecsTest {

    // =========================================================================
    // DateCodec
    // =========================================================================

    @Test
    void dateCodec_epochProduces_zeroDays() throws IOException {
        DateCodec codec = new DateCodec();
        int[] src = codec.allocate(1);
        codec.set(src, 0, LocalDate.of(1970, 1, 1));
        assertEquals(0, src[0]);

        byte[] wire = capture(w -> codec.write(w, src, 1));
        // UInt16 LE encoding of 0 is two zero bytes
        assertArrayEquals(new byte[]{0, 0}, wire);

        int[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(LocalDate.of(1970, 1, 1), codec.get(dest, 0));
    }

    @Test
    void dateCodec_knownDate_roundTrip() throws IOException {
        DateCodec codec = new DateCodec();
        // 2024-01-15: toEpochDay() = 19737
        LocalDate date = LocalDate.of(2024, 1, 15);
        long expected = date.toEpochDay(); // should be 19737

        int[] src = codec.allocate(1);
        codec.set(src, 0, date);
        assertEquals(expected, src[0]);

        byte[] wire = capture(w -> codec.write(w, src, 1));
        // 19737 in UInt16 LE: 19737 = 0x4D19 → bytes [0x19, 0x4D]
        assertEquals(2, wire.length);
        assertEquals(0x19, wire[0] & 0xFF);
        assertEquals(0x4D, wire[1] & 0xFF);

        int[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(date, codec.get(dest, 0));
    }

    @Test
    void dateCodec_multipleRows_roundTrip() throws IOException {
        DateCodec codec = new DateCodec();
        LocalDate[] dates = {
            LocalDate.of(1970, 1, 1),
            LocalDate.of(2000, 3, 1),
            LocalDate.of(2023, 12, 31),
            LocalDate.of(2038, 1, 1),
        };

        int[] src = codec.allocate(dates.length);
        for (int i = 0; i < dates.length; i++) {
            codec.set(src, i, dates[i]);
        }

        byte[] wire = capture(w -> codec.write(w, src, dates.length));
        assertEquals(dates.length * 2, wire.length); // 2 bytes per UInt16

        int[] dest = codec.allocate(dates.length);
        codec.read(reader(wire), dates.length, dest);
        for (int i = 0; i < dates.length; i++) {
            assertEquals(dates[i], codec.get(dest, i),
                    "Mismatch at row " + i);
        }
    }

    @Test
    void dateCodec_nullValue_setsZero() {
        DateCodec codec = new DateCodec();
        int[] arr = codec.allocate(1);
        arr[0] = 999;
        codec.set(arr, 0, null);
        assertEquals(0, arr[0]);
    }

    @Test
    void dateCodec_metadata() {
        DateCodec codec = new DateCodec();
        assertEquals("Date", codec.typeName());
        assertEquals(LocalDate.class, codec.javaType());
    }

    @Test
    void dateCodec_wireBytes_exactValues() throws IOException {
        // Manually craft a 2-byte UInt16 LE wire payload for day 365 (0x016D → [0x6D, 0x01])
        // 365 = 0x016D; LE bytes: low byte first → 0x6D, 0x01
        byte[] wire = {0x6D, 0x01};
        DateCodec codec = new DateCodec();
        int[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(365, dest[0]);
        assertEquals(LocalDate.of(1971, 1, 1), codec.get(dest, 0));
    }

    // =========================================================================
    // DateTimeCodec
    // =========================================================================

    @Test
    void dateTimeCodec_epochProduces_zeroSeconds() throws IOException {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        long[] src = codec.allocate(1);
        codec.set(src, 0, Instant.EPOCH);
        assertEquals(0L, src[0]);

        byte[] wire = capture(w -> codec.write(w, src, 1));
        // UInt32 LE encoding of 0 is four zero bytes
        assertArrayEquals(new byte[]{0, 0, 0, 0}, wire);

        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(Instant.EPOCH, codec.get(dest, 0));
    }

    @Test
    void dateTimeCodec_knownTimestamp_roundTrip() throws IOException {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        // 2023-06-15T12:34:56Z → epoch second = 1686829696
        Instant ts = Instant.parse("2023-06-15T12:34:56Z");
        long epochSec = ts.getEpochSecond(); // 1686829696

        long[] src = codec.allocate(1);
        codec.set(src, 0, ts);
        assertEquals(epochSec, src[0]);

        byte[] wire = capture(w -> codec.write(w, src, 1));
        assertEquals(4, wire.length);

        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(ts, codec.get(dest, 0));
    }

    @Test
    void dateTimeCodec_subSecondTruncated() {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        // 1000.999999999 seconds since epoch — sub-second part must be dropped
        Instant ts = Instant.ofEpochSecond(1000, 999_999_999L);
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, ts);
        // stored as whole seconds only
        assertEquals(1000L, arr[0]);
        assertEquals(Instant.ofEpochSecond(1000), codec.get(arr, 0));
    }

    @Test
    void dateTimeCodec_nullZoneDefaultsToUtc() {
        DateTimeCodec codec = new DateTimeCodec(null);
        assertEquals(ZoneId.of("UTC"), codec.zoneId());
    }

    @Test
    void dateTimeCodec_zoneIdPreserved() {
        ZoneId zone = ZoneId.of("Europe/London");
        DateTimeCodec codec = new DateTimeCodec(zone);
        assertEquals(zone, codec.zoneId());
    }

    @Test
    void dateTimeCodec_wireBytes_exactValues() throws IOException {
        // UInt32 LE for 86400 (= 1 day): 0x00015180 → bytes [0x80, 0x51, 0x01, 0x00]
        byte[] wire = {(byte) 0x80, 0x51, 0x01, 0x00};
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(86400L, dest[0]);
        assertEquals(Instant.ofEpochSecond(86400), codec.get(dest, 0));
    }

    @Test
    void dateTimeCodec_multipleRows_roundTrip() throws IOException {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        Instant[] instants = {
            Instant.EPOCH,
            Instant.parse("2000-01-01T00:00:00Z"),
            Instant.parse("2023-12-31T23:59:59Z"),
        };

        long[] src = codec.allocate(instants.length);
        for (int i = 0; i < instants.length; i++) {
            codec.set(src, i, instants[i]);
        }

        byte[] wire = capture(w -> codec.write(w, src, instants.length));
        assertEquals(instants.length * 4, wire.length); // 4 bytes per UInt32

        long[] dest = codec.allocate(instants.length);
        codec.read(reader(wire), instants.length, dest);
        for (int i = 0; i < instants.length; i++) {
            assertEquals(instants[i], codec.get(dest, i),
                    "Mismatch at row " + i);
        }
    }

    @Test
    void dateTimeCodec_metadata() {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        assertEquals("DateTime", codec.typeName());
        assertEquals(Instant.class, codec.javaType());
    }

    @Test
    void dateTimeCodec_nullValue_setsZero() {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        long[] arr = codec.allocate(1);
        arr[0] = 999L;
        codec.set(arr, 0, null);
        assertEquals(0L, arr[0]);
    }

    // =========================================================================
    // DateTime64Codec
    // =========================================================================

    @Test
    void dateTime64Codec_precision0_epochZero() throws IOException {
        DateTime64Codec codec = new DateTime64Codec(0, ZoneId.of("UTC"));
        long[] src = codec.allocate(1);
        codec.set(src, 0, Instant.EPOCH);
        assertEquals(0L, src[0]);

        byte[] wire = capture(w -> codec.write(w, src, 1));
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}, wire);

        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(Instant.EPOCH, codec.get(dest, 0));
    }

    @Test
    void dateTime64Codec_precision3_millisecondRoundTrip() throws IOException {
        // precision=3 → each tick is 1ms
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        // 2023-06-15T12:34:56.789Z → epoch ms = 1686829696789
        Instant ts = Instant.parse("2023-06-15T12:34:56.789Z");
        long expectedTicks = ts.toEpochMilli(); // == epochSecond*1000 + ms

        long[] src = codec.allocate(1);
        codec.set(src, 0, ts);
        assertEquals(expectedTicks, src[0]);

        byte[] wire = capture(w -> codec.write(w, src, 1));
        assertEquals(8, wire.length);

        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        // Recovered instant must match original (ms precision → no sub-ms loss)
        assertEquals(ts, codec.get(dest, 0));
    }

    @Test
    void dateTime64Codec_precision6_microsecondRoundTrip() throws IOException {
        DateTime64Codec codec = new DateTime64Codec(6, ZoneId.of("UTC"));
        // 1000 seconds + 123456 microseconds
        Instant ts = Instant.ofEpochSecond(1000, 123_456_000L); // 123456 µs in ns
        long expectedTicks = 1000L * 1_000_000L + 123_456L;

        long[] src = codec.allocate(1);
        codec.set(src, 0, ts);
        assertEquals(expectedTicks, src[0]);

        long[] dest = codec.allocate(1);
        codec.read(reader(capture(w -> codec.write(w, src, 1))), 1, dest);
        assertEquals(ts, codec.get(dest, 0));
    }

    @Test
    void dateTime64Codec_precision9_nanosecondRoundTrip() throws IOException {
        DateTime64Codec codec = new DateTime64Codec(9, ZoneId.of("UTC"));
        // 500 seconds + 123456789 nanoseconds
        Instant ts = Instant.ofEpochSecond(500, 123_456_789L);
        long expectedTicks = 500L * 1_000_000_000L + 123_456_789L;

        long[] src = codec.allocate(1);
        codec.set(src, 0, ts);
        assertEquals(expectedTicks, src[0]);

        long[] dest = codec.allocate(1);
        codec.read(reader(capture(w -> codec.write(w, src, 1))), 1, dest);
        assertEquals(ts, codec.get(dest, 0));
    }

    @Test
    void dateTime64Codec_precision3_subMsPrecisionTruncated() {
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        // 1000 seconds + 999 microseconds (sub-ms) — should be truncated to 1000s + 0ms
        Instant ts = Instant.ofEpochSecond(1000, 999_000L); // 999 µs in ns
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, ts);
        // stored as 1000 * 1000 ticks (ms-precision), the 999 µs is below 1 ms
        assertEquals(1_000_000L, arr[0]);
        assertEquals(Instant.ofEpochSecond(1000, 0), codec.get(arr, 0));
    }

    @Test
    void dateTime64Codec_wireBytes_exactLayout() throws IOException {
        // Verify the wire is a little-endian Int64
        // Value 1000 (Int64 LE): 0x00000000000003E8 → bytes [0xE8, 0x03, 0, 0, 0, 0, 0, 0]
        DateTime64Codec codec = new DateTime64Codec(0, ZoneId.of("UTC"));
        long[] src = {1000L};
        byte[] wire = capture(w -> codec.write(w, src, 1));
        assertEquals(8, wire.length);
        assertEquals(0xE8, wire[0] & 0xFF);
        assertEquals(0x03, wire[1] & 0xFF);
        for (int i = 2; i < 8; i++) {
            assertEquals(0, wire[i] & 0xFF, "byte " + i + " should be 0");
        }

        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(1000L, dest[0]);
    }

    @Test
    void dateTime64Codec_nullZoneDefaultsToUtc() {
        DateTime64Codec codec = new DateTime64Codec(3, null);
        assertEquals(ZoneId.of("UTC"), codec.zoneId());
    }

    @Test
    void dateTime64Codec_invalidPrecisionThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DateTime64Codec(-1, ZoneId.of("UTC")));
        assertThrows(IllegalArgumentException.class, () -> new DateTime64Codec(10, ZoneId.of("UTC")));
    }

    @Test
    void dateTime64Codec_metadata() {
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        assertEquals("DateTime64", codec.typeName());
        assertEquals(Instant.class, codec.javaType());
        assertEquals(3, codec.precision());
    }

    @Test
    void dateTime64Codec_nullValue_setsZero() {
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        long[] arr = codec.allocate(1);
        arr[0] = 999L;
        codec.set(arr, 0, null);
        assertEquals(0L, arr[0]);
    }

    @Test
    void dateTime64Codec_multipleRows_roundTrip() throws IOException {
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        Instant[] instants = {
            Instant.EPOCH,
            Instant.parse("2023-01-01T00:00:00.123Z"),
            Instant.parse("2023-12-31T23:59:59.999Z"),
        };

        long[] src = codec.allocate(instants.length);
        for (int i = 0; i < instants.length; i++) {
            codec.set(src, i, instants[i]);
        }

        byte[] wire = capture(w -> codec.write(w, src, instants.length));
        assertEquals(instants.length * 8, wire.length);

        long[] dest = codec.allocate(instants.length);
        codec.read(reader(wire), instants.length, dest);
        for (int i = 0; i < instants.length; i++) {
            assertEquals(instants[i], codec.get(dest, i),
                    "Mismatch at row " + i);
        }
    }

    @Test
    void dateTime64Codec_precision3_knownTickValue() throws IOException {
        // Wire: Int64 LE value 1_686_829_696_789 (ms ticks for 2023-06-15T11:48:16.789Z)
        // Let's compute the LE bytes manually and verify read -> get pipeline
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        long ticks = 1_686_829_696_789L;
        // Write the known tick value directly
        long[] src = {ticks};
        byte[] wire = capture(w -> codec.write(w, src, 1));

        long[] dest = codec.allocate(1);
        codec.read(reader(wire), 1, dest);
        assertEquals(ticks, dest[0]);

        Instant recovered = (Instant) codec.get(dest, 0);
        assertEquals(Instant.parse("2023-06-15T11:48:16.789Z"), recovered);
    }

    // =========================================================================
    // Shared wire helpers (self-contained, no dependency on Wave-1 other tasks)
    // =========================================================================

    private interface WriterConsumer {
        void accept(BinaryWriter w) throws IOException;
    }

    private static byte[] capture(WriterConsumer fn) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryWriter w = new LeBinaryWriter(baos);
        fn.accept(w);
        w.flush();
        return baos.toByteArray();
    }

    private static BinaryReader reader(byte[] bytes) {
        return new LeBinaryReader(new ByteArrayInputStream(bytes));
    }

    /**
     * Minimal little-endian {@link BinaryReader} backed by a {@link ByteArrayInputStream}.
     * Only implements the methods exercised by the temporal codecs
     * (readUInt16, readUInt32, readInt64).
     */
    private static final class LeBinaryReader implements BinaryReader {
        private final ByteArrayInputStream in;

        LeBinaryReader(ByteArrayInputStream in) {
            this.in = in;
        }

        private int readByte() throws IOException {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF");
            }
            return b;
        }

        @Override
        public int readByteUnsigned() throws IOException {
            return readByte();
        }

        @Override
        public byte readInt8() throws IOException {
            return (byte) readByte();
        }

        @Override
        public short readInt16() throws IOException {
            int lo = readByte();
            int hi = readByte();
            return (short) (lo | (hi << 8));
        }

        @Override
        public int readInt32() throws IOException {
            int b0 = readByte();
            int b1 = readByte();
            int b2 = readByte();
            int b3 = readByte();
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }

        @Override
        public long readInt64() throws IOException {
            long lo = readInt32() & 0xFFFFFFFFL;
            long hi = readInt32() & 0xFFFFFFFFL;
            return lo | (hi << 32);
        }

        @Override
        public int readUInt8() throws IOException {
            return readByte();
        }

        @Override
        public int readUInt16() throws IOException {
            int lo = readByte();
            int hi = readByte();
            return lo | (hi << 8);
        }

        @Override
        public long readUInt32() throws IOException {
            long b0 = readByte();
            long b1 = readByte();
            long b2 = readByte();
            long b3 = readByte();
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }

        @Override
        public long readUInt64() throws IOException {
            return readInt64();
        }

        @Override
        public float readFloat32() throws IOException {
            return Float.intBitsToFloat(readInt32());
        }

        @Override
        public double readFloat64() throws IOException {
            return Double.longBitsToDouble(readInt64());
        }

        @Override
        public long readVarUInt() { throw new UnsupportedOperationException(); }

        @Override
        public long readVarInt() { throw new UnsupportedOperationException(); }

        @Override
        public String readString() { throw new UnsupportedOperationException(); }

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
    }

    /**
     * Minimal little-endian {@link BinaryWriter} backed by a {@link ByteArrayOutputStream}.
     * Only implements the methods exercised by the temporal codecs
     * (writeUInt16, writeUInt32, writeInt64).
     */
    private static final class LeBinaryWriter implements BinaryWriter {
        private final ByteArrayOutputStream out;

        LeBinaryWriter(ByteArrayOutputStream out) {
            this.out = out;
        }

        @Override
        public void writeInt8(byte v) {
            out.write(v & 0xFF);
        }

        @Override
        public void writeInt16(short v) {
            out.write(v & 0xFF);
            out.write((v >> 8) & 0xFF);
        }

        @Override
        public void writeInt32(int v) {
            out.write(v & 0xFF);
            out.write((v >> 8) & 0xFF);
            out.write((v >> 16) & 0xFF);
            out.write((v >> 24) & 0xFF);
        }

        @Override
        public void writeInt64(long v) {
            writeInt32((int) (v & 0xFFFFFFFFL));
            writeInt32((int) ((v >> 32) & 0xFFFFFFFFL));
        }

        @Override
        public void writeUInt8(int v) {
            out.write(v & 0xFF);
        }

        @Override
        public void writeUInt16(int v) {
            out.write(v & 0xFF);
            out.write((v >> 8) & 0xFF);
        }

        @Override
        public void writeUInt32(long v) {
            out.write((int) (v & 0xFF));
            out.write((int) ((v >> 8) & 0xFF));
            out.write((int) ((v >> 16) & 0xFF));
            out.write((int) ((v >> 24) & 0xFF));
        }

        @Override
        public void writeUInt64(long v) {
            writeInt64(v);
        }

        @Override
        public void writeFloat32(float v) {
            writeInt32(Float.floatToIntBits(v));
        }

        @Override
        public void writeFloat64(double v) {
            writeInt64(Double.doubleToLongBits(v));
        }

        @Override
        public void writeVarUInt(long v) { throw new UnsupportedOperationException(); }

        @Override
        public void writeVarInt(long v) { throw new UnsupportedOperationException(); }

        @Override
        public void writeString(String v) { throw new UnsupportedOperationException(); }

        @Override
        public void writeBytes(byte[] src, int offset, int length) {
            out.write(src, offset, length);
        }

        @Override
        public void flush() {
            // ByteArrayOutputStream needs no flush.
        }
    }
}
