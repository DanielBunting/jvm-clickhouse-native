package io.github.danielbunting.clickhouse.types.codec;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the non-boxing {@code getLong}/{@code setLong} and
 * {@code getDouble}/{@code setDouble} accessors added to the {@code ColumnCodec}
 * contract (improvements 01 + 02).
 *
 * <p>For each numeric/temporal codec these assert a primitive round-trip and that
 * {@code getLong}/{@code getDouble} numerically agrees with what the boxed
 * {@code get()}/{@code value()} would produce, including the documented
 * unsigned-raw-bits and temporal-numeric semantics.
 */
class PrimitiveAccessorTest {

    // -----------------------------------------------------------------------
    // Signed integers
    // -----------------------------------------------------------------------

    @Test
    void int8_getSetLong_roundTrip() {
        Int8Codec codec = new Int8Codec();
        byte[] a = codec.allocate(3);
        codec.setLong(a, 0, Byte.MIN_VALUE);
        codec.setLong(a, 1, 0);
        codec.setLong(a, 2, Byte.MAX_VALUE);
        assertEquals(Byte.MIN_VALUE, codec.getLong(a, 0));
        assertEquals(0, codec.getLong(a, 1));
        assertEquals(Byte.MAX_VALUE, codec.getLong(a, 2));
        assertEquals(((Number) codec.get(a, 0)).longValue(), codec.getLong(a, 0));
    }

    @Test
    void int16_getSetLong_roundTrip() {
        Int16Codec codec = new Int16Codec();
        short[] a = codec.allocate(2);
        codec.setLong(a, 0, Short.MIN_VALUE);
        codec.setLong(a, 1, Short.MAX_VALUE);
        assertEquals(Short.MIN_VALUE, codec.getLong(a, 0));
        assertEquals(Short.MAX_VALUE, codec.getLong(a, 1));
    }

    @Test
    void int32_getSetLong_roundTrip() {
        Int32Codec codec = new Int32Codec();
        int[] a = codec.allocate(2);
        codec.setLong(a, 0, Integer.MIN_VALUE);
        codec.setLong(a, 1, Integer.MAX_VALUE);
        assertEquals(Integer.MIN_VALUE, codec.getLong(a, 0));
        assertEquals(Integer.MAX_VALUE, codec.getLong(a, 1));
    }

    @Test
    void int64_getSetLong_minMax() {
        Int64Codec codec = new Int64Codec();
        long[] a = codec.allocate(3);
        codec.setLong(a, 0, Long.MIN_VALUE);
        codec.setLong(a, 1, 0L);
        codec.setLong(a, 2, Long.MAX_VALUE);
        assertEquals(Long.MIN_VALUE, codec.getLong(a, 0));
        assertEquals(0L, codec.getLong(a, 1));
        assertEquals(Long.MAX_VALUE, codec.getLong(a, 2));
        assertEquals(((Number) codec.get(a, 2)).longValue(), codec.getLong(a, 2));
    }

    // -----------------------------------------------------------------------
    // Unsigned integers
    // -----------------------------------------------------------------------

    @Test
    void uint8_getSetLong_widensCleanly() {
        UInt8Codec codec = new UInt8Codec();
        int[] a = codec.allocate(2);
        codec.setLong(a, 0, 0);
        codec.setLong(a, 1, 255);
        assertEquals(0L, codec.getLong(a, 0));
        assertEquals(255L, codec.getLong(a, 1));
    }

    @Test
    void uint16_getSetLong_widensCleanly() {
        UInt16Codec codec = new UInt16Codec();
        int[] a = codec.allocate(1);
        codec.setLong(a, 0, 65535);
        assertEquals(65535L, codec.getLong(a, 0));
    }

    @Test
    void uint32_getSetLong_atMax() {
        UInt32Codec codec = new UInt32Codec();
        long[] a = codec.allocate(1);
        long max = (1L << 32) - 1; // 2^32 - 1
        codec.setLong(a, 0, max);
        assertEquals(max, codec.getLong(a, 0));
        assertEquals(((Number) codec.get(a, 0)).longValue(), codec.getLong(a, 0));
    }

    @Test
    void uint64_getSetLong_rawBitsHighBitSet() {
        UInt64Codec codec = new UInt64Codec();
        long[] a = codec.allocate(1);
        // A value above Long.MAX_VALUE: stored as raw bits (a negative signed long).
        long raw = 0x8000_0000_0000_0001L; // 2^63 + 1, unsigned
        codec.setLong(a, 0, raw);
        // getLong returns the raw bits as-is.
        assertEquals(raw, codec.getLong(a, 0));
        // Boxed get() returns the same raw bits boxed as Long.
        assertEquals((Long) raw, codec.get(a, 0));
        // Unsigned interpretation is the caller's responsibility.
        assertEquals("9223372036854775809", Long.toUnsignedString(codec.getLong(a, 0)));
    }

    // -----------------------------------------------------------------------
    // Enums (raw ordinal via getLong)
    // -----------------------------------------------------------------------

    @Test
    void enum8_getSetLong_rawOrdinal() {
        Enum8Codec codec = new Enum8Codec(Map.of(1, "active", 2, "inactive"));
        int[] a = codec.allocate(1);
        codec.setLong(a, 0, 2);
        assertEquals(2L, codec.getLong(a, 0));
        assertEquals("inactive", codec.get(a, 0)); // boxed get still maps to name
    }

    @Test
    void enum16_getSetLong_rawOrdinal() {
        Enum16Codec codec = new Enum16Codec(Map.of(-5, "lo", 1000, "hi"));
        int[] a = codec.allocate(1);
        codec.setLong(a, 0, -5);
        assertEquals(-5L, codec.getLong(a, 0));
        assertEquals("lo", codec.get(a, 0));
    }

    // -----------------------------------------------------------------------
    // Temporal: numeric semantics (stored value, not re-derived)
    // -----------------------------------------------------------------------

    @Test
    void date_getLong_returnsStoredDays() {
        DateCodec codec = new DateCodec();
        int[] a = codec.allocate(1);
        LocalDate date = LocalDate.of(2021, 6, 15);
        long days = date.toEpochDay();
        codec.setLong(a, 0, days);
        assertEquals(days, codec.getLong(a, 0));
        // Boxed get() re-derives the LocalDate from the same stored days.
        assertEquals(date, codec.get(a, 0));
    }

    @Test
    void dateTime_getLong_returnsStoredEpochSeconds() {
        DateTimeCodec codec = new DateTimeCodec(ZoneId.of("UTC"));
        long[] a = codec.allocate(1);
        long epoch = 1_700_000_000L;
        codec.setLong(a, 0, epoch);
        assertEquals(epoch, codec.getLong(a, 0));
        assertEquals(Instant.ofEpochSecond(epoch), codec.get(a, 0));
    }

    @Test
    void dateTime64_getLong_returnsStoredTicks() {
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC")); // millis
        long[] a = codec.allocate(1);
        long ticks = 1_700_000_000_123L; // ms ticks
        codec.setLong(a, 0, ticks);
        assertEquals(ticks, codec.getLong(a, 0));
        assertEquals(Instant.ofEpochSecond(1_700_000_000L, 123_000_000L), codec.get(a, 0));
    }

    // -----------------------------------------------------------------------
    // Floats: getDouble / setDouble
    // -----------------------------------------------------------------------

    @Test
    void float32_getDouble_widens() {
        Float32Codec codec = new Float32Codec();
        float[] a = codec.allocate(1);
        codec.setDouble(a, 0, 1.5);
        assertEquals(1.5, codec.getDouble(a, 0), 0.0);
        // Widening from the stored float matches the boxed Float widened to double.
        assertEquals(((Number) codec.get(a, 0)).doubleValue(), codec.getDouble(a, 0), 0.0);
    }

    @Test
    void float32_getDouble_precisionFloat() {
        Float32Codec codec = new Float32Codec();
        float[] a = codec.allocate(1);
        float v = 0.1f;
        codec.setDouble(a, 0, v);
        assertEquals((double) v, codec.getDouble(a, 0), 0.0);
    }

    @Test
    void float64_getSetDouble_roundTrip() {
        Float64Codec codec = new Float64Codec();
        double[] a = codec.allocate(2);
        codec.setDouble(a, 0, Math.PI);
        codec.setDouble(a, 1, -1.0e300);
        assertEquals(Math.PI, codec.getDouble(a, 0), 0.0);
        assertEquals(-1.0e300, codec.getDouble(a, 1), 0.0);
    }

    // -----------------------------------------------------------------------
    // Reference codecs keep the boxed default (no primitive override)
    // -----------------------------------------------------------------------

    @Test
    void string_getLong_throwsViaBoxedDefault() {
        StringColumnCodec codec = new StringColumnCodec();
        StringColumn a = codec.allocate(1);
        codec.set(a, 0, "hello");
        // Default getLong casts get() to Number, which fails for a String.
        assertThrows(ClassCastException.class, () -> codec.getLong(a, 0));
    }
}
