package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Range-validation tests for {@link DateTimeCodec} and {@link DateTime64Codec}
 * (companion to {@link DateRangeValidationTest}; reference:
 * BinaryStreamUtilsTest#testWriteDateTime32, which rejects pre-epoch and beyond-max).
 *
 * <p>{@code DateTime} is an unsigned 32-bit epoch-second count
 * (1970-01-01T00:00:00Z..2106-02-07T06:28:15Z): an out-of-range {@link Instant} must be
 * rejected at {@code set} time rather than silently wrapped by the UInt32 wire cast.
 * {@code DateTime64} is a SIGNED Int64 tick count — pre-epoch values are valid by design
 * (see {@code DateTime64PrecisionTest#dateTime64_preEpochNegativeTicks}) — so its only
 * hazard is Int64 tick overflow (±292 years at precision 9), which must also fail fast.
 *
 * <p>Validating at {@code set} (not {@code write}) keeps the failure BEFORE any bytes are
 * streamed, so a bad value cannot poison an in-flight insert block.
 */
class DateTimeRangeValidationTest {

    private static final long MAX_EPOCH_SECOND = 0xFFFF_FFFFL; // 2106-02-07T06:28:15Z

    // ----- DateTime (UInt32 seconds) -----

    @Test
    void dateTime_rejectsPreEpochInstant() {
        DateTimeCodec codec = new DateTimeCodec(null);
        long[] arr = codec.allocate(1);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, Instant.parse("1969-12-31T23:59:59Z")),
                "DateTime cannot represent instants before the epoch");
        assertTrue(ex.getMessage().contains("1969-12-31T23:59:59Z"),
                "message names the offending value: " + ex.getMessage());
    }

    @Test
    void dateTime_rejectsInstantBeyondUInt32Max() {
        DateTimeCodec codec = new DateTimeCodec(null);
        long[] arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, Instant.ofEpochSecond(MAX_EPOCH_SECOND + 1)),
                "DateTime cannot represent instants after 2106-02-07T06:28:15Z");
    }

    @Test
    void dateTime_acceptsBoundaryValues() {
        DateTimeCodec codec = new DateTimeCodec(null);
        long[] arr = codec.allocate(1);
        assertDoesNotThrow(() -> codec.set(arr, 0, Instant.EPOCH));
        assertDoesNotThrow(() -> codec.set(arr, 0, Instant.ofEpochSecond(MAX_EPOCH_SECOND)));
        assertEquals(Instant.ofEpochSecond(MAX_EPOCH_SECOND), codec.get(arr, 0),
                "boundary value round-trips exactly");
    }

    // ----- DateTime64 (signed Int64 ticks; pre-epoch is valid, overflow is not) -----

    @Test
    void dateTime64_acceptsPreEpochInstants() {
        DateTime64Codec codec = new DateTime64Codec(3, null);
        long[] arr = codec.allocate(1);
        assertDoesNotThrow(() -> codec.set(arr, 0, Instant.parse("1969-12-31T23:59:59.999Z")),
                "DateTime64 pre-epoch values are valid by design (signed ticks)");
    }

    @Test
    void dateTime64_rejectsTickOverflowAtHighPrecision() {
        DateTime64Codec codec = new DateTime64Codec(9, null);
        long[] arr = codec.allocate(1);
        // Long.MAX_VALUE nanoseconds ≈ +292 years; anything past that overflows Int64 ticks.
        Instant tooFar = Instant.ofEpochSecond(Long.MAX_VALUE / 1_000_000_000L + 1);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, tooFar),
                "DateTime64(9) tick count must not silently wrap");
        assertTrue(ex.getMessage().contains("DateTime64(9)"),
                "message names the precision: " + ex.getMessage());

        Instant tooFarNegative = Instant.ofEpochSecond(-(Long.MAX_VALUE / 1_000_000_000L) - 2);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, tooFarNegative),
                "negative overflow is rejected symmetrically");
    }

    @Test
    void dateTime64_acceptsValuesNearTheTickBoundary() {
        DateTime64Codec codec = new DateTime64Codec(9, null);
        long[] arr = codec.allocate(1);
        Instant nearMax = Instant.ofEpochSecond(Long.MAX_VALUE / 1_000_000_000L - 1);
        assertDoesNotThrow(() -> codec.set(arr, 0, nearMax));
        assertEquals(nearMax, codec.get(arr, 0), "near-boundary value round-trips");
    }
}
