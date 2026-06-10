package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Precision and truncation edge-case tests for {@link DateTime64Codec} and
 * {@link Time64Codec}, covering each precision 0..9.
 *
 * <p>Both codecs store one Int64 "tick" per value where a tick is
 * {@code 10^(-precision)} seconds, and convert via floor div/mod so pre-epoch
 * (negative) values are handled.
 *
 * <p>Key behavioral facts grounded in the implementation:
 * <ul>
 *   <li>{@code set} converts {@code nano / nanosPerTick} with integer division —
 *       sub-tick precision is <em>truncated toward zero</em>, NOT rounded. (This
 *       differs from DecimalCodec, which uses HALF_UP — called out in the plan.)</li>
 *   <li>{@code get} re-derives an {@link Instant}/{@link Duration} from ticks;
 *       a tick value that lost sub-tick nanos cannot recover them.</li>
 *   <li>{@code DateTime64} stores a {@link ZoneId} for display re-zoning but the
 *       {@link Instant} it returns is epoch-neutral — the zone does not shift the
 *       stored tick count nor the recovered Instant.</li>
 * </ul>
 */
class DateTime64PrecisionTest {

    private static byte[] writeOne(DateTime64Codec codec, Instant ts) {
        long[] src = codec.allocate(1);
        codec.set(src, 0, ts);
        return Bytes.capture(w -> codec.write(w, src, 1));
    }

    private static Instant readOneInstant(DateTime64Codec codec, byte[] wire) throws IOException {
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        return (Instant) codec.get(dest, 0);
    }

    // =========================================================================
    // DateTime64: each precision 0..9 round-trips a value at its own resolution
    // =========================================================================

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    void dateTime64_eachPrecisionRoundTripsAtOwnResolution(int precision) throws IOException {
        DateTime64Codec codec = new DateTime64Codec(precision, ZoneId.of("UTC"));
        long nanosPerTick = (long) Math.pow(10, 9 - precision);
        // An instant with exactly one tick of sub-second value: representable at this precision.
        Instant ts = Instant.ofEpochSecond(1_700_000_000L, nanosPerTick);

        long[] src = codec.allocate(1);
        codec.set(src, 0, ts);
        // tick count = seconds*ticksPerSecond + 1
        long ticksPerSecond = (long) Math.pow(10, precision);
        assertEquals(1_700_000_000L * ticksPerSecond + 1, src[0]);

        assertEquals(ts, readOneInstant(codec, writeOne(codec, ts)));
    }

    @Test
    void dateTime64_precision3_truncatesSubMilliTowardZero() throws IOException {
        // 999_000 ns = 999 µs is below 1 ms; precision-3 truncates it to 0.
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        Instant ts = Instant.ofEpochSecond(1000, 999_000L);
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, ts);
        assertEquals(1_000_000L, arr[0]);  // 1000 s * 1000 ms, sub-ms dropped
        assertEquals(Instant.ofEpochSecond(1000, 0), codec.get(arr, 0));
    }

    @Test
    void dateTime64_truncationIsTowardZeroNotHalfUp() {
        // 1_500_000 ns = 1.5 ms. HALF_UP would give 2 ms; truncation gives 1 ms.
        // This locks the (deliberate) divergence from DecimalCodec's rounding.
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, Instant.ofEpochSecond(0, 1_500_000L));
        assertEquals(1L, arr[0]);  // 1 ms tick, not 2
    }

    @Test
    void dateTime64_preEpochNegativeTicks() throws IOException {
        // Instant.ofEpochSecond(-1, 500_000_000) = 0.5s before the epoch.
        // precision 3: ticksFromSeconds = -1*1000 = -1000; ticksFromNanos = 500_000_000/1_000_000 = 500;
        // stored ticks = -500. get() uses floorDiv/floorMod to recover the same Instant.
        DateTime64Codec codec = new DateTime64Codec(3, ZoneId.of("UTC"));
        Instant ts = Instant.ofEpochSecond(-1, 500_000_000);
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, ts);
        assertEquals(-500L, arr[0]);
        assertEquals(ts, readOneInstant(codec, writeOne(codec, ts)));
    }

    // =========================================================================
    // DateTime64 zone re-zoning: Instant is epoch-neutral
    // =========================================================================

    @Test
    void dateTime64_zoneDoesNotShiftStoredTicksOrInstant() throws IOException {
        Instant ts = Instant.parse("2023-06-15T12:34:56.789Z");
        DateTime64Codec utc = new DateTime64Codec(3, ZoneId.of("UTC"));
        DateTime64Codec tokyo = new DateTime64Codec(3, ZoneId.of("Asia/Tokyo"));

        // Same Instant -> identical tick count regardless of zone.
        long[] a = utc.allocate(1);
        long[] b = tokyo.allocate(1);
        utc.set(a, 0, ts);
        tokyo.set(b, 0, ts);
        assertEquals(a[0], b[0]);

        // And the recovered Instant is the same epoch instant (zone is display-only).
        assertEquals(readOneInstant(utc, writeOne(utc, ts)),
                     readOneInstant(tokyo, writeOne(tokyo, ts)));
    }

    @Test
    void dateTime64_nullZoneDefaultsToUtc() {
        assertEquals(ZoneId.of("UTC"), new DateTime64Codec(6, null).zoneId());
    }

    // =========================================================================
    // Time64: each precision 0..9
    // =========================================================================

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    void time64_eachPrecisionRoundTrips(int precision) throws IOException {
        Time64Codec codec = new Time64Codec(precision);
        long nanosPerTick = (long) Math.pow(10, 9 - precision);
        // 13:00:00 plus one tick of sub-second.
        Duration d = Duration.ofSeconds(13 * 3600).plusNanos(nanosPerTick);

        long[] src = codec.allocate(1);
        codec.set(src, 0, d);

        byte[] wire = Bytes.capture(w -> codec.write(w, src, 1));
        long[] dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(d, codec.get(dest, 0));
    }

    @Test
    void time64_precision6_truncatesSubMicro() {
        // 1_999 ns is below 1 µs at precision 6 -> truncated to 1 µs (1_000 ns), not 2.
        Time64Codec codec = new Time64Codec(6);
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, Duration.ofSeconds(0, 1_999L));
        assertEquals(1L, arr[0]);  // 1 µs tick
        assertEquals(Duration.ofNanos(1_000L), codec.get(arr, 0));
    }

    @Test
    void time64_typeNameIncludesPrecision() {
        assertEquals("Time64(3)", new Time64Codec(3).typeName());
        assertEquals(Duration.class, new Time64Codec(3).javaType());
    }

    @Test
    void time64_parsesHmsString() {
        // "01:02:03.5" = 3723.5 s. precision 3: ticks = 3723*1000 + 500 = 3_723_500.
        Time64Codec codec = new Time64Codec(3);
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, "01:02:03.5");
        assertEquals(3_723_500L, arr[0]);
        assertEquals(Duration.ofSeconds(3723, 500_000_000L), codec.get(arr, 0));
    }

    @Test
    void time64_negativeDurationString() {
        // "-00:00:01" = -1 s. precision 3: ticks = -1000; get() floor-recovers Duration.ofSeconds(-1).
        Time64Codec codec = new Time64Codec(3);
        long[] arr = codec.allocate(1);
        codec.set(arr, 0, "-00:00:01");
        assertEquals(-1000L, arr[0]);
        assertEquals(Duration.ofSeconds(-1), codec.get(arr, 0));
    }
}
