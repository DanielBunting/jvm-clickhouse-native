package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.codec.Date32Codec;
import io.github.danielbunting.clickhouse.types.codec.DateCodec;
import io.github.danielbunting.clickhouse.types.codec.DateTime64Codec;
import io.github.danielbunting.clickhouse.types.codec.DateTimeCodec;
import io.github.danielbunting.clickhouse.types.codec.IntervalCodec;
import io.github.danielbunting.clickhouse.types.codec.Time64Codec;
import io.github.danielbunting.clickhouse.types.codec.TimeCodec;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Offline temporal-edge coverage for the Block→Arrow bridge: epoch-day edges (pre-1970 Date32),
 * DateTime64 tick scaling for every precision/unit pairing, timezone-carrying columns, Time/Time64
 * and Interval tick math. The ADBC analogue of the JDBC module's {@code ChResultSetTemporalTest} +
 * the temporal half of {@code JdbcValuesTest}. Values are asserted through
 * {@link ArrowToBlock#toJavaValue}, the same decoder the equivalence ITs trust.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class BlockToArrowTemporalTest {

    // ---- dates ---------------------------------------------------------------------------------

    @Test
    @DisplayName("Date epoch days carry through (DateDayVector)")
    void dateEpochDays(BufferAllocator allocator) {
        LocalDate date = LocalDate.of(2026, 5, 30);
        Object read = roundTrip(allocator, "Date", new DateCodec(), date);
        assertEquals(date, read);
    }

    @Test
    @DisplayName("Date32 supports pre-1970 dates as negative epoch days")
    void date32PreEpochDates(BufferAllocator allocator) {
        LocalDate date = LocalDate.of(1950, 3, 4);
        Object read = roundTrip(allocator, "Date32", new Date32Codec(), date);
        assertEquals(date, read, "negative epoch days must survive the bridge");
    }

    // ---- DateTime / DateTime64 -------------------------------------------------------------------

    @Test
    @DisplayName("DateTime carries whole epoch seconds")
    void dateTimeEpochSeconds(BufferAllocator allocator) {
        Instant instant = Instant.parse("2021-06-15T12:34:56Z");
        Object read = roundTrip(allocator, "DateTime('UTC')", new DateTimeCodec(ZoneId.of("UTC")), instant);
        assertEquals(instant, read);
    }

    @ParameterizedTest(name = "DateTime64({0})")
    @MethodSource("dateTime64Precisions")
    @DisplayName("DateTime64 ticks scale exactly from the column precision to the Arrow unit")
    void dateTime64TickScaling(int precision, Instant instant, BufferAllocator allocator) {
        // Precisions that are not a whole Arrow unit (1, 4, 7) exercise the *10^diff rescale;
        // exact ones (0, 3, 6, 9) pass through unscaled.
        Object read = roundTrip(
                allocator,
                "DateTime64(" + precision + ", 'UTC')",
                new DateTime64Codec(precision, ZoneId.of("UTC")),
                instant);
        assertEquals(instant, read, "precision " + precision);
    }

    static Stream<Arguments> dateTime64Precisions() {
        return Stream.of(
                Arguments.of(0, Instant.parse("2021-06-15T12:34:56Z")),
                Arguments.of(1, Instant.parse("2021-06-15T12:34:56.700Z")),
                Arguments.of(3, Instant.parse("2021-06-15T12:34:56.789Z")),
                Arguments.of(4, Instant.parse("2021-06-15T12:34:56.789100Z")),
                Arguments.of(6, Instant.parse("2021-06-15T12:34:56.789123Z")),
                Arguments.of(7, Instant.parse("2021-06-15T12:34:56.789123400Z")),
                Arguments.of(9, Instant.parse("2021-06-15T12:34:56.789123456Z")));
    }

    @Test
    @DisplayName("a pre-1970 DateTime64 value (negative ticks) survives the bridge")
    void dateTime64PreEpoch(BufferAllocator allocator) {
        Instant instant = Instant.parse("1969-12-31T23:59:59.500Z");
        Object read = roundTrip(allocator, "DateTime64(3, 'UTC')",
                new DateTime64Codec(3, ZoneId.of("UTC")), instant);
        assertEquals(instant, read);
    }

    @Test
    @DisplayName("a non-UTC column zone is carried on the Arrow field, values stay instants")
    void nonUtcZoneCarriedOnField(BufferAllocator allocator) {
        Instant instant = Instant.parse("2021-06-15T12:34:56Z");
        Schema schema = ClickHouseArrowTypes.schema(
                List.of("t"), List.of("DateTime('Europe/London')"));
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "t", "DateTime('Europe/London')",
                new DateTimeCodec(ZoneId.of("Europe/London")), new Object[] {instant}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            FieldVector vector = root.getVector("t");
            assertEquals("Europe/London",
                    ((ArrowType.Timestamp) vector.getField().getType()).getTimezone());
            assertEquals(instant, ArrowToBlock.toJavaValue(vector, 0),
                    "the instant itself is zone-independent");
        }
    }

    // ---- Time / Time64 / Interval ------------------------------------------------------------------

    @Test
    @DisplayName("Time spans carry whole seconds, including spans beyond 24h")
    void timeSpansBeyondTwentyFourHours(BufferAllocator allocator) {
        Duration span = Duration.ofHours(30).plusMinutes(5);
        Object read = roundTrip(allocator, "Time", new TimeCodec(), span);
        assertEquals(span, read, "Arrow Duration (not time-of-day) must fit >24h spans");
    }

    @Test
    @DisplayName("Time64(9) preserves nanosecond resolution")
    void time64NanosecondResolution(BufferAllocator allocator) {
        Duration span = Duration.ofSeconds(90, 123_456_789L);
        Object read = roundTrip(allocator, "Time64(9)", new Time64Codec(9), span);
        assertEquals(span, read);
    }

    @Test
    @DisplayName("Time64(6) truncates below its declared precision, exactly at ticks")
    void time64MicrosecondTicks(BufferAllocator allocator) {
        Duration span = Duration.ofSeconds(1, 234_567_000L);
        Object read = roundTrip(allocator, "Time64(6)", new Time64Codec(6), span);
        assertEquals(span, read);
    }

    @Test
    @DisplayName("a negative Time span (ClickHouse allows them) survives the bridge")
    void negativeTimeSpan(BufferAllocator allocator) {
        Duration span = Duration.ofSeconds(-3661);
        Object read = roundTrip(allocator, "Time", new TimeCodec(), span);
        assertEquals(span, read);
    }

    @Test
    @DisplayName("IntervalHour counts surface as exact second Durations")
    void intervalHourAsDuration(BufferAllocator allocator) {
        Duration interval = Duration.ofHours(5);
        Object read = roundTrip(allocator, "IntervalHour",
                new IntervalCodec(IntervalCodec.Unit.HOUR), interval);
        assertEquals(interval, read);
    }

    @Test
    @DisplayName("IntervalMillisecond keeps sub-second resolution")
    void intervalMillisecondAsDuration(BufferAllocator allocator) {
        Duration interval = Duration.ofMillis(1234);
        Object read = roundTrip(allocator, "IntervalMillisecond",
                new IntervalCodec(IntervalCodec.Unit.MILLISECOND), interval);
        assertEquals(interval, read);
    }

    @Test
    @DisplayName("calendar IntervalYear surfaces as a Period of total months")
    void intervalYearAsPeriod(BufferAllocator allocator) {
        java.time.Period interval = java.time.Period.ofYears(3);
        Object read = roundTrip(allocator, "IntervalYear",
                new IntervalCodec(IntervalCodec.Unit.YEAR), interval);
        assertEquals(java.time.Period.ofMonths(36), read,
                "calendar intervals normalise to total months");
    }

    @Test
    @DisplayName("Duration vectors expose their unit through the field type")
    void durationVectorUnitMatchesPrecision(BufferAllocator allocator) {
        Schema schema = ClickHouseArrowTypes.schema(List.of("t"), List.of("Time64(3)"));
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "t", "Time64(3)", new Time64Codec(3), new Object[] {Duration.ofMillis(1500)}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            DurationVector vector = (DurationVector) root.getVector("t");
            assertEquals(org.apache.arrow.vector.types.TimeUnit.MILLISECOND,
                    ((ArrowType.Duration) vector.getField().getType()).getUnit());
            assertEquals(Duration.ofMillis(1500), vector.getObject(0));
        }
    }

    @Test
    @DisplayName("date columns fill the raw epoch-day int on the vector")
    void dateVectorRawEpochDays(BufferAllocator allocator) {
        LocalDate date = LocalDate.of(2020, 1, 1);
        Schema schema = ClickHouseArrowTypes.schema(List.of("d"), List.of("Date"));
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "d", "Date", new DateCodec(), new Object[] {date}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            assertEquals((int) date.toEpochDay(), ((DateDayVector) root.getVector("d")).get(0));
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Fills a one-cell column into Arrow and decodes it back via the production reverse bridge. */
    private static Object roundTrip(
            BufferAllocator allocator,
            String type,
            io.github.danielbunting.clickhouse.types.ColumnCodec<?> codec,
            Object value) {
        Schema schema = ClickHouseArrowTypes.schema(List.of("c"), List.of(type));
        Block block = TestBlocks.blockOf(TestBlocks.column("c", type, codec, new Object[] {value}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            return ArrowToBlock.toJavaValue(root.getVector("c"), 0);
        }
    }
}
