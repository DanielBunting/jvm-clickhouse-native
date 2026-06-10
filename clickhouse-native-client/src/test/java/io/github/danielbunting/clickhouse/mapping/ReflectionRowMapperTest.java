package io.github.danielbunting.clickhouse.mapping;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ReflectionRowMapper} and {@link RowMappers}.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Round-trip (read path + write path) for a Java record.</li>
 *   <li>Round-trip for a POJO with a no-arg constructor.</li>
 *   <li>Case-insensitive column-name matching.</li>
 *   <li>Wire-type coercions: Long->Instant, Integer->LocalDate, String->UUID.</li>
 *   <li>Null handling for nullable column values.</li>
 * </ul>
 *
 * <p>No running ClickHouse server is required; all assertions are in-memory.
 */
class ReflectionRowMapperTest {

    // -----------------------------------------------------------------------
    // Sample record used by the record round-trip tests
    // -----------------------------------------------------------------------

    /**
     * A sample Java record covering all supported field types.
     */
    record SampleRow(
        long id,
        String name,
        Instant createdAt,
        LocalDate birthDate,
        UUID externalId,
        BigDecimal amount,
        List<String> tags
    ) {}

    // -----------------------------------------------------------------------
    // Record: read path (map)
    // -----------------------------------------------------------------------

    @Test
    void record_map_allSupportedTypes() {
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class);

        assertArrayEquals(
            new String[]{"id", "name", "createdAt", "birthDate", "externalId", "amount", "tags"},
            mapper.columnNames()
        );

        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        BigDecimal amount = new BigDecimal("12345.67");
        List<String> tags = List.of("a", "b");

        // Wire values as the block layer would supply them:
        // id        -> long (raw), stored as Long boxed
        // createdAt -> epoch-seconds as Long
        // birthDate -> days-since-epoch as Integer
        // externalId -> UUID object directly
        // amount     -> BigDecimal directly
        // tags       -> List<String> directly
        Object[] wire = {
            42L,                         // id
            "Alice",                     // name
            1_700_000_000L,              // createdAt (epoch-seconds)
            (int) LocalDate.of(1990, 6, 15).toEpochDay(),  // birthDate
            uuid,                        // externalId
            amount,                      // amount
            tags                         // tags
        };

        SampleRow row = mapper.map(wire);

        assertEquals(42L, row.id());
        assertEquals("Alice", row.name());
        assertEquals(Instant.ofEpochSecond(1_700_000_000L), row.createdAt());
        assertEquals(LocalDate.of(1990, 6, 15), row.birthDate());
        assertEquals(uuid, row.externalId());
        assertEquals(amount, row.amount());
        assertEquals(tags, row.tags());
    }

    // -----------------------------------------------------------------------
    // Record: write path (bind)
    // -----------------------------------------------------------------------

    @Test
    void record_bind_allSupportedTypes() {
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class);

        UUID uuid = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("99.01");
        Instant ts = Instant.ofEpochSecond(1_600_000_000L);
        LocalDate date = LocalDate.of(2000, 1, 1);
        List<String> tags = List.of("x");

        SampleRow row = new SampleRow(7L, "Bob", ts, date, uuid, amount, tags);
        Object[] dest = new Object[mapper.columnNames().length];
        mapper.bind(row, dest);

        assertEquals(7L, dest[0]);                                // id
        assertEquals("Bob", dest[1]);                            // name
        // bind passes Java values straight to the codecs (which convert to wire):
        assertEquals(ts, dest[2]);                               // createdAt -> Instant (codec converts)
        assertEquals(date, dest[3]);                             // birthDate -> LocalDate (codec converts)
        assertEquals(uuid, dest[4]);                             // externalId pass-through
        assertEquals(amount, dest[5]);                           // amount pass-through
        assertEquals(tags, dest[6]);                             // tags pass-through
    }

    // -----------------------------------------------------------------------
    // Round-trip: map then bind should reproduce wire values
    // -----------------------------------------------------------------------

    @Test
    void record_roundTrip_mapThenBind() {
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class);

        UUID uuid = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("1.23");
        List<String> tags = List.of("foo");
        long epochSec = 1_000_000_000L;
        int epochDay = (int) LocalDate.of(2024, 3, 15).toEpochDay();

        Object[] wire = {
            1L, "Test", epochSec, epochDay, uuid, amount, tags
        };

        SampleRow row = mapper.map(wire);
        Object[] dest = new Object[wire.length];
        mapper.bind(row, dest);

        // map() converts wire->Java; bind() now reproduces the Java values (the codecs
        // do Java->wire on insert). Pass-through types match the wire input directly;
        // temporal fields come back as the mapped Java types.
        assertEquals(wire[0], dest[0]);              // id
        assertEquals(wire[1], dest[1]);              // name
        assertEquals(row.createdAt(), dest[2]);      // createdAt -> Instant
        assertEquals(row.birthDate(), dest[3]);      // birthDate -> LocalDate
        assertEquals(wire[4], dest[4]);              // externalId
        assertEquals(wire[5], dest[5]);              // amount
        assertEquals(wire[6], dest[6]);              // tags
    }

    // -----------------------------------------------------------------------
    // POJO round-trip
    // -----------------------------------------------------------------------

    /**
     * A minimal POJO with a public no-arg constructor covering a subset of types.
     */
    static final class EventPojo {
        public long eventId;
        public String eventName;
        public Integer count;

        public EventPojo() {}
    }

    @Test
    void pojo_map_basicTypes() {
        RowMapper<EventPojo> mapper = RowMappers.forClass(EventPojo.class);

        String[] cols = mapper.columnNames();
        // All non-static, non-synthetic, non-transient fields in declaration order.
        assertEquals("eventId", cols[0]);
        assertEquals("eventName", cols[1]);
        assertEquals("count", cols[2]);

        Object[] wire = {100L, "click", 5};
        EventPojo pojo = mapper.map(wire);

        assertEquals(100L, pojo.eventId);
        assertEquals("click", pojo.eventName);
        assertEquals(5, pojo.count);
    }

    @Test
    void pojo_bind_basicTypes() {
        RowMapper<EventPojo> mapper = RowMappers.forClass(EventPojo.class);

        EventPojo pojo = new EventPojo();
        pojo.eventId = 200L;
        pojo.eventName = "view";
        pojo.count = 3;

        Object[] dest = new Object[3];
        mapper.bind(pojo, dest);

        assertEquals(200L, dest[0]);
        assertEquals("view", dest[1]);
        assertEquals(3, dest[2]);
    }

    @Test
    void pojo_roundTrip() {
        RowMapper<EventPojo> mapper = RowMappers.forClass(EventPojo.class);

        Object[] wire = {999L, "purchase", 1};
        EventPojo pojo = mapper.map(wire);
        Object[] dest = new Object[3];
        mapper.bind(pojo, dest);

        assertArrayEquals(wire, dest);
    }

    // -----------------------------------------------------------------------
    // Case-insensitive matching
    // -----------------------------------------------------------------------

    @Test
    void record_caseInsensitiveColumnMatch() {
        // Use upper-cased column names; mapper should still match record components.
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class,
            "ID", "NAME", "CREATEDAT", "BIRTHDATE", "EXTERNALID", "AMOUNT", "TAGS");

        UUID uuid = UUID.randomUUID();
        Object[] wire = {1L, "Z", 0L, 0, uuid, BigDecimal.ONE, List.of()};
        SampleRow row = mapper.map(wire);

        assertEquals(1L, row.id());
        assertEquals("Z", row.name());
    }

    // -----------------------------------------------------------------------
    // Null handling
    // -----------------------------------------------------------------------

    @Test
    void record_map_nullableValuesPassedAsNull() {
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class);

        // Provide null for name, externalId, amount, tags (nullable by convention in CH).
        Object[] wire = {10L, null, 0L, 0, null, null, null};
        SampleRow row = mapper.map(wire);

        assertEquals(10L, row.id());
        assertNull(row.name());
        assertNull(row.externalId());
        assertNull(row.amount());
        assertNull(row.tags());
    }

    // -----------------------------------------------------------------------
    // forClass with explicit column names — partial column set
    // -----------------------------------------------------------------------

    @Test
    void record_explicitColumns_partialSubset() {
        // Only request two of the seven record components.
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class, "id", "name");

        assertArrayEquals(new String[]{"id", "name"}, mapper.columnNames());

        Object[] wire = {55L, "Partial"};
        SampleRow row = mapper.map(wire);

        assertEquals(55L, row.id());
        assertEquals("Partial", row.name());
        // Unmapped components will be null / zero (Java default for records).
        assertNull(row.createdAt());
    }

    // -----------------------------------------------------------------------
    // Wire coercions: Number widening/narrowing
    // -----------------------------------------------------------------------

    @Test
    void wireCoercion_integerToLong() {
        // Simulate a wire value arriving as Integer (e.g. from UInt32 codec)
        // being coerced into a long field.
        record WithLong(long value) {}

        RowMapper<WithLong> mapper = RowMappers.forClass(WithLong.class);
        // Pass an Integer (not Long) as the wire value.
        Object[] wire = {42};  // Integer
        WithLong row = mapper.map(wire);
        assertEquals(42L, row.value());
    }

    @Test
    void wireCoercion_longToInteger() {
        record WithInt(Integer value) {}

        RowMapper<WithInt> mapper = RowMappers.forClass(WithInt.class);
        Object[] wire = {9L};  // Long
        WithInt row = mapper.map(wire);
        assertEquals(9, row.value());
    }

    @Test
    void wireCoercion_stringToUuid() {
        record WithUuid(UUID id) {}

        RowMapper<WithUuid> mapper = RowMappers.forClass(WithUuid.class);
        String uuidStr = "550e8400-e29b-41d4-a716-446655440000";
        Object[] wire = {uuidStr};
        WithUuid row = mapper.map(wire);
        assertEquals(UUID.fromString(uuidStr), row.id());
    }

    @Test
    void instantBind_passesInstantThroughToCodec() {
        record WithInstant(Instant ts) {}

        RowMapper<WithInstant> mapper = RowMappers.forClass(WithInstant.class);
        Instant now = Instant.ofEpochSecond(9_999_999L);
        WithInstant instance = new WithInstant(now);

        Object[] dest = new Object[1];
        mapper.bind(instance, dest);
        // bind passes the raw Instant; the DateTime codec performs the epoch conversion.
        assertEquals(now, dest[0]);
    }

    @Test
    void localDateBind_passesLocalDateThroughToCodec() {
        record WithDate(LocalDate d) {}

        RowMapper<WithDate> mapper = RowMappers.forClass(WithDate.class);
        LocalDate date = LocalDate.of(2025, 1, 1);
        WithDate instance = new WithDate(date);

        Object[] dest = new Object[1];
        mapper.bind(instance, dest);
        // bind passes the raw LocalDate; the Date codec performs the day-number conversion.
        assertEquals(date, dest[0]);
    }

    // -----------------------------------------------------------------------
    // columnNames() returns a defensive copy
    // -----------------------------------------------------------------------

    @Test
    void columnNames_defensiveCopy() {
        RowMapper<SampleRow> mapper = RowMappers.forClass(SampleRow.class);
        String[] names1 = mapper.columnNames();
        String[] names2 = mapper.columnNames();
        assertNotSame(names1, names2);
        assertArrayEquals(names1, names2);
    }
}
