package io.github.danielbunting.clickhouse.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Error-path and composite-type coverage for {@link ReflectionRowMapper}, complementing the
 * happy-path {@link ReflectionRowMapperTest}. Exercises the mapper's rejection branches
 * (missing column, POJO without a no-arg constructor, wire-type mismatches) and confirms
 * that {@code List}/{@code Map} column values pass through the mapper unchanged. All
 * in-memory; no server.
 *
 * <p>Not covered here by design: the {@code InetAddress}-parse-failure branch would call
 * {@code InetAddress.getByName} on a bad literal, which can trigger real name resolution —
 * unsuitable for an offline unit test; and the "cannot find canonical constructor" /
 * immutable-record-set branches are unreachable through the normal map/bind flow.
 */
class ReflectionRowMapperErrorTest {

    record Simple(long id, String name) {}

    @Test
    void record_missingColumn_isRejectedAtConstruction() {
        ClickHouseException ex = assertThrows(ClickHouseException.class,
                () -> RowMappers.forClass(Simple.class, "id", "does_not_exist"));
        assertTrue(ex.getMessage().contains("does_not_exist"),
                "message names the unmatched column: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("record component"),
                "message identifies it as a missing record component: " + ex.getMessage());
    }

    /** A POJO whose only constructor takes arguments — legal to build a write-only mapper for. */
    static final class NoDefaultCtor {
        public long x;

        NoDefaultCtor(long x) {
            this.x = x;
        }
    }

    @Test
    void pojo_missingField_isRejectedAtConstruction() {
        ClickHouseException ex = assertThrows(ClickHouseException.class,
                () -> RowMappers.forClass(NoDefaultCtor.class, "nope"));
        assertTrue(ex.getMessage().contains("No field found for column 'nope'"),
                ex.getMessage());
    }

    @Test
    void pojo_withoutNoArgConstructor_failsOnlyOnMap_notOnBuild() {
        // Building the mapper is lenient (write-only use never constructs a T)...
        RowMapper<NoDefaultCtor> mapper = RowMappers.forClass(NoDefaultCtor.class, "x");
        // ...but the read path needs to instantiate and must fail clearly.
        ClickHouseException ex = assertThrows(ClickHouseException.class,
                () -> mapper.map(new Object[]{1L}));
        assertTrue(ex.getMessage().contains("requires a no-arg constructor"), ex.getMessage());
    }

    @Test
    void wireTypeMismatch_intoNumericField_surfacesClassCast() {
        record WithLong(long value) {}

        RowMapper<WithLong> mapper = RowMappers.forClass(WithLong.class);
        // A String cannot be coerced to a numeric field; the cast surfaces naturally.
        assertThrows(ClassCastException.class, () -> mapper.map(new Object[]{"not-a-number"}));
    }

    @Test
    void invalidUuidString_surfacesIllegalArgument() {
        record WithUuid(UUID id) {}

        RowMapper<WithUuid> mapper = RowMappers.forClass(WithUuid.class);
        assertThrows(IllegalArgumentException.class, () -> mapper.map(new Object[]{"not-a-uuid"}));
    }

    @Test
    void compositeListAndMap_passThroughMapperUnchanged() {
        record Composite(long id, List<String> tags, Map<String, Long> counts) {}

        RowMapper<Composite> mapper = RowMappers.forClass(Composite.class);
        List<String> tags = List.of("a", "b");
        // Map(K,V) decodes to a LinkedHashMap at the block layer; the mapper must not re-wrap it.
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("x", 1L);

        Composite row = mapper.map(new Object[]{7L, tags, counts});

        assertEquals(7L, row.id());
        assertSame(tags, row.tags(), "List column value passes through the mapper by reference");
        assertSame(counts, row.counts(), "Map column value passes through the mapper by reference");
    }
}
