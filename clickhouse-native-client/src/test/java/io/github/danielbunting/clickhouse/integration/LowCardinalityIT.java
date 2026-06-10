package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips {@code LowCardinality(T)} columns through a real server, exercising the native
 * dictionary + index-key wire format decoded/encoded by
 * {@link io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec}.
 *
 * <p>Previously the client transparently unwrapped {@code LowCardinality(String)} to a plain
 * {@code String} codec and <b>silently corrupted</b> the dictionary/index bytes on read. These
 * tests assert the real format now round-trips exactly, in both directions:
 * <ul>
 *   <li><b>DECODE</b> — server encodes literal {@code INSERT ... VALUES}; client decodes.</li>
 *   <li><b>ENCODE</b> — client bulk-inserts a mapped record; values are read back.</li>
 * </ul>
 * covering {@code LowCardinality(String)}, {@code LowCardinality(Nullable(String))} (with a NULL
 * row), and {@code LowCardinality(UInt32)} (numeric dictionary).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class LowCardinalityIT extends TypeRoundTripBase {

    /** Row mirroring {@code (id UInt32, lc LowCardinality(String))}; field names match columns. */
    record LcStringRow(long id, String lc) {}

    /** Row mirroring {@code (id UInt32, lc LowCardinality(UInt32))}. */
    record LcUInt32Row(long id, long lc) {}

    private List<String> stringColumn(List<Object[]> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (Object[] r : rows) {
            out.add(r[0] == null ? null : r[0].toString());
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // LowCardinality(String)
    // -------------------------------------------------------------------------

    /**
     * DECODE: server encodes {@code LowCardinality(String)} literals; client decodes the
     * dictionary/index stream and reconstructs the exact values in order. This is the
     * positive test that replaces the former silent-corruption tripwire.
     */
    @Test
    void lowCardinalityStringDecodeRoundTrip() {
        withTable("lc_str_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(String))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, lc) VALUES"
                    + " (1,'red'),(2,'green'),(3,'red'),(4,'blue')");

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            assertEquals(List.of("red", "green", "red", "blue"), stringColumn(rows),
                    "LowCardinality(String) values must decode exactly in order");
        });
    }

    /**
     * ENCODE: client bulk-inserts mapped records into a {@code LowCardinality(String)} column
     * (building the dictionary + index keys), then reads them back and asserts equality.
     */
    @Test
    void lowCardinalityStringEncodeRoundTrip() {
        withTable("lc_str_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(String))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<LcStringRow> input = List.of(
                    new LcStringRow(1, "red"),
                    new LcStringRow(2, "green"),
                    new LcStringRow(3, "red"),
                    new LcStringRow(4, "blue"));

            try (BulkInserter<LcStringRow> inserter =
                         conn.createBulkInserter(table, LcStringRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            assertEquals(List.of("red", "green", "red", "blue"), stringColumn(rows),
                    "Bulk-inserted LowCardinality(String) values must round-trip in order");
        });
    }

    // -------------------------------------------------------------------------
    // LowCardinality(Nullable(String))
    // -------------------------------------------------------------------------

    /**
     * DECODE: {@code LowCardinality(Nullable(String))} with a NULL row. The dictionary's slot 0
     * is the NULL placeholder; the codec maps key 0 to {@code null}.
     */
    @Test
    void lowCardinalityNullableStringDecodeRoundTrip() {
        withTable("lc_nstr_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, lc LowCardinality(Nullable(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, lc) VALUES"
                    + " (1,'red'),(2,NULL),(3,'green'),(4,'red')");

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            assertEquals(Arrays.asList("red", null, "green", "red"), stringColumn(rows),
                    "LowCardinality(Nullable(String)) must decode NULL and values in order");
        });
    }

    /**
     * ENCODE: bulk-insert a {@code LowCardinality(Nullable(String))} column including a null
     * field, read back, and assert.
     */
    @Test
    void lowCardinalityNullableStringEncodeRoundTrip() {
        withTable("lc_nstr_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, lc LowCardinality(Nullable(String)))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<LcStringRow> input = Arrays.asList(
                    new LcStringRow(1, "red"),
                    new LcStringRow(2, null),
                    new LcStringRow(3, "green"),
                    new LcStringRow(4, "red"));

            try (BulkInserter<LcStringRow> inserter =
                         conn.createBulkInserter(table, LcStringRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            assertEquals(Arrays.asList("red", null, "green", "red"), stringColumn(rows),
                    "Bulk-inserted LowCardinality(Nullable(String)) must round-trip incl. NULL");
        });
    }

    // -------------------------------------------------------------------------
    // LowCardinality(UInt32) — numeric dictionary
    // -------------------------------------------------------------------------

    /**
     * DECODE: numeric dictionary. {@code LowCardinality(UInt32)} values (widened to {@code Long})
     * must decode exactly in order.
     */
    @Test
    void lowCardinalityUInt32DecodeRoundTrip() {
        withTable("lc_u32_decode", (conn, table) -> {
            // LowCardinality of a fixed-width numeric is "suspicious" and gated by default.
            conn.execute("SET allow_suspicious_low_cardinality_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(UInt32))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, lc) VALUES"
                    + " (1,100),(2,200),(3,100),(4,300)");

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            List<Long> values = new ArrayList<>();
            for (Object[] r : rows) {
                values.add(((Number) r[0]).longValue());
            }
            assertEquals(List.of(100L, 200L, 100L, 300L), values,
                    "LowCardinality(UInt32) values must decode exactly in order");
        });
    }

    /**
     * ENCODE: bulk-insert a numeric {@code LowCardinality(UInt32)} column, read back, assert.
     */
    @Test
    void lowCardinalityUInt32EncodeRoundTrip() {
        withTable("lc_u32_encode", (conn, table) -> {
            conn.execute("SET allow_suspicious_low_cardinality_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, lc LowCardinality(UInt32))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<LcUInt32Row> input = List.of(
                    new LcUInt32Row(1, 100L),
                    new LcUInt32Row(2, 200L),
                    new LcUInt32Row(3, 100L),
                    new LcUInt32Row(4, 300L));

            try (BulkInserter<LcUInt32Row> inserter =
                         conn.createBulkInserter(table, LcUInt32Row.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT lc FROM " + table + " ORDER BY id");
            List<Long> values = new ArrayList<>();
            for (Object[] r : rows) {
                values.add(((Number) r[0]).longValue());
            }
            assertEquals(List.of(100L, 200L, 100L, 300L), values,
                    "Bulk-inserted LowCardinality(UInt32) must round-trip in order");
        });
    }
}
