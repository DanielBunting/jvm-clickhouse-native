package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * DECODE round-trip for a {@code Dynamic} column via the FLATTENED native serialization,
 * exercising {@link io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec}.
 *
 * <p>The concrete member types are discovered from the wire (a {@code UInt64} version of 3,
 * a {@code VarUInt} type count, the sorted type-name strings, then one {@code UInt8}
 * discriminator per row where {@code num_types} = NULL). DECODE-only (see codec javadoc).
 */
@Tag("integration")
class DynamicTypesIT extends TypeRoundTripBase {

    @Test
    void dynamicMixedDecode() {
        withTable("dynamic_mixed", (conn, table) -> {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, 42::Int64), (2, 'hello'), (3, NULL)");

            List<Object[]> rows = decode(conn, "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            Object v0 = rows.get(0)[0];
            assertInstanceOf(Number.class, v0, "Int64 dynamic should decode to a Number");
            assertEquals(42L, ((Number) v0).longValue());

            assertInstanceOf(String.class, rows.get(1)[0], "String dynamic should decode to String");
            assertEquals("hello", rows.get(1)[0]);

            assertNull(rows.get(2)[0], "NULL dynamic row should decode to null");
        });
    }

    @Test
    void dynamicAllNullDecode() {
        withTable("dynamic_nulls", (conn, table) -> {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES (1, NULL), (2, NULL)");

            List<Object[]> rows = decode(conn, "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());
            assertNull(rows.get(0)[0]);
            assertNull(rows.get(1)[0]);
        });
    }

    /** A row whose Dynamic value is an {@link Object} (Long / String / null here). */
    record DRow(long id, Object d) {}

    /**
     * ENCODE round-trip: bulk-insert mixed {@code Int64}/{@code String}/NULL into a
     * {@code Dynamic} column via the flattened binary write path, then SELECT back and assert.
     *
     * <p>The inference maps {@link Long} &rarr; {@code Int64} and {@link String} &rarr;
     * {@code String} (see {@link io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec}
     * javadoc), so this exact set of Java types round-trips cleanly.
     */
    @Test
    void dynamicMixedEncode() {
        withTable("dynamic_mixed_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree() ORDER BY id");

            List<DRow> input = List.of(
                    new DRow(1, 42L),
                    new DRow(2, "hello"),
                    new DRow(3, null));

            try (BulkInserter<DRow> inserter = conn.createBulkInserter(table, DRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(3, count, "expected 3 bulk-inserted Dynamic rows");

            List<Object[]> rows = decode(conn, "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            Object v0 = rows.get(0)[0];
            assertInstanceOf(Number.class, v0, "row 1 should be an Int64 (Number)");
            assertEquals(42L, ((Number) v0).longValue());

            assertInstanceOf(String.class, rows.get(1)[0], "row 2 should be a String");
            assertEquals("hello", rows.get(1)[0]);

            assertNull(rows.get(2)[0], "row 3 should be NULL");
        });
    }
}
