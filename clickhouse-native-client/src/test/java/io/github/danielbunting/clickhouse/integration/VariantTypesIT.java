package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DECODE and ENCODE round-trips for {@code Variant(UInt32, String)} via the FLATTENED native
 * serialization, exercising {@link io.github.danielbunting.clickhouse.types.codec.VariantColumnCodec}.
 *
 * <p>The server reports the member types in canonical sorted order
 * ({@code Variant(String, UInt32)}) and emits one UInt8 discriminator per row (0xFF = NULL)
 * followed by each member's sub-column. ENCODE is exercised too: {@link #variantUInt32StringEncode}
 * bulk-inserts via the flattened binary write path and reads the rows back (see
 * {@code VariantColumnCodec} javadoc — write emits {@code version = 0} BASIC, accepted on Native INPUT).
 */
@Tag("integration")
class VariantTypesIT extends TypeRoundTripBase {

    @Test
    void variantUInt32StringDecode() {
        withTable("variant_u32s", (conn, table) -> {
            conn.execute("SET allow_experimental_variant_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(UInt32, String)) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, 42), (2, 'hello'), (3, NULL)");

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            // Row 1: a UInt32 (widened to Long by the UInt32 codec).
            Object v0 = rows.get(0)[0];
            assertInstanceOf(Number.class, v0, "UInt32 variant should decode to a Number");
            assertEquals(42L, ((Number) v0).longValue());

            // Row 2: a String.
            Object v1 = rows.get(1)[0];
            assertInstanceOf(String.class, v1, "String variant should decode to a String");
            assertEquals("hello", v1);

            // Row 3: NULL.
            assertNull(rows.get(2)[0], "NULL discriminator (0xFF) should decode to null");
        });
    }

    @Test
    void variantInt64StringDecodeMixedOrder() {
        withTable("variant_i64s", (conn, table) -> {
            conn.execute("SET allow_experimental_variant_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(Int64, String)) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, 'a'), (2, 'b'), (3, 7::Int64), (4, NULL)");

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size());
            assertEquals("a", rows.get(0)[0]);
            assertEquals("b", rows.get(1)[0]);
            assertInstanceOf(Number.class, rows.get(2)[0]);
            assertEquals(7L, ((Number) rows.get(2)[0]).longValue());
            assertNull(rows.get(3)[0]);
        });
    }

    /** A row whose Variant value can be a UInt32 {@link Long}, a {@link String}, or {@code null}. */
    record VRow(long id, Object v) {}

    /**
     * ENCODE round-trip: bulk-insert an int row, a string row, and a NULL row into
     * {@code Variant(UInt32, String)} via the flattened binary write path, then SELECT back and
     * assert each row's value and type. Confirms the server accepts our flattened layout on
     * binary Native INPUT.
     */
    @Test
    void variantUInt32StringEncode() {
        withTable("variant_u32s_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_variant_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Variant(UInt32, String)) ENGINE = MergeTree() ORDER BY id");

            // Server reports the member order as Variant(String, UInt32) (sorted): a Long maps to
            // the UInt32 member, a String to the String member, null to the NULL discriminator.
            List<VRow> input = List.of(
                    new VRow(1, 42L),
                    new VRow(2, "hello"),
                    new VRow(3, null));

            try (BulkInserter<VRow> inserter = conn.createBulkInserter(table, VRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(3, count, "expected 3 bulk-inserted Variant rows");

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            Object v0 = rows.get(0)[0];
            assertInstanceOf(Number.class, v0, "row 1 should be a UInt32 (Number)");
            assertEquals(42L, ((Number) v0).longValue());

            assertInstanceOf(String.class, rows.get(1)[0], "row 2 should be a String");
            assertEquals("hello", rows.get(1)[0]);

            assertNull(rows.get(2)[0], "row 3 should be NULL");
        });
    }
}
