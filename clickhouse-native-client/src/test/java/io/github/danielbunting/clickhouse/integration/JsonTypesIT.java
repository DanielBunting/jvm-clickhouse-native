package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DECODE round-trip for a {@code JSON} column via the FLATTENED native serialization,
 * exercising {@link io.github.danielbunting.clickhouse.types.codec.JsonColumnCodec}.
 *
 * <p>In flattened form a JSON column is a set of typed paths, each a {@code Dynamic}
 * sub-column; this client reconstructs a JSON object {@link String} per row from the decoded
 * path values (sorted keys, NULL paths omitted). DECODE-only (see codec javadoc).
 */
@Tag("integration")
class JsonTypesIT extends TypeRoundTripBase {

    @Test
    void jsonObjectDecode() {
        withTable("json_obj", (conn, table) -> {
            conn.execute("SET allow_experimental_json_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, j JSON) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, j) VALUES"
                    + " (1, '{\"a\":1,\"b\":\"x\"}'), (2, '{\"a\":2}')");

            List<Object[]> rows = decode(conn, "SELECT j FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            // Row 1: both paths present.
            Object j0 = rows.get(0)[0];
            assertInstanceOf(String.class, j0, "JSON should decode to a String");
            String s0 = (String) j0;
            assertTrue(s0.contains("\"a\":1"), "row 1 must contain a:1, was " + s0);
            assertTrue(s0.contains("\"b\":\"x\""), "row 1 must contain b:\"x\", was " + s0);

            // Row 2: only path "a" present (b is NULL for this row and is omitted).
            String s1 = (String) rows.get(1)[0];
            assertTrue(s1.contains("\"a\":2"), "row 2 must contain a:2, was " + s1);
            assertTrue(!s1.contains("\"b\""), "row 2 must omit the absent b path, was " + s1);
        });
    }

    /** A row carrying a flat JSON object as a {@link String}. */
    record JRow(long id, String j) {}

    /**
     * ENCODE round-trip for the supported flat-object subset: bulk-insert flat JSON strings
     * (int + string scalar paths) via the binary write path, then SELECT back and assert the
     * decoded JSON contains the expected paths/values. See
     * {@link io.github.danielbunting.clickhouse.types.codec.JsonColumnCodec} for the supported
     * subset (flat objects of int/string/bool/float scalars).
     */
    @Test
    void jsonFlatObjectEncode() {
        withTable("json_obj_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_json_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, j JSON) ENGINE = MergeTree() ORDER BY id");

            List<JRow> input = List.of(
                    new JRow(1, "{\"a\":1,\"b\":\"x\"}"),
                    new JRow(2, "{\"a\":7}"));

            try (BulkInserter<JRow> inserter = conn.createBulkInserter(table, JRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(2, count, "expected 2 bulk-inserted JSON rows");

            List<Object[]> rows = decode(conn, "SELECT j FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size());

            String s0 = (String) rows.get(0)[0];
            assertTrue(s0.contains("\"a\":1"), "row 1 must contain a:1, was " + s0);
            assertTrue(s0.contains("\"b\":\"x\""), "row 1 must contain b:\"x\", was " + s0);

            String s1 = (String) rows.get(1)[0];
            assertTrue(s1.contains("\"a\":7"), "row 2 must contain a:7, was " + s1);
            assertTrue(!s1.contains("\"b\""), "row 2 must omit the absent b path, was " + s1);
        });
    }
}
