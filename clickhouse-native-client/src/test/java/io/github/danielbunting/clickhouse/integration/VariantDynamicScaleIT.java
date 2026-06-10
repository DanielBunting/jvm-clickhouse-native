package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scale / multi-block DECODE probes for {@code Variant} and {@code Dynamic}, exercising the
 * <em>conditional</em> wire-format branches in
 * {@link io.github.danielbunting.clickhouse.types.codec.VariantColumnCodec} and
 * {@link io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec} that the existing
 * 2–3-row single-block tests ({@code VariantTypesIT} / {@code DynamicTypesIT}) cannot reach.
 *
 * <h2>What this targets</h2>
 * <ul>
 *   <li><b>Compact discriminator form.</b> ClickHouse has a <em>compact</em> discriminator
 *       serialization (controlled by {@code use_compact_variant_discriminators_serialization},
 *       on by default) used when a whole granule/block has a single discriminator. Our codecs
 *       decode only the <b>basic</b> form (one {@code UInt8} per row) and hard-reject any other
 *       {@code discriminators_version}. A <b>uniform</b> column over many rows is the realistic
 *       trigger — these are the {@code *Uniform*} probes.</li>
 *   <li><b>Multi-block.</b> A native result over ~200k rows spans several blocks
 *       (~65k rows/block). The {@code *Mixed*} probes assert correct decode of the basic form
 *       across {@code &gt; 1} non-empty block.</li>
 * </ul>
 *
 * <p>All data is generated server-side via {@code numbers(N)} with N just large enough to span
 * multiple native blocks. Values/types are asserted by each row's rule (Variant member order is
 * the server's canonical sort, so we assert by value/type, never by discriminator index).
 *
 * <p>These are DECODE-only (encode is covered by spec 03 / {@code VariantTypesIT}).
 */
@Tag("integration")
class VariantDynamicScaleIT extends TypeRoundTripBase {

    /** ~3 native blocks worth of rows (a block is ~65k); kept far below millions. */
    private static final int SCALE_ROWS = 200_000;

    /** Smaller volume for the all-NULL uniform probe. */
    private static final int NULL_ROWS = 1_000;

    /**
     * Materialises {@code selectSql} and returns the number of non-empty blocks observed,
     * appending every row to {@code rowsOut}. Mirrors {@link #materialize} but also exposes the
     * block count so the multi-block invariant can be asserted.
     */
    private int decodeCountingBlocks(ClickHouseConnection conn, String selectSql,
                                     List<Object[]> rowsOut) {
        int blockCount = 0;
        try (QueryResult result = conn.query(selectSql)) {
            Iterator<Block> blocks = result.blocks();
            while (blocks.hasNext()) {
                Block block = blocks.next();
                if (block.isEmpty()) {
                    continue;
                }
                blockCount++;
                int colCount = block.columnCount();
                int rowCount = block.rowCount();
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[colCount];
                    for (int c = 0; c < colCount; c++) {
                        var col = block.column(c);
                        boolean isNull = col.nulls() != null && col.nulls()[r];
                        row[c] = isNull ? null : col.value(r);
                    }
                    rowsOut.add(row);
                }
            }
        }
        return blockCount;
    }

    // ------------------------------------------------------------------
    // Variant
    // ------------------------------------------------------------------

    /**
     * Compact-form probe: a {@code Variant(UInt32, String)} column where every row is the same
     * variant (all UInt32) over many rows — the most likely shape to trigger the server's
     * compact discriminator serialization. Asserts every row equals its UInt32 value and the
     * total count.
     */
    @Test
    void variantUniformColumnDecodes() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("SET allow_experimental_variant_type = 1");

            // (a) all-UInt32 uniform column.
            List<Object[]> u32 = new java.util.ArrayList<>();
            // numbers() yields UInt64; CAST to a Variant is only allowed from a member type,
            // so narrow to the UInt32 member first (values < SCALE_ROWS fit UInt32).
            int u32Blocks = decodeCountingBlocks(conn,
                    "SELECT CAST(toUInt32(number), 'Variant(UInt32, String)') AS v FROM numbers("
                            + SCALE_ROWS + ")", u32);
            assertEquals(SCALE_ROWS, u32.size(), "all-UInt32 uniform row count");
            assertTrue(u32Blocks >= 1, "expected at least one non-empty block");
            // Spot-check first, a middle, and last rows; full scan validates every value.
            for (int i = 0; i < u32.size(); i++) {
                Object v = u32.get(i)[0];
                assertInstanceOf(Number.class, v, "row " + i + " should be a UInt32 Number");
                assertEquals((long) i, ((Number) v).longValue(), "row " + i + " value");
            }

            // (b) all-String uniform column. NB: casting a *numeric* string into a Variant with
            // a UInt32 member makes the server resolve it to the UInt32 member, so use a
            // non-numeric string ("s<n>") to guarantee the value lands in the String member.
            List<Object[]> str = new java.util.ArrayList<>();
            decodeCountingBlocks(conn,
                    "SELECT CAST(concat('s', toString(number)), 'Variant(UInt32, String)') AS v "
                            + "FROM numbers(" + SCALE_ROWS + ")", str);
            assertEquals(SCALE_ROWS, str.size(), "all-String uniform row count");
            for (int i = 0; i < str.size(); i++) {
                Object v = str.get(i)[0];
                assertInstanceOf(String.class, v, "row " + i + " should be a String");
                assertEquals("s" + i, v, "row " + i + " value");
            }

            // (c) all-NULL uniform column.
            List<Object[]> nulls = new java.util.ArrayList<>();
            decodeCountingBlocks(conn,
                    "SELECT CAST(NULL, 'Variant(UInt32, String)') AS v FROM numbers("
                            + NULL_ROWS + ")", nulls);
            assertEquals(NULL_ROWS, nulls.size(), "all-NULL uniform row count");
            for (int i = 0; i < nulls.size(); i++) {
                assertNull(nulls.get(i)[0], "row " + i + " should be NULL");
            }
        }
    }

    /**
     * Multi-block / basic-form probe: mixed discriminators at volume so the basic form is used
     * across a large, multi-block result. Asserts {@code &gt; 1} non-empty block and each row's
     * value/type by its {@code number % 3} rule, plus the total count.
     */
    @Test
    void variantMixedAtScaleDecodes() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("SET allow_experimental_variant_type = 1");

            // %3==1 uses a non-numeric string so it resolves to the String member (a numeric
            // string would be reparsed into the UInt32 member; see variantUniformColumnDecodes).
            String sql = "SELECT multiIf("
                    + "number % 3 = 0, CAST(toUInt32(number), 'Variant(UInt32, String)'), "
                    + "number % 3 = 1, CAST(concat('s', toString(number)), 'Variant(UInt32, String)'), "
                    + "CAST(NULL, 'Variant(UInt32, String)')) AS v "
                    + "FROM numbers(" + SCALE_ROWS + ")";

            List<Object[]> rows = new java.util.ArrayList<>();
            int blocks = decodeCountingBlocks(conn, sql, rows);

            assertEquals(SCALE_ROWS, rows.size(), "mixed-at-scale row count");
            assertTrue(blocks > 1,
                    "mixed-at-scale (" + SCALE_ROWS + " rows) should span multiple native blocks; "
                            + "saw " + blocks);

            for (int i = 0; i < rows.size(); i++) {
                Object v = rows.get(i)[0];
                switch (i % 3) {
                    case 0 -> {
                        assertInstanceOf(Number.class, v, "row " + i + " (%3==0) UInt32");
                        assertEquals((long) i, ((Number) v).longValue(), "row " + i + " value");
                    }
                    case 1 -> {
                        assertInstanceOf(String.class, v, "row " + i + " (%3==1) String");
                        assertEquals("s" + i, v, "row " + i + " value");
                    }
                    default -> assertNull(v, "row " + i + " (%3==2) NULL");
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Dynamic
    // ------------------------------------------------------------------

    /**
     * Dynamic counterpart of both Variant probes: a uniform all-{@code Int64} column over many
     * rows (compact-form probe) and a mixed {@code Int64}/{@code String}/NULL column over a
     * multi-block result (basic-form / multi-block probe). Confirms the Dynamic discriminator
     * path under both conditions.
     */
    @Test
    void dynamicUniformAndMixedAtScale() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("SET allow_experimental_dynamic_type = 1");

            // (a) uniform all-Int64 column.
            List<Object[]> uniform = new java.util.ArrayList<>();
            decodeCountingBlocks(conn,
                    "SELECT CAST(number, 'Dynamic') AS d FROM numbers(" + SCALE_ROWS + ")",
                    uniform);
            assertEquals(SCALE_ROWS, uniform.size(), "uniform Dynamic row count");
            for (int i = 0; i < uniform.size(); i++) {
                Object v = uniform.get(i)[0];
                assertInstanceOf(Number.class, v, "row " + i + " should be a Number");
                assertEquals((long) i, ((Number) v).longValue(), "row " + i + " value");
            }

            // (b) mixed Int64 / String / NULL over a multi-block result.
            String sql = "SELECT multiIf("
                    + "number % 3 = 0, CAST(number, 'Dynamic'), "
                    + "number % 3 = 1, CAST(toString(number), 'Dynamic'), "
                    + "CAST(NULL, 'Dynamic')) AS d "
                    + "FROM numbers(" + SCALE_ROWS + ")";
            List<Object[]> mixed = new java.util.ArrayList<>();
            int blocks = decodeCountingBlocks(conn, sql, mixed);

            assertEquals(SCALE_ROWS, mixed.size(), "mixed Dynamic row count");
            assertTrue(blocks > 1,
                    "mixed Dynamic (" + SCALE_ROWS + " rows) should span multiple blocks; saw "
                            + blocks);

            for (int i = 0; i < mixed.size(); i++) {
                Object v = mixed.get(i)[0];
                switch (i % 3) {
                    case 0 -> {
                        assertInstanceOf(Number.class, v, "row " + i + " (%3==0) Int64");
                        assertEquals((long) i, ((Number) v).longValue(), "row " + i + " value");
                    }
                    case 1 -> {
                        assertInstanceOf(String.class, v, "row " + i + " (%3==1) String");
                        assertEquals(Integer.toString(i), v, "row " + i + " value");
                    }
                    default -> assertNull(v, "row " + i + " (%3==2) NULL");
                }
            }
        }
    }
}
