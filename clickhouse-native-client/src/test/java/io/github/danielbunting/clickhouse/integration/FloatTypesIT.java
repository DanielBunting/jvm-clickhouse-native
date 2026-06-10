package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips {@code Float32} and {@code Float64} through a real server in both
 * directions — DECODE (raw {@code INSERT ... VALUES} with SQL literals, server
 * encodes) and ENCODE (bulk insert of a mapped record, client encodes) — plus
 * the {@code query(sql, Class)} mapped-read path.
 *
 * <p>The focus is IEEE-754 edge values: finite high-precision values, the three
 * special literals ({@code nan}, {@code inf}, {@code -inf}), negative zero
 * (distinguished from positive zero via raw bit pattern), and the smallest
 * denormal of each width.
 *
 * <p>Float32 {@code get()} returns {@link Float}, so the record field for the
 * Float32 column is {@code float}; Float64 returns {@link Double} and its field
 * is {@code double}. Special values are inserted in the ENCODE path as their
 * Java constants directly ({@link Double#NaN}, {@link Double#POSITIVE_INFINITY},
 * {@code -0.0}, {@link Double#MIN_VALUE}).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class FloatTypesIT extends TypeRoundTripBase {

    /**
     * Row mirroring the float table. Field names and types match the columns:
     * {@code f32} is {@code float} (Float32 -> Float) and {@code f64} is
     * {@code double} (Float64 -> Double).
     */
    record FloatRow(int id, float f32, double f64) {}

    private static final String COLUMNS =
            "  id  UInt32,"
            + "  f32 Float32,"
            + "  f64 Float64";

    private static final String SELECT_COLS = "id, f32, f64";

    // Finite high-precision representable values, used verbatim in both paths.
    private static final float  F32_PI  = 3.14159f;
    private static final double F64_PI  = 3.141592653589793;
    private static final float  F32_DEN = 1.4e-45f;          // smallest Float32 denormal
    private static final double F64_DEN = 5e-324;            // == Double.MIN_VALUE, smallest denormal

    /**
     * DECODE: server encodes literal rows (including the {@code nan}/{@code inf}/
     * {@code -inf} specials and {@code -0.0}/denormals), client decodes them back.
     */
    @Test
    void floatDecodeRoundTrip() {
        withTable("flt_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute("INSERT INTO " + table + " (" + SELECT_COLS + ") VALUES"
                    // row 1: finite negative
                    + " (1, -1.5, -1.5),"
                    // row 2: finite high-precision
                    + " (2, 3.14159, 3.141592653589793),"
                    // row 3: NaN
                    + " (3, nan, nan),"
                    // row 4: +Inf
                    + " (4, inf, inf),"
                    // row 5: -Inf
                    + " (5, -inf, -inf),"
                    // row 6: negative zero
                    + " (6, -0.0, -0.0),"
                    // row 7: smallest denormal of each width
                    + " (7, 1.4e-45, 5e-324)");

            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(7, rows.size(), "Expected 7 rows from " + table);

            assertFinite(rows.get(0), -1.5, -1.5);
            assertFinite(rows.get(1), F32_PI, F64_PI);
            assertNaN(rows.get(2));
            assertInf(rows.get(3), Double.POSITIVE_INFINITY);
            assertInf(rows.get(4), Double.NEGATIVE_INFINITY);
            assertNegZero(rows.get(5));
            // PINNED server-side literal behavior (image 25.6): the Float32 denormal
            // literal 1.4e-45 survives the SQL parser, but the Float64 smallest
            // denormal literal 5e-324 is collapsed to 0.0 by ClickHouse's literal
            // parser (the real Double.MIN_VALUE round-trips fine via the ENCODE/wire
            // path, asserted in floatEncodeAndMappedReadRoundTrip).
            assertEquals(F32_DEN, ((Number) rows.get(6)[1]).doubleValue(), 0.0,
                    "Float32 denormal literal 1.4e-45 must survive");
            assertEquals(0.0, ((Number) rows.get(6)[2]).doubleValue(), 0.0,
                    "Float64 literal 5e-324 is collapsed to 0.0 by the CH literal parser");
        });
    }

    /**
     * ENCODE + MAPPED-READ: client encodes the same edge rows via a mapped record
     * (Java specials set directly), then reads them back via the block API and via
     * {@code query(sql, FloatRow.class)}.
     */
    @Test
    void floatEncodeAndMappedReadRoundTrip() {
        withTable("flt_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<FloatRow> input = List.of(
                    new FloatRow(1, -1.5f, -1.5),
                    new FloatRow(2, F32_PI, F64_PI),
                    new FloatRow(3, Float.NaN, Double.NaN),
                    new FloatRow(4, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
                    new FloatRow(5, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY),
                    new FloatRow(6, -0.0f, -0.0),
                    new FloatRow(7, Float.MIN_VALUE, Double.MIN_VALUE));

            try (BulkInserter<FloatRow> inserter =
                    conn.createBulkInserter(table, FloatRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(7, rows.size(), "Expected 7 bulk-inserted rows");
            assertFinite(rows.get(0), -1.5, -1.5);
            assertFinite(rows.get(1), F32_PI, F64_PI);
            assertNaN(rows.get(2));
            assertInf(rows.get(3), Double.POSITIVE_INFINITY);
            assertInf(rows.get(4), Double.NEGATIVE_INFINITY);
            assertNegZero(rows.get(5));
            assertFinite(rows.get(6), F32_DEN, F64_DEN);

            // (b) mapped-read via query(sql, Class)
            List<FloatRow> mapped;
            try (var stream = conn.query(
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id",
                    FloatRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(7, mapped.size(), "Expected 7 mapped rows");
            // Finite rows compare exactly via raw bits (covers -0.0 too).
            assertEquals(Float.floatToRawIntBits(-1.5f),
                    Float.floatToRawIntBits(mapped.get(0).f32()), "mapped f32 -1.5");
            assertEquals(Double.doubleToRawLongBits(-1.5),
                    Double.doubleToRawLongBits(mapped.get(0).f64()), "mapped f64 -1.5");
            assertEquals(Float.floatToRawIntBits(F32_PI),
                    Float.floatToRawIntBits(mapped.get(1).f32()), "mapped f32 pi");
            assertEquals(Double.doubleToRawLongBits(F64_PI),
                    Double.doubleToRawLongBits(mapped.get(1).f64()), "mapped f64 pi");
            assertTrue(Float.isNaN(mapped.get(2).f32()), "mapped f32 NaN");
            assertTrue(Double.isNaN(mapped.get(2).f64()), "mapped f64 NaN");
            assertEquals(Float.POSITIVE_INFINITY, mapped.get(3).f32(), "mapped f32 +Inf");
            assertEquals(Double.POSITIVE_INFINITY, mapped.get(3).f64(), "mapped f64 +Inf");
            assertEquals(Float.NEGATIVE_INFINITY, mapped.get(4).f32(), "mapped f32 -Inf");
            assertEquals(Double.NEGATIVE_INFINITY, mapped.get(4).f64(), "mapped f64 -Inf");
            assertEquals(Integer.MIN_VALUE, Float.floatToRawIntBits(mapped.get(5).f32()),
                    "mapped f32 -0.0 raw bits");
            assertEquals(Long.MIN_VALUE, Double.doubleToRawLongBits(mapped.get(5).f64()),
                    "mapped f64 -0.0 raw bits");
            assertEquals(Float.floatToRawIntBits(F32_DEN),
                    Float.floatToRawIntBits(mapped.get(6).f32()), "mapped f32 denormal");
            assertEquals(Double.doubleToRawLongBits(F64_DEN),
                    Double.doubleToRawLongBits(mapped.get(6).f64()), "mapped f64 denormal");
        });
    }

    private static void assertFinite(Object[] r, double expF32, double expF64) {
        // delta 0.0: the exact same double/float that was inserted must come back.
        assertEquals(expF32, ((Number) r[1]).doubleValue(), 0.0, "Float32 finite");
        assertEquals(expF64, ((Number) r[2]).doubleValue(), 0.0, "Float64 finite");
    }

    private static void assertNaN(Object[] r) {
        assertTrue(Double.isNaN(((Number) r[1]).doubleValue()), "Float32 NaN");
        assertTrue(Double.isNaN(((Number) r[2]).doubleValue()), "Float64 NaN");
    }

    private static void assertInf(Object[] r, double sign) {
        assertEquals(sign, ((Number) r[1]).doubleValue(), "Float32 Inf");
        assertEquals(sign, ((Number) r[2]).doubleValue(), "Float64 Inf");
    }

    private static void assertNegZero(Object[] r) {
        // doubleToRawLongBits(-0.0) == Long.MIN_VALUE distinguishes -0.0 from 0.0.
        assertEquals(Long.MIN_VALUE,
                Double.doubleToRawLongBits(((Number) r[1]).doubleValue()),
                "Float32 -0.0 must decode to negative zero");
        assertEquals(Long.MIN_VALUE,
                Double.doubleToRawLongBits(((Number) r[2]).doubleValue()),
                "Float64 -0.0 must decode to negative zero");
    }
}
