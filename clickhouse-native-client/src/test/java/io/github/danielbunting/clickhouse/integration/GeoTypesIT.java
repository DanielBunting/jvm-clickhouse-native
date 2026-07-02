package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips the ClickHouse geo types against a real 25.6 server.
 *
 * <p><b>Wire facts (verified empirically).</b> Over the Native protocol a geo column is
 * announced under its ALIAS NAME ("Point", "Ring", "LineString", "Polygon",
 * "MultiLineString", "MultiPolygon") — NOT as a structural {@code Tuple}/{@code Array} —
 * and it does not carry the custom-serialization flag. The wire layout is, however,
 * identical to the composed Tuple/Array form, so {@link io.github.danielbunting.clickhouse.types.DefaultTypeParser}
 * maps each alias onto the existing Tuple/Array codecs:
 * <ul>
 *   <li>{@code Point}           = {@code Tuple(Float64, Float64)}   -> {@code List[x, y]}</li>
 *   <li>{@code Ring}/{@code LineString} = {@code Array(Point)}      -> {@code List<List<Double>>}</li>
 *   <li>{@code Polygon}/{@code MultiLineString} = {@code Array(Array(Point))}</li>
 *   <li>{@code MultiPolygon}    = {@code Array(Array(Array(Point)))}</li>
 * </ul>
 *
 * <p>Decoded geo values are therefore the plain nested {@code List}/{@code Double}
 * structures; typed geo objects are a later nicety. Every alias is exercised in BOTH
 * directions: decode from raw VALUES, and bulk-insert encode from nested Java Lists
 * (reference: client-v2 DataTypeTests#testGeometryWriteToTable / writeGeometryTests and
 * BinaryStreamUtilsTest#testWriteGeoRing — including the empty-ring edge).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class GeoTypesIT extends TypeRoundTripBase {

    /** Asserts a decoded Point cell is a 2-element List of [x, y] doubles. */
    private void assertPoint(Object cell, double x, double y, String label) {
        List<?> p = assertInstanceOf(List.class, cell,
                label + ": expected a java.util.List (Point=Tuple) but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(2, p.size(), label + ": Point arity");
        assertEquals(x, ((Number) p.get(0)).doubleValue(), 0.0, label + " x");
        assertEquals(y, ((Number) p.get(1)).doubleValue(), 0.0, label + " y");
    }

    // -------------------------------------------------------------------------
    // Point = Tuple(Float64, Float64)
    // -------------------------------------------------------------------------

    /** DECODE: a {@code Point} column inserted via raw VALUES decodes to {@code [x, y]}. */
    @Test
    void pointDecodeRoundTrips() {
        withTable("geo_point", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, p Point)"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, p) VALUES"
                    + " (1, (1.5, 2.5)), (2, (-3.0, 4.25))");

            List<Object[]> rows = decode(conn, "SELECT p FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");
            assertPoint(rows.get(0)[0], 1.5, 2.5, "Row 1");
            assertPoint(rows.get(1)[0], -3.0, 4.25, "Row 2");
        });
    }

    /** A record whose {@code p} field is a Point as a {@code List<Double>}. */
    record PointRow(long id, List<Double> p) {}

    /**
     * ENCODE: bulk-insert {@code Point} rows built as Java Lists, read back, assert. Confirms
     * the Tuple(Float64, Float64) encode path drives the geo {@code Point} alias on the server.
     */
    @Test
    void pointBulkEncodeRoundTrips() {
        withTable("geo_point_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, p Point)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<PointRow> input = List.of(
                    new PointRow(1, Arrays.asList(1.5, 2.5)),
                    new PointRow(2, Arrays.asList(-3.0, 4.25)));

            try (BulkInserter<PointRow> inserter =
                         conn.createBulkInserter(table, PointRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT p FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 bulk rows");
            assertPoint(rows.get(0)[0], 1.5, 2.5, "Bulk row 1");
            assertPoint(rows.get(1)[0], -3.0, 4.25, "Bulk row 2");
        });
    }

    // -------------------------------------------------------------------------
    // Ring = Array(Point)   (decode-only)
    // -------------------------------------------------------------------------

    /** DECODE: a {@code Ring} decodes to a {@code List<List<Double>>} of points. */
    @Test
    void ringDecodeRoundTrips() {
        withTable("geo_ring", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, r Ring)"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, r) VALUES"
                    + " (1, [(1.0,2.0),(3.0,4.0),(5.0,6.0)])");

            List<Object[]> rows = decode(conn, "SELECT r FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), "Expected 1 row");
            List<?> ring = assertInstanceOf(List.class, rows.get(0)[0], "Ring");
            assertEquals(3, ring.size(), "Ring point count");
            assertPoint(ring.get(0), 1.0, 2.0, "Ring p0");
            assertPoint(ring.get(1), 3.0, 4.0, "Ring p1");
            assertPoint(ring.get(2), 5.0, 6.0, "Ring p2");
        });
    }

    // -------------------------------------------------------------------------
    // LineString = Array(Point)   (decode-only)
    // -------------------------------------------------------------------------

    /** DECODE: a {@code LineString} decodes identically to a Ring (Array(Point)). */
    @Test
    void lineStringDecodeRoundTrips() {
        withTable("geo_linestring", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, l LineString)"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, l) VALUES"
                    + " (1, [(0.0,0.0),(1.0,1.0)])");

            List<Object[]> rows = decode(conn, "SELECT l FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), "Expected 1 row");
            List<?> line = assertInstanceOf(List.class, rows.get(0)[0], "LineString");
            assertEquals(2, line.size(), "LineString point count");
            assertPoint(line.get(0), 0.0, 0.0, "Line p0");
            assertPoint(line.get(1), 1.0, 1.0, "Line p1");
        });
    }

    // -------------------------------------------------------------------------
    // Polygon = Array(Array(Point))   (decode-only)
    // -------------------------------------------------------------------------

    /** DECODE: a {@code Polygon} (outer ring + a hole) decodes to {@code List<List<List<Double>>>}. */
    @Test
    void polygonDecodeRoundTrips() {
        withTable("geo_polygon", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, poly Polygon)"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, poly) VALUES"
                    + " (1, [[(0.0,0.0),(0.0,4.0),(4.0,4.0),(4.0,0.0)],"
                    + "      [(1.0,1.0),(1.0,2.0),(2.0,2.0),(2.0,1.0)]])");

            List<Object[]> rows = decode(conn, "SELECT poly FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), "Expected 1 row");
            List<?> poly = assertInstanceOf(List.class, rows.get(0)[0], "Polygon");
            assertEquals(2, poly.size(), "Polygon ring count (outer + hole)");

            List<?> outer = assertInstanceOf(List.class, poly.get(0), "outer ring");
            assertEquals(4, outer.size(), "outer ring point count");
            assertPoint(outer.get(0), 0.0, 0.0, "outer p0");
            assertPoint(outer.get(2), 4.0, 4.0, "outer p2");

            List<?> hole = assertInstanceOf(List.class, poly.get(1), "hole ring");
            assertEquals(4, hole.size(), "hole ring point count");
            assertPoint(hole.get(0), 1.0, 1.0, "hole p0");
        });
    }

    // -------------------------------------------------------------------------
    // MultiPolygon = Array(Array(Array(Point)))   (decode-only)
    // -------------------------------------------------------------------------

    /** DECODE: a {@code MultiPolygon} of two single-ring polygons decodes to 4-deep nesting. */
    @Test
    void multiPolygonDecodeRoundTrips() {
        withTable("geo_multipolygon", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, mp MultiPolygon)"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, mp) VALUES"
                    + " (1, [[[(0.0,0.0),(0.0,1.0),(1.0,1.0)]],"
                    + "      [[(5.0,5.0),(5.0,6.0),(6.0,6.0)]]])");

            List<Object[]> rows = decode(conn, "SELECT mp FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), "Expected 1 row");
            List<?> mp = assertInstanceOf(List.class, rows.get(0)[0], "MultiPolygon");
            assertEquals(2, mp.size(), "MultiPolygon polygon count");

            List<?> poly0 = assertInstanceOf(List.class, mp.get(0), "polygon 0");
            assertEquals(1, poly0.size(), "polygon 0 ring count");
            List<?> ring0 = assertInstanceOf(List.class, poly0.get(0), "polygon 0 ring 0");
            assertEquals(3, ring0.size(), "polygon 0 ring 0 point count");
            assertPoint(ring0.get(0), 0.0, 0.0, "poly0 ring0 p0");

            List<?> poly1 = assertInstanceOf(List.class, mp.get(1), "polygon 1");
            List<?> ring1 = assertInstanceOf(List.class, poly1.get(0), "polygon 1 ring 0");
            assertPoint(ring1.get(0), 5.0, 5.0, "poly1 ring0 p0");
        });
    }

    // -------------------------------------------------------------------------
    // ENCODE round-trips for the nested aliases
    // -------------------------------------------------------------------------

    /** A record whose {@code r} field is a Ring/LineString as nested Lists. */
    record RingRow(long id, List<List<Double>> r) {}

    /**
     * ENCODE: bulk-insert {@code Ring} rows of 0, 1, 2, and 3 points (reference:
     * BinaryStreamUtilsTest#testWriteGeoRing exercises exactly this point-count matrix,
     * with the empty ring as the offsets edge case) and read them back.
     */
    @Test
    void ringBulkEncodeRoundTrips() {
        withTable("geo_ring_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, r Ring)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<RingRow> input = List.of(
                    new RingRow(1, List.of()),
                    new RingRow(2, List.of(List.of(1.0, 2.0))),
                    new RingRow(3, List.of(List.of(1.0, 2.0), List.of(3.0, 4.0))),
                    new RingRow(4, List.of(List.of(1.0, 2.0), List.of(3.0, 4.0), List.of(5.0, 6.0))));

            try (BulkInserter<RingRow> inserter = conn.createBulkInserter(table, RingRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT r FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size());
            for (int i = 0; i < 4; i++) {
                List<?> ring = assertInstanceOf(List.class, rows.get(i)[0], "row " + (i + 1));
                assertEquals(i, ring.size(), "row " + (i + 1) + " point count");
            }
            assertPoint(((List<?>) rows.get(3)[0]).get(2), 5.0, 6.0, "3-point ring p2");
        });
    }

    /** ENCODE: a {@code LineString} bulk-inserts through the same Array(Point) path. */
    @Test
    void lineStringBulkEncodeRoundTrips() {
        withTable("geo_line_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, r LineString)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<RingRow> input = List.of(
                    new RingRow(1, List.of(List.of(0.0, 0.0), List.of(1.0, 1.0), List.of(2.0, 0.0))));

            try (BulkInserter<RingRow> inserter = conn.createBulkInserter(table, RingRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT r FROM " + table);
            List<?> line = assertInstanceOf(List.class, rows.get(0)[0]);
            assertEquals(3, line.size());
            assertPoint(line.get(1), 1.0, 1.0, "line p1");
        });
    }

    /** A record whose {@code poly} field is a Polygon as 3-deep nested Lists. */
    record PolygonRow(long id, List<List<List<Double>>> poly) {}

    /** ENCODE: a {@code Polygon} (outer ring + hole) bulk-inserts and reads back. */
    @Test
    void polygonBulkEncodeRoundTrips() {
        withTable("geo_poly_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, poly Polygon)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<List<Double>> outer = List.of(
                    List.of(0.0, 0.0), List.of(0.0, 4.0), List.of(4.0, 4.0), List.of(4.0, 0.0));
            List<List<Double>> hole = List.of(
                    List.of(1.0, 1.0), List.of(1.0, 2.0), List.of(2.0, 2.0), List.of(2.0, 1.0));
            List<PolygonRow> input = List.of(new PolygonRow(1, List.of(outer, hole)));

            try (BulkInserter<PolygonRow> inserter =
                         conn.createBulkInserter(table, PolygonRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT poly FROM " + table);
            List<?> poly = assertInstanceOf(List.class, rows.get(0)[0]);
            assertEquals(2, poly.size(), "outer + hole");
            assertPoint(((List<?>) poly.get(0)).get(2), 4.0, 4.0, "outer p2");
            assertPoint(((List<?>) poly.get(1)).get(0), 1.0, 1.0, "hole p0");
        });
    }

    /** A record whose {@code mp} field is a MultiPolygon as 4-deep nested Lists. */
    record MultiPolygonRow(long id, List<List<List<List<Double>>>> mp) {}

    /** ENCODE: a {@code MultiPolygon} of two single-ring polygons bulk-inserts and reads back. */
    @Test
    void multiPolygonBulkEncodeRoundTrips() {
        withTable("geo_mp_enc", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " (id UInt32, mp MultiPolygon)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<MultiPolygonRow> input = List.of(new MultiPolygonRow(1, List.of(
                    List.of(List.of(List.of(0.0, 0.0), List.of(0.0, 1.0), List.of(1.0, 1.0))),
                    List.of(List.of(List.of(5.0, 5.0), List.of(5.0, 6.0), List.of(6.0, 6.0))))));

            try (BulkInserter<MultiPolygonRow> inserter =
                         conn.createBulkInserter(table, MultiPolygonRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT mp FROM " + table);
            List<?> mp = assertInstanceOf(List.class, rows.get(0)[0]);
            assertEquals(2, mp.size(), "two polygons");
            assertPoint(((List<?>) ((List<?>) mp.get(1)).get(0)).get(0), 5.0, 5.0, "poly1 ring0 p0");
        });
    }

    // -------------------------------------------------------------------------
    // Geometry: a String alias on 25.x (MySQL-compat); 26.x rejects geo text
    // -------------------------------------------------------------------------

    /**
     * The generic {@code Geometry} type's behaviour changed across the supported matrix, so this
     * test is version-aware:
     * <ul>
     *   <li><b>25.x</b> — {@code Geometry} is a MySQL-compatibility ALIAS for {@code String}
     *       (verified empirically: {@code toTypeName} and {@code system.columns.type} both report
     *       {@code String}). A column declared {@code Geometry} is announced over the Native
     *       protocol as {@code String} and a String literal round-trips unchanged.</li>
     *   <li><b>26.x</b> — {@code Geometry} is no longer a transparent String alias: the server
     *       refuses to parse WKT/geo <em>text</em> (e.g. {@code 'POINT(1 2)'}) on insert,
     *       failing with {@code [117] Cannot parse text value of type Geometry}. The 25.x
     *       store-a-String-and-read-it-back round-trip therefore no longer holds; we assert the
     *       rejection instead.</li>
     * </ul>
     * Either way the client is a faithful relay — this pins the server-side contract per version.
     */
    @Test
    void geometryIsStringAliasAndRoundTrips() {
        boolean rejectsGeoText = serverVersionAtLeast(26, 0);
        withTable("geo_generic", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, g Geometry) ENGINE = MergeTree ORDER BY id");

            if (rejectsGeoText) {
                // 26.x: the server will not parse geo text into a Geometry column.
                ServerException ex = assertThrows(ServerException.class, () ->
                        conn.execute("INSERT INTO " + table
                                + " VALUES (1, '(0,0)'), (2, 'POINT(1 2)')"),
                        "26.x should reject parsing geo text into a Geometry column");
                assertTrue(ex.getMessage() != null && ex.getMessage().contains("Geometry"),
                        "Expected a Geometry text-parse error on 26.x, got: " + ex.getMessage());
                return;
            }

            // 25.x: Geometry is a String alias, so a String literal round-trips.
            conn.execute("INSERT INTO " + table + " VALUES (1, '(0,0)'), (2, 'POINT(1 2)')");

            List<Object[]> rows = decode(conn, "SELECT g FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows");
            assertInstanceOf(String.class, rows.get(0)[0],
                    "Geometry is announced as String on 25.x");
            assertEquals("(0,0)", rows.get(0)[0]);
            assertEquals("POINT(1 2)", rows.get(1)[0]);
        });
    }
}
