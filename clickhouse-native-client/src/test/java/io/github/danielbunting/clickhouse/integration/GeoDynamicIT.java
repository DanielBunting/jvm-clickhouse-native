package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Geo values held inside a {@code Dynamic} column (reference: client-v2
 * SerializerUtilsTests#testDynamicTypeTagUsesCustomEncodingForGeoTypes — the v2 client
 * needed a custom binary type tag for geo member types inside Dynamic).
 *
 * <p>Over this client's Native-protocol FLATTENED Dynamic serialization the member type
 * names travel as plain strings ("Point", "LineString", ...) discovered on the wire, and
 * {@link io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec} resolves each
 * one through {@link io.github.danielbunting.clickhouse.types.DefaultTypeParser}, which maps
 * the geo aliases onto the Tuple/Array codecs (see {@code GeoTypesIT}). So a Dynamic cell
 * holding a geo value decodes to the same nested {@code List}/{@code Double} boxing as a
 * dedicated geo column — that DECODE path is what these tests pin.
 *
 * <p>ENCODE of a geo value into a Dynamic column is <b>unsupported by design</b>:
 * {@code DynamicColumnCodec.inferClickHouseType} maps only scalar Java classes
 * (Long/Integer/Short/Byte/Double/Float/Boolean/CharSequence) and rejects everything else
 * — a {@code List}-boxed geo value has no inferable ClickHouse type. The last test pins
 * that rejection so the boundary is documented rather than silent.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class GeoDynamicIT extends TypeRoundTripBase {

    /** Asserts a decoded Point cell is a 2-element List of [x, y] doubles. */
    private void assertPoint(Object cell, double x, double y, String label) {
        List<?> p = assertInstanceOf(List.class, cell,
                label + ": expected a java.util.List (Point=Tuple) but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
        assertEquals(2, p.size(), label + ": Point arity");
        assertEquals(x, ((Number) p.get(0)).doubleValue(), 0.0, label + " x");
        assertEquals(y, ((Number) p.get(1)).doubleValue(), 0.0, label + " y");
    }

    /**
     * DECODE: a {@code Dynamic} column holding {@code Point}, {@code LineString},
     * {@code Polygon} and NULL rows round-trips through the flattened Dynamic wire
     * format; {@code dynamicType(d)} confirms the server really stored the geo member
     * types (i.e. the type tags on the wire were the geo aliases, not a lossy fallback).
     */
    @Test
    void dynamicHoldingGeoValuesDecodes() {
        withTable("dyn_geo", (conn, table) -> {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, CAST((1.5, 2.5), 'Point')),"
                    + " (2, CAST([(0.0, 0.0), (1.0, 1.0)], 'LineString')),"
                    + " (3, CAST([[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)]], 'Polygon'))");
            // The NULL row goes in its own INSERT: a bare NULL in the same VALUES list as
            // geo (Tuple-shaped) rows is stored by the server as the String 'NULL', not as
            // NULL (server-side inference quirk, confirmed via toString(d)/dynamicType(d);
            // see VariantDynamicCompositeIT's class javadoc). A standalone NULL stays NULL.
            conn.execute("INSERT INTO " + table + " (id, d) VALUES (4, NULL)");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size(), "Expected 4 Dynamic rows");

            // Row 1: Point -> List[x, y]
            assertEquals("Point", rows.get(0)[1], "row 1 dynamicType");
            assertPoint(rows.get(0)[0], 1.5, 2.5, "Dynamic Point");

            // Row 2: LineString -> List<Point>
            assertEquals("LineString", rows.get(1)[1], "row 2 dynamicType");
            List<?> line = assertInstanceOf(List.class, rows.get(1)[0], "Dynamic LineString");
            assertEquals(2, line.size(), "LineString point count");
            assertPoint(line.get(0), 0.0, 0.0, "Line p0");
            assertPoint(line.get(1), 1.0, 1.0, "Line p1");

            // Row 3: Polygon -> List<Ring>
            assertEquals("Polygon", rows.get(2)[1], "row 3 dynamicType");
            List<?> poly = assertInstanceOf(List.class, rows.get(2)[0], "Dynamic Polygon");
            assertEquals(1, poly.size(), "Polygon ring count");
            List<?> ring = assertInstanceOf(List.class, poly.get(0), "Polygon ring 0");
            assertEquals(4, ring.size(), "ring point count");
            assertPoint(ring.get(0), 0.0, 0.0, "ring p0");
            assertPoint(ring.get(2), 4.0, 4.0, "ring p2");

            // Row 4: NULL Dynamic ("None" member type)
            assertNull(rows.get(3)[0], "NULL Dynamic row should decode to null");
            assertEquals("None", rows.get(3)[1], "row 4 dynamicType");
        });
    }

    /**
     * DECODE: geo member types mixed with scalar member types in the SAME Dynamic column
     * — each discriminator resolves to its own codec, so a {@code Point} row and an
     * {@code Int64} row coexist in one flattened block.
     */
    @Test
    void dynamicMixesGeoAndScalarMembers() {
        withTable("dyn_geo_mix", (conn, table) -> {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, d) VALUES"
                    + " (1, CAST((-3.0, 4.25), 'Point')),"
                    + " (2, 42::Int64),"
                    + " (3, 'hello')");

            List<Object[]> rows = decode(conn,
                    "SELECT d, dynamicType(d) FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            assertEquals("Point", rows.get(0)[1]);
            assertPoint(rows.get(0)[0], -3.0, 4.25, "mixed Point");

            assertEquals("Int64", rows.get(1)[1]);
            assertEquals(42L, ((Number) rows.get(1)[0]).longValue(), "Int64 member");

            assertEquals("String", rows.get(2)[1]);
            assertEquals("hello", rows.get(2)[0], "String member");
        });
    }

    /** A row whose Dynamic value is an arbitrary {@link Object}. */
    record DynRow(long id, Object d) {}

    /**
     * ENCODE boundary: writing a geo value (a {@code List}-boxed Point) INTO a Dynamic
     * column is unsupported — {@code DynamicColumnCodec.inferClickHouseType} has no
     * mapping for {@code java.util.List}, so the write path rejects it with an
     * {@link IllegalArgumentException} naming the unsupported Java type. Mirrors the
     * reference row's concern (geo type tags in Dynamic encoding) at this client's
     * actual capability boundary.
     *
     * <p>PINNED side effect: the rejection is raised while the block is already being
     * streamed (header and sibling columns are on the socket), so the connection is left
     * mid-protocol and POISONED — subsequent use fails server-side ("Unknown codec family
     * code"). This test therefore manages its table on a second connection and pins
     * {@code isPoisoned()} rather than reusing the broken connection.
     */
    @Test
    void dynamicGeoEncodeIsRejected() {
        String table = "dyn_geo_enc_" + System.nanoTime();
        ClickHouseConnection conn = ClickHouseConnection.open(config());
        try {
            conn.execute("SET allow_experimental_dynamic_type = 1");
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, d Dynamic) ENGINE = MergeTree() ORDER BY id");

            List<DynRow> input = List.of(new DynRow(1, Arrays.asList(1.5, 2.5)));

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
                try (BulkInserter<DynRow> inserter =
                             conn.createBulkInserter(table, DynRow.class)) {
                    inserter.init();
                    inserter.addRange(input);
                    inserter.complete();
                }
            }, "encoding a List-boxed geo value into Dynamic must be rejected");
            assertTrue(ex.getMessage() != null
                            && ex.getMessage().contains("No Dynamic type inference"),
                    "expected the inference rejection, got: " + ex);

            assertTrue(conn.isPoisoned(),
                    "the mid-block rejection leaves the connection poisoned");
        } finally {
            try {
                conn.close();
            } catch (RuntimeException ignored) {
                // Closing the poisoned connection may itself fail; the socket is gone either way.
            }
            try (ClickHouseConnection cleanup = ClickHouseConnection.open(config())) {
                cleanup.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }
}
