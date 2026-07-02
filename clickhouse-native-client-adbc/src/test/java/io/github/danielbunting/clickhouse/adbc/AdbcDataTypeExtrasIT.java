package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Data-type edge coverage at the ADBC surface, ported from the JDBC module's
 * {@code JdbcDataTypeExtrasIT} (itself ported from clickhouse-java jdbc-v2): Array(UUID)/
 * Array(IP), Map with Array values, SimpleAggregateFunction, Nested (flattened and
 * {@code flatten_nested = 0}), JSON as text, Dynamic/Variant reads, geo shapes, and the
 * supported-type boundary (AggregateFunction state → NOT_IMPLEMENTED).
 *
 * <p>Wherever the core client and Arrow canonicalise to the same value space the assertions go
 * through the {@link AdbcRoundTripBase} equivalence harness; representation-divergent families
 * (IPv4 → unsigned int, JSON/Dynamic/Variant → Utf8) get explicit vector assertions instead,
 * mirroring {@link AdbcTypeRepresentationIT}'s approach.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcDataTypeExtrasIT extends AdbcRoundTripBase {

    // ---- arrays of representation-shared types (equivalence-safe) -----------------------------

    @Test
    @DisplayName("UUID scalars and Array(UUID) agree with the core client")
    void arrayOfUuidEquivalence(BufferAllocator allocator) throws Exception {
        String uuid = "2d1f626d-eb07-4c81-be3d-ac1173f0d018";
        assertSameAsCore(allocator,
                "SELECT '" + uuid + "'::UUID AS elem, ['" + uuid + "']::Array(UUID) AS arr");
    }

    @Test
    @DisplayName("Array(IPv6) and Array(Nullable(IPv6)) agree with the core client")
    void arrayOfIpv6Equivalence(BufferAllocator allocator) throws Exception {
        assertSameAsCore(allocator,
                "SELECT ['2001:adb8:85a3:1:2:8a2e:370:7334'::IPv6] AS a6, "
                        + "['2001:adb8:85a3:1:2:8a2e:370:7334'::IPv6, NULL] AS a6n");
    }

    @Test
    @DisplayName("Array(IPv4) elements surface as unsigned 32-bit values")
    void arrayOfIpv4RawValues(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            List<List<Object>> rows = viaAdbc(adbc, "SELECT ['192.168.0.1'::IPv4] AS a4");
            assertEquals(List.of(List.of(0xC0A80001L)), List.of(rows.get(0).get(0)),
                    "IPv4 keeps the unsigned-int representation inside arrays");
        } finally {
            database.close();
        }
    }

    // ---- Map with Array values ------------------------------------------------------------------

    @Test
    @DisplayName("Map(String, Array(Int32)) including an empty-array value agrees with the core client")
    void mapWithArrayValues(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_map_arr");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int8, m Map(String, Array(Int32))) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, map('k0', [1, 2, 3], 'k1', []))");
                assertEquals(viaCore(core, "SELECT m FROM " + table),
                        viaAdbc(adbc, "SELECT m FROM " + table));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- SimpleAggregateFunction ------------------------------------------------------------------

    @Test
    @DisplayName("SimpleAggregateFunction columns read as their inner type, including Nullable inner")
    void simpleAggregateFunctionColumn(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_saf");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table + " (k Int8, "
                    + "v SimpleAggregateFunction(any, Nullable(Int8)), "
                    + "s SimpleAggregateFunction(sum, UInt64)"
                    + ") ENGINE = AggregatingMergeTree ORDER BY k");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, NULL, 100), (2, 7, 250), (3, NULL, 300)");
                String sql = "SELECT v, s FROM " + table + " ORDER BY k";
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql),
                        "the SAF wrapper must be transparent to the reader");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- Nested ----------------------------------------------------------------------------------

    @Test
    @DisplayName("flattened Nested sub-columns (n.a / n.b) and the re-assembled whole column read equal to core")
    void nestedFlattenedSubcolumns(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_nested_flat");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int8, n Nested(a UInt32, b String)) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " (id, `n.a`, `n.b`) VALUES (1, [10, 20], ['x', 'y'])");
                String subColumns = "SELECT `n.a`, `n.b` FROM " + table + " ORDER BY id";
                assertEquals(viaCore(core, subColumns), viaAdbc(adbc, subColumns));

                String whole = "SELECT n FROM " + table;
                assertEquals(viaCore(core, whole), viaAdbc(adbc, whole),
                        "the server-re-assembled Array(Tuple) must bridge to a list of structs");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("flatten_nested = 0 stores Nested whole; it reads as one Array(Tuple) equal to core")
    void nestedNonFlattenedWholeColumn(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_nested_nf");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            // The session setting must be live on the DDL connection; one ADBC connection
            // = one native session, so a SET through a statement persists for the CREATE.
            execDdl(adbc, "SET flatten_nested = 0");
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int8, n Nested(a Int32, b String)) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, [(1, 'x'), (2, 'y')])");
                String sql = "SELECT n FROM " + table;
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- JSON / Dynamic / Variant (deliberate Utf8 degradation — explicit asserts) -----------------

    @Test
    @DisplayName("a JSON column reads back as JSON text per row, omitting absent paths")
    void jsonColumnReadsAsJsonText(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_json");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "SET allow_experimental_json_type = 1");
            execDdl(adbc, "CREATE TABLE " + table + " (id Int8, j JSON) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES "
                        + "(1, '{\"key1\": \"value1\", \"key2\": 42}'), (2, '{\"key1\": \"value2\"}')");
                List<String> rows = readStrings(adbc, "SELECT j FROM " + table + " ORDER BY id");
                assertEquals(2, rows.size());
                assertTrue(rows.get(0).contains("\"key1\":\"value1\""), rows.get(0));
                assertTrue(rows.get(0).contains("\"key2\":42"), rows.get(0));
                assertTrue(rows.get(1).contains("\"key1\":\"value2\""), rows.get(1));
                assertFalse(rows.get(1).contains("key2"), "absent path must be omitted: " + rows.get(1));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a Dynamic column renders each row's runtime value as a string")
    void dynamicColumnRendersPerRowValues(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_dynamic");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "SET allow_experimental_dynamic_type = 1");
            execDdl(adbc, "CREATE TABLE " + table + " (id Int8, d Dynamic) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, 'str7'), (2, 42), (3, 0.5)");
                List<String> rows = readStrings(adbc, "SELECT d FROM " + table + " ORDER BY id");
                assertEquals("str7", rows.get(0));
                assertEquals(42L, Long.parseLong(rows.get(1)), "numeric member renders as its value");
                assertEquals(0.5, Double.parseDouble(rows.get(2)));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a Variant column renders members as strings and NULL as a null cell")
    void variantColumnRendersMembersAndNull(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_variant");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "SET allow_experimental_variant_type = 1");
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int8, v Variant(String, Int32)) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, 'vstr'), (2, 42), (3, NULL)");
                List<String> rows = readStrings(adbc, "SELECT v FROM " + table + " ORDER BY id");
                assertEquals("vstr", rows.get(0));
                assertEquals(42L, Long.parseLong(rows.get(1)));
                assertNull(rows.get(2), "a NULL variant must be a null cell, not a rendered string");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- geo shapes (Tuple/Array structures — equivalence-safe) ------------------------------------

    @Test
    @DisplayName("Point, Ring and LineString bridge as (nested) float structs equal to core")
    void geoPointRingLineString(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_geo_prl");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "SET allow_experimental_geo_types = 1");
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int8, p Point, r Ring, l LineString) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, "
                        + "(1.5, 2.5), "
                        + "[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 0.0)], "
                        + "[(1.0, 2.0), (3.0, 4.0)])");
                String sql = "SELECT p, r, l FROM " + table;
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("MultiLineString, Polygon and MultiPolygon bridge their deep nesting equal to core")
    void geoMultiLineStringPolygonMultiPolygon(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_geo_poly");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "SET allow_experimental_geo_types = 1");
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int8, ml MultiLineString, poly Polygon, mp MultiPolygon) ENGINE = Memory");
            try {
                execDdl(adbc, "INSERT INTO " + table + " VALUES (1, "
                        + "[[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]], "
                        + "[[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)],"
                        + " [(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)]], "
                        + "[[[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0)]],"
                        + " [[(10.0, 10.0), (10.0, 14.0), (14.0, 14.0)]]])");
                String sql = "SELECT ml, poly, mp FROM " + table;
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- supported-type boundary --------------------------------------------------------------------

    @Test
    @DisplayName("selecting an AggregateFunction state column raises NOT_IMPLEMENTED, not a crash")
    void aggregateFunctionStateIsNotImplemented(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_dt_aggstate");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (k Int8, state AggregateFunction(sum, UInt64))"
                    + " ENGINE = AggregatingMergeTree ORDER BY k");
            try (AdbcStatement statement = adbc.createStatement()) {
                statement.setSqlQuery("SELECT state FROM " + table);
                AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
                assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus(),
                        "an opaque aggregation state is a supported-type boundary, not an IO failure");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------------

    /** Core-vs-ADBC equivalence over fresh connections for a standalone SELECT. */
    private void assertSameAsCore(BufferAllocator allocator, String sql) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            assertEquals(viaCore(core, sql), viaAdbc(adbc, sql), sql);
        } finally {
            database.close();
        }
    }

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }

    /** Drains a one-VarChar-column query into strings (null cells become null entries). */
    private static List<String> readStrings(AdbcConnection adbc, String sql) throws Exception {
        List<String> out = new ArrayList<>();
        try (AdbcStatement statement = adbc.createStatement()) {
            statement.setSqlQuery(sql);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    VarCharVector vector = (VarCharVector) root.getVector(0);
                    for (int r = 0; r < root.getRowCount(); r++) {
                        out.add(vector.isNull(r) ? null : String.valueOf(vector.getObject(r)));
                    }
                }
            }
        }
        return out;
    }
}
