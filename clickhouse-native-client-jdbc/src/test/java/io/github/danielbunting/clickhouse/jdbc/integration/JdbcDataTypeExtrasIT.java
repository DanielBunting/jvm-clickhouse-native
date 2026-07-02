package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Data-type edge coverage at the JDBC surface, ported from the official clickhouse-java
 * jdbc-v2 {@code JdbcDataTypeTests}: Array(UUID)/Array(IP) getters, Decimal scale
 * truncation on write, float parameters bound into Decimal columns, setBytes into
 * FixedString, Map with Array values, SimpleAggregateFunction, Nested (flattened and
 * {@code flatten_nested=0}), JSON written/read as text, Dynamic and Variant reads, and
 * geo type reads. Codec fidelity is proven in the core module's per-type ITs; these
 * tests pin how those values surface through {@code java.sql} accessors.
 *
 * <p>Known divergences from jdbc-v2 asserted here as this driver's contract:
 * <ul>
 *   <li>{@code Array.getResultSet()} is unsupported; arrays are consumed via
 *       {@code getArray()} (see {@code ChArray}).</li>
 *   <li>{@code Connection.createArrayOf}/{@code createStruct} are unsupported, so geo
 *       values cannot be bound through {@code PreparedStatement} (jdbc-v2
 *       {@code testGeoGeometryPreparedStatement} has no equivalent write path).</li>
 *   <li>JSON columns decode to a JSON text {@link String}, not a {@code Map} (jdbc-v2
 *       {@code testReadingJSONBinary} object semantics do not apply).</li>
 *   <li>Geo values box as nested {@code java.util.List}s (Point = {@code List[x, y]}),
 *       not primitive {@code double[]} arrays.</li>
 * </ul>
 */
@Tag("integration")
@Testcontainers
class JdbcDataTypeExtrasIT {

    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks></default></users></clickhouse>";

    private static final int NATIVE_PORT = 9000;

    @Container
    @SuppressWarnings("resource")
    private static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(ClickHouseImages.SERVER)
                    .withExposedPorts(NATIVE_PORT)
                    .withCopyToContainer(
                            Transferable.of(OPEN_DEFAULT_USER_XML),
                            "/etc/clickhouse-server/users.d/zz-open-default.xml")
                    .waitingFor(Wait.forListeningPort());

    private static Connection connect() throws SQLException {
        String url = "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
        return DriverManager.getConnection(url);
    }

    // ---- Array(UUID) (jdbc-v2 testArrayOfUUID) -------------------------------

    @Test
    void arrayOfUuid() throws Exception {
        UUID uuid = UUID.fromString("2d1f626d-eb07-4c81-be3d-ac1173f0d018");
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT '" + uuid + "'::UUID elem, ['" + uuid + "']::Array(UUID) arr")) {
            assertTrue(rs.next());
            assertEquals(uuid, rs.getObject(1));
            Array arr = rs.getArray(2);
            assertEquals("UUID", arr.getBaseTypeName());
            Object[] values = (Object[]) arr.getArray();
            assertEquals(1, values.length);
            assertEquals(rs.getObject(1), values[0]);
        }
    }

    // ---- Array(IPv4) / Array(IPv6) / Array(Nullable(IPv6)) -------------------
    // (jdbc-v2 testArrayOfIpAddress)

    @Test
    void arrayOfIpAddresses() throws Exception {
        InetAddress v4 = InetAddress.getByName("90.176.75.97");
        InetAddress v6 = InetAddress.getByName("2001:adb8:85a3:1:2:8a2e:370:7334");
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT ['90.176.75.97'::IPv4] a4, "
                        + "['2001:adb8:85a3:1:2:8a2e:370:7334'::IPv6] a6, "
                        + "['2001:adb8:85a3:1:2:8a2e:370:7334'::IPv6, NULL] a6n")) {
            assertTrue(rs.next());
            assertArrayEquals(new Object[] {v4}, (Object[]) rs.getArray(1).getArray());
            assertArrayEquals(new Object[] {v6}, (Object[]) rs.getArray(2).getArray());
            Array withNull = rs.getArray(3);
            assertEquals("Nullable(IPv6)", withNull.getBaseTypeName());
            assertArrayEquals(new Object[] {v6, null}, (Object[]) withNull.getArray());
        }
    }

    // ---- Decimal scale truncation on write (jdbc-v2 testDecimalTypesTruncateOnWriteAndRead)

    /**
     * {@code setBigDecimal} with more fractional digits than the column's scale lands
     * as the value truncated (not rounded) to the column scale, for positive and
     * negative values across every Decimal width; reads agree across
     * {@code getString}/{@code getBigDecimal}/{@code getObject}.
     */
    @Test
    void decimalScaleTruncationOnWrite() throws Exception {
        String table = "jdbc_dt_dec_trunc";
        BigDecimal[] written = {
                new BigDecimal("1234567.899"),
                new BigDecimal("12345.67891"),
                new BigDecimal("1234567890.123456789"),
                new BigDecimal("12345678901234567890.1234567890123456789"),
                new BigDecimal("1234567890123456789012345678901234567890.1234567890123456789"),
        };
        BigDecimal[] expected = {
                new BigDecimal("1234567.89"),
                new BigDecimal("12345.6789"),
                new BigDecimal("1234567890.12345678"),
                new BigDecimal("12345678901234567890.123456789012345678"),
                new BigDecimal("1234567890123456789012345678901234567890.123456789012345678"),
        };
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int8, dec Decimal(9, 2), "
                        + "dec32 Decimal32(4), dec64 Decimal64(8), dec128 Decimal128(18), "
                        + "dec256 Decimal256(18)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?, ?, ?, ?, ?)")) {
                ps.setInt(1, 1);
                for (int i = 0; i < written.length; i++) {
                    ps.setBigDecimal(i + 2, written[i]);
                }
                ps.executeUpdate();
                ps.setInt(1, 2);
                for (int i = 0; i < written.length; i++) {
                    ps.setBigDecimal(i + 2, written[i].negate());
                }
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertDecimalRow(rs, expected, false);
                assertTrue(rs.next());
                assertDecimalRow(rs, expected, true);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertDecimalRow(ResultSet rs, BigDecimal[] expected, boolean negated)
            throws SQLException {
        String[] columns = {"dec", "dec32", "dec64", "dec128", "dec256"};
        for (int i = 0; i < columns.length; i++) {
            BigDecimal want = negated ? expected[i].negate() : expected[i];
            assertEquals(want.toPlainString(), rs.getString(columns[i]), columns[i]);
            assertEquals(want, rs.getBigDecimal(columns[i]), columns[i]);
            assertEquals(want, rs.getObject(columns[i]), columns[i]);
            assertEquals(want, rs.getObject(columns[i], BigDecimal.class), columns[i]);
        }
    }

    // ---- float parameters into Decimal columns -------------------------------
    // (jdbc-v2 testDecimalTypesWithFractionalFloatParameters)

    /**
     * {@code setFloat} values chosen around float-mantissa boundaries bind into Decimal
     * columns exactly (no binary-float dust), at both scale 4 and scale 8.
     */
    @Test
    void floatParametersIntoDecimalColumns() throws Exception {
        String table = "jdbc_dt_dec_float";
        float[] values = {
                0.0001f, 0.0127f, 0.0128f, 0.0255f, 0.0256f,
                6.5535f, 6.5536f, 838.8607f, 838.8608f,
        };
        String[] expectedScale4 = {
                "0.0001", "0.0127", "0.0128", "0.0255", "0.0256",
                "6.5535", "6.5536", "838.8607", "838.8608",
        };
        String[] expectedScale8 = {
                "0.00010000", "0.01270000", "0.01280000", "0.02550000", "0.02560000",
                "6.55350000", "6.55360000", "838.86070000", "838.86080000",
        };
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int8, dec Decimal(9, 4), "
                        + "dec32 Decimal32(4), dec64 Decimal64(8)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < values.length; i++) {
                    ps.setInt(1, i + 1);
                    ps.setFloat(2, values[i]);
                    ps.setFloat(3, values[i]);
                    ps.setFloat(4, values[i]);
                    ps.executeUpdate();
                }
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM " + table + " ORDER BY id")) {
                for (int i = 0; i < values.length; i++) {
                    assertTrue(rs.next());
                    assertEquals(i + 1, rs.getInt("id"));
                    assertEquals(expectedScale4[i], rs.getString("dec"));
                    assertEquals(new BigDecimal(expectedScale4[i]), rs.getBigDecimal("dec"));
                    assertEquals(expectedScale4[i], rs.getString("dec32"));
                    assertEquals(new BigDecimal(expectedScale4[i]), rs.getObject("dec32"));
                    assertEquals(expectedScale8[i], rs.getString("dec64"));
                    assertEquals(new BigDecimal(expectedScale8[i]),
                            rs.getObject("dec64", BigDecimal.class));
                }
                assertFalse(rs.next());
            }
        }
    }

    // ---- setBytes into FixedString (jdbc-v2 testStringsUsedAsBytes) ----------
    // The String-column half is covered by
    // JdbcPreparedStatementExtrasIT#insertByteArrayIntoStringColumn; this adds the
    // FixedString dimension (server NUL-pads to the declared width).

    @Test
    void setBytesIntoFixedStringColumn() throws Exception {
        String table = "jdbc_dt_fixed_bytes";
        String[] payloads = {"FixedStr", "ABC"};
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int8, fixed FixedString(10)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                for (int i = 0; i < payloads.length; i++) {
                    ps.setInt(1, i + 1);
                    ps.setBytes(2, payloads[i].getBytes(StandardCharsets.UTF_8));
                    ps.executeUpdate();
                }
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT fixed FROM " + table + " ORDER BY id")) {
                for (String expected : payloads) {
                    assertTrue(rs.next());
                    // This client strips the server's NUL padding on decode (jdbc-v2
                    // returns the raw padded bytes), so the logical value comes back.
                    assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8),
                            rs.getBytes(1));
                    assertEquals(expected, rs.getString(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    // ---- Map(String, Array(Int32)) (jdbc-v2 testMapTypesWithArrayValues) -----

    /**
     * A Map whose values are arrays surfaces through {@code getObject} as a
     * {@code Map<String, List<Integer>>} and through {@code getString} as a ClickHouse
     * literal. jdbc-v2 also binds a Java {@code Map} via {@code setObject}; this
     * driver's parameter renderer has no Map form, so the write side here uses a SQL
     * literal (the Map-binding gap is asserted as a bug by
     * {@link #knownBug_setObjectMapMustBindMapColumns()}).
     */
    @Test
    void mapWithArrayValues() throws Exception {
        String table = "jdbc_dt_map_arr";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, m Map(String, Array(Int32))) ENGINE = Memory");
            st.execute("INSERT INTO " + table
                    + " VALUES (1, map('k0', [1, 2, 3], 'k1', []))");
            try (ResultSet rs = st.executeQuery("SELECT m FROM " + table)) {
                assertTrue(rs.next());
                Map<?, ?> map = assertInstanceOf(Map.class, rs.getObject(1));
                assertEquals(2, map.size());
                assertEquals(List.of(1, 2, 3), map.get("k0"));
                assertEquals(List.of(), map.get("k1"));
                assertEquals("{'k0': [1, 2, 3], 'k1': []}", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * KNOWN BUG — this test asserts the CORRECT behavior and fails until fixed.
     *
     * <p>Expected (jdbc-v2 {@code JdbcDataTypeTests#testMapTypesWithArrayValues}):
     * {@code setObject(1, Map.of("a", List.of(1, 2)))} into a
     * {@code Map(String, Array(Int32))} column binds as a ClickHouse map literal
     * {@code {'a':[1,2]}} and the row inserts and reads back as the same Map. Actual
     * (client-side binding mode): {@code ChPreparedStatement.toLiteral} has no Map
     * branch, so the value falls through to the string fallback and renders as
     * {@code '{a=[1, 2]}'} (Java's {@code Map.toString()}), which the server rejects
     * when parsing the VALUES tuple for the Map column.
     *
     * <p>HOW TO FIX: in
     * {@code src/main/java/io/github/danielbunting/clickhouse/jdbc/ChPreparedStatement.java},
     * method {@code toLiteral}, add a {@code java.util.Map} branch (before the generic
     * Collection/array branch) that renders {@code {'k':v, ...}} — recursing through
     * {@code toLiteral} for each key and value, mirroring the existing
     * {@code arrayLiteral} helper for Collections.
     */
    @Test
    void knownBug_setObjectMapMustBindMapColumns() throws Exception {
        String table = "jdbc_dt_map_bind";
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int8, m Map(String, Array(Int32))) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, Map.of("a", List.of(1, 2)));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT m FROM " + table)) {
                assertTrue(rs.next());
                Map<?, ?> map = assertInstanceOf(Map.class, rs.getObject(1));
                assertEquals(1, map.size());
                assertEquals(List.of(1, 2), map.get("a"));
                assertFalse(rs.next());
            }
        }
    }

    // ---- SimpleAggregateFunction (jdbc-v2 testSimpleAggregateFunction) -------

    /**
     * A {@code SimpleAggregateFunction} column reads through JDBC as its inner type,
     * including a {@code Nullable} inner type reporting SQL NULL via {@code wasNull};
     * aggregating over it works like any other column.
     */
    @Test
    void simpleAggregateFunctionColumn() throws Exception {
        String table = "jdbc_dt_saf";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table + " (k Int8, int8 Int8, "
                    + "v SimpleAggregateFunction(any, Nullable(Int8)), "
                    + "s SimpleAggregateFunction(sum, UInt64)"
                    + ") ENGINE = AggregatingMergeTree ORDER BY k");
            st.execute("INSERT INTO " + table + " VALUES "
                    + "(1, 5, NULL, 100), (2, 5, 7, 250), (3, 5, NULL, 300)");

            // Raw column reads: the SAF wrapper is transparent to the JDBC getters.
            try (ResultSet rs = st.executeQuery(
                    "SELECT v, s FROM " + table + " ORDER BY k")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertEquals(100L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals((byte) 7, rs.getByte(1));
                assertFalse(rs.wasNull());
                assertTrue(rs.next());
                assertFalse(rs.next());
            }

            // Aggregates over the table behave like jdbc-v2's reference test.
            try (ResultSet rs = st.executeQuery("SELECT sum(int8) FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(15, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery(
                    "SELECT any(v) FROM " + table + " WHERE k = 1")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    // ---- Nested, flattened (jdbc-v2 testNestedTypeSimpleStatement) ------------

    /**
     * A flattened Nested column exposes per-field Array sub-columns addressable as
     * {@code "n.a"} labels through {@code getArray}, and the whole column re-assembles
     * as an Array(Tuple) via {@code getObject}.
     */
    @Test
    void nestedFlattenedSubcolumns() throws Exception {
        String table = "jdbc_dt_nested_flat";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, n Nested(a UInt32, b String)) ENGINE = Memory");
            st.execute("INSERT INTO " + table + " (id, n.a, n.b) VALUES"
                    + " (1, [10, 20], ['x', 'y'])");

            try (ResultSet rs = st.executeQuery(
                    "SELECT n.a, n.b FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertArrayEquals(new Object[] {10L, 20L},
                        (Object[]) rs.getArray("n.a").getArray());
                assertArrayEquals(new Object[] {"x", "y"},
                        (Object[]) rs.getArray("n.b").getArray());
                assertFalse(rs.next());
            }

            // Whole-column read: the server re-assembles Array(Tuple(a, b)).
            try (ResultSet rs = st.executeQuery("SELECT n FROM " + table)) {
                assertTrue(rs.next());
                List<?> entries = assertInstanceOf(List.class, rs.getObject(1));
                assertEquals(2, entries.size());
                assertEquals(List.of(10L, "x"), entries.get(0));
                assertEquals(List.of(20L, "y"), entries.get(1));
            }
        }
    }

    // ---- Nested, flatten_nested = 0 (jdbc-v2 testNestedTypeNonFlatten) --------

    /**
     * With {@code flatten_nested = 0} the Nested column is stored whole; JDBC reads it
     * as one Array(Tuple) value through {@code getObject}/{@code getArray}.
     */
    @Test
    void nestedNonFlattenedWholeColumn() throws Exception {
        String table = "jdbc_dt_nested_nf";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET flatten_nested = 0");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, n Nested(a Int32, b String)) ENGINE = Memory");
            st.execute("INSERT INTO " + table + " VALUES (1, [(1, 'x'), (2, 'y')])");

            try (ResultSet rs = st.executeQuery("SELECT n FROM " + table)) {
                assertTrue(rs.next());
                List<?> entries = assertInstanceOf(List.class, rs.getObject(1));
                assertEquals(2, entries.size());
                assertEquals(List.of(1, "x"), entries.get(0));
                assertEquals(List.of(2, "y"), entries.get(1));

                Object[] materialised = (Object[]) rs.getArray(1).getArray();
                assertEquals(2, materialised.length);
                assertArrayEquals(new Object[] {1, "x"}, (Object[]) materialised[0]);
                assertFalse(rs.next());
            }
        }
    }

    // ---- JSON written and read as text ----------------------------------------
    // (jdbc-v2 testJSONWritingAsString; testReadingJSONBinary/testJSONRead are N/A —
    // this client decodes JSON columns to a JSON text String, not to Map objects.)

    @Test
    void jsonColumnWrittenAsStringReadsBackAsJsonText() throws Exception {
        String table = "jdbc_dt_json";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET allow_experimental_json_type = 1");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table + " (id Int8, j JSON) ENGINE = Memory");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "{\"key1\": \"value1\", \"key2\": 42}");
                ps.executeUpdate();
                ps.setInt(1, 2);
                ps.setString(2, "{\"key1\": \"value2\"}");
                ps.executeUpdate();
            }
            try (ResultSet rs = st.executeQuery(
                    "SELECT j FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                String row1 = rs.getString(1);
                assertTrue(row1.contains("\"key1\":\"value1\""), row1);
                assertTrue(row1.contains("\"key2\":42"), row1);
                assertTrue(rs.next());
                String row2 = rs.getString(1);
                assertTrue(row2.contains("\"key1\":\"value2\""), row2);
                assertFalse(row2.contains("key2"), "absent path must be omitted: " + row2);
                assertFalse(rs.next());
            }
        }
    }

    // ---- Dynamic (jdbc-v2 testDynamicTypesSimpleStatement) --------------------

    @Test
    void dynamicColumnPerRowTypedGetters() throws Exception {
        String table = "jdbc_dt_dynamic";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET allow_experimental_dynamic_type = 1");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table + " (id Int8, d Dynamic) ENGINE = Memory");
            st.execute("INSERT INTO " + table
                    + " VALUES (1, 'str7'), (2, 42), (3, 0.5)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT d FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("str7", rs.getString(1));
                assertInstanceOf(String.class, rs.getObject(1));
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertInstanceOf(Number.class, rs.getObject(1));
                assertTrue(rs.next());
                assertEquals(0.5, rs.getDouble(1), 0.0);
                assertFalse(rs.next());
            }
        }
    }

    // ---- Variant (jdbc-v2 testVariantTypesSimpleStatement) --------------------

    @Test
    void variantColumnTypedGetters() throws Exception {
        String table = "jdbc_dt_variant";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET allow_experimental_variant_type = 1");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, v Variant(String, Int32)) ENGINE = Memory");
            st.execute("INSERT INTO " + table + " VALUES (1, 'vstr'), (2, 42), (3, NULL)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT v FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("vstr", rs.getString(1));
                assertInstanceOf(String.class, rs.getObject(1));
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertInstanceOf(Number.class, rs.getObject(1));
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
        }
    }

    // ---- Geo reads (jdbc-v2 testGeoPoint/testGeoRing/testGeoLineString/
    // testGeoMultiLineString/testGeoPolygon/testGeoMultiPolygon/
    // testGeometricTypesSimpleStatement) ----------------------------------------
    // This client boxes Point as List[x, y] and the aggregate geo types as nested
    // Lists (jdbc-v2 uses primitive double[] nests); getArray materialises Object[]s.

    @Test
    void geoPointRingLineStringRead() throws Exception {
        String table = "jdbc_dt_geo_prl";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET allow_experimental_geo_types = 1");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, p Point, r Ring, l LineString) ENGINE = Memory");
            st.execute("INSERT INTO " + table + " VALUES (1, "
                    + "(1.5, 2.5), "
                    + "[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 0.0)], "
                    + "[(1.0, 2.0), (3.0, 4.0)])");
            try (ResultSet rs = st.executeQuery(
                    "SELECT p, r, l FROM " + table)) {
                assertEquals("Point", rs.getMetaData().getColumnTypeName(1));
                assertEquals("Ring", rs.getMetaData().getColumnTypeName(2));
                assertEquals("LineString", rs.getMetaData().getColumnTypeName(3));

                assertTrue(rs.next());
                assertEquals(List.of(1.5, 2.5), rs.getObject(1));
                assertEquals(List.of(
                                List.of(0.0, 0.0), List.of(10.0, 0.0),
                                List.of(10.0, 10.0), List.of(0.0, 0.0)),
                        rs.getObject(2));
                assertEquals(List.of(List.of(1.0, 2.0), List.of(3.0, 4.0)),
                        rs.getObject(3));

                // getArray works over the List-boxed geo values.
                assertArrayEquals(new Object[] {1.5, 2.5},
                        (Object[]) rs.getArray(1).getArray());
                Object[] line = (Object[]) rs.getArray(3).getArray();
                assertEquals(2, line.length);
                assertArrayEquals(new Object[] {1.0, 2.0}, (Object[]) line[0]);

                // getString renders ClickHouse-style literals for every geo shape
                // (reference: jdbc-v2/client-v2 DataTypeConverterTest#testGeoToString).
                assertEquals("[1.5, 2.5]", rs.getString(1));
                assertEquals("[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 0.0]]",
                        rs.getString(2));
                assertEquals("[[1.0, 2.0], [3.0, 4.0]]", rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void geoMultiLineStringPolygonMultiPolygonRead() throws Exception {
        String table = "jdbc_dt_geo_poly";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET allow_experimental_geo_types = 1");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, ml MultiLineString, poly Polygon, mp MultiPolygon)"
                    + " ENGINE = Memory");
            st.execute("INSERT INTO " + table + " VALUES (1, "
                    + "[[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]], "
                    + "[[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)],"
                    + " [(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)]], "
                    + "[[[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0)]],"
                    + " [[(10.0, 10.0), (10.0, 14.0), (14.0, 14.0)]]])");
            try (ResultSet rs = st.executeQuery(
                    "SELECT ml, poly, mp FROM " + table)) {
                assertEquals("MultiLineString", rs.getMetaData().getColumnTypeName(1));
                assertEquals("Polygon", rs.getMetaData().getColumnTypeName(2));
                assertEquals("MultiPolygon", rs.getMetaData().getColumnTypeName(3));

                assertTrue(rs.next());
                assertEquals(List.of(
                                List.of(List.of(1.0, 2.0), List.of(3.0, 4.0)),
                                List.of(List.of(5.0, 6.0), List.of(7.0, 8.0))),
                        rs.getObject(1));
                assertEquals(List.of(
                                List.of(List.of(0.0, 0.0), List.of(0.0, 4.0),
                                        List.of(4.0, 4.0), List.of(4.0, 0.0)),
                                List.of(List.of(1.0, 1.0), List.of(1.0, 2.0),
                                        List.of(2.0, 2.0), List.of(2.0, 1.0))),
                        rs.getObject(2));
                assertEquals(List.of(
                                List.of(List.of(List.of(0.0, 0.0), List.of(0.0, 4.0),
                                        List.of(4.0, 4.0))),
                                List.of(List.of(List.of(10.0, 10.0), List.of(10.0, 14.0),
                                        List.of(14.0, 14.0)))),
                        rs.getObject(3));

                // Nested materialisation through java.sql.Array.
                Object[] polygon = (Object[]) rs.getArray(2).getArray();
                assertEquals(2, polygon.length, "outer ring + hole");
                Object[] hole = (Object[]) polygon[1];
                assertArrayEquals(new Object[] {1.0, 1.0}, (Object[]) hole[0]);

                // Literal rendering of the deep-nested shapes.
                assertEquals("[[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]",
                        rs.getString(1));
                assertEquals("[[[0.0, 0.0], [0.0, 4.0], [4.0, 4.0], [4.0, 0.0]],"
                                + " [[1.0, 1.0], [1.0, 2.0], [2.0, 2.0], [2.0, 1.0]]]",
                        rs.getString(2));
                assertEquals("[[[[0.0, 0.0], [0.0, 4.0], [4.0, 4.0]]],"
                                + " [[[10.0, 10.0], [10.0, 14.0], [14.0, 14.0]]]]",
                        rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Geo values held inside Dynamic and Variant columns render through {@code getString}
     * by their RUNTIME shape — the same nested-list literal a dedicated geo column
     * produces (reference: client-v2 DataTypeConverterTest#testVariantOrDynamicGeoToString).
     */
    @Test
    void geoInsideDynamicAndVariantRendersViaGetString() throws Exception {
        String table = "jdbc_dt_geo_dyn";
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET allow_experimental_dynamic_type = 1");
            st.execute("SET allow_experimental_variant_type = 1");
            st.execute("SET allow_experimental_geo_types = 1");
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table
                    + " (id Int8, d Dynamic, v Variant(Point, String)) ENGINE = Memory");
            st.execute("INSERT INTO " + table + " VALUES"
                    + " (1, CAST((1.5, 2.5), 'Point'), (3.5, 4.5)),"
                    + " (2, CAST([(0.0, 0.0), (1.0, 1.0)], 'LineString'), 'plain')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT d, v FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("[1.5, 2.5]", rs.getString(1), "Point inside Dynamic");
                assertEquals("[3.5, 4.5]", rs.getString(2), "Point inside Variant");
                assertTrue(rs.next());
                assertEquals("[[0.0, 0.0], [1.0, 1.0]]", rs.getString(1),
                        "LineString inside Dynamic");
                assertEquals("plain", rs.getString(2), "String member renders as itself");
                assertFalse(rs.next());
            }
        }
    }

    // ---- Geo binding via PreparedStatement (jdbc-v2 testGeoGeometryPreparedStatement)
    // jdbc-v2 binds geo values through Connection.createStruct/createArrayOf; this
    // driver does not implement either factory, so the write path is unsupported.

    @Test
    void geoBindingFactoriesAreUnsupported() throws Exception {
        try (Connection conn = connect()) {
            assertThrows(SQLFeatureNotSupportedException.class,
                    () -> conn.createStruct("Tuple(Float64, Float64)",
                            new Object[] {1.0, 2.0}));
            assertThrows(SQLFeatureNotSupportedException.class,
                    () -> conn.createArrayOf("Array(Point)", new Object[0]));
        }
    }
}
