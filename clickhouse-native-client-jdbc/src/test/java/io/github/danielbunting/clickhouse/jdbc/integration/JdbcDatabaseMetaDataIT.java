package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration coverage for the heavy {@link DatabaseMetaData} catalog-query methods
 * ({@link DatabaseMetaData#getTables}, {@link DatabaseMetaData#getColumns},
 * {@link DatabaseMetaData#getSchemas}, {@link DatabaseMetaData#getCatalogs},
 * {@link DatabaseMetaData#getTableTypes}, {@link DatabaseMetaData#getTypeInfo},
 * {@link DatabaseMetaData#getPrimaryKeys}). These are backed by {@code system.*} queries and
 * therefore require a live server; they had no assertions previously (the unit test at
 * {@code ChDatabaseMetaDataTest} explicitly excludes them). Mirrors the reference driver's
 * {@code jdbc-v2/metadata/DatabaseMetaDataTest} and old {@code ClickHouseDatabaseMetaDataTest}.
 */
@Tag("integration")
@Testcontainers
class JdbcDatabaseMetaDataIT {

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

    /** Column labels of a ResultSet, in order, upper-cased for JDBC-standard comparison. */
    private static List<String> columnLabels(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        List<String> labels = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            labels.add(md.getColumnLabel(i).toUpperCase());
        }
        return labels;
    }

    private static void createTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS md_probe");
            st.execute("CREATE TABLE md_probe (id UInt32, name Nullable(String), amount Decimal(10, 2)) "
                    + "ENGINE = MergeTree ORDER BY id");
        }
    }

    @Test
    void getTablesExposesStandardColumnsAndFindsCreatedTable() throws Exception {
        try (Connection conn = connect()) {
            createTable(conn);
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getTables("default", null, "md_probe", null)) {
                assertTrue(columnLabels(rs).containsAll(List.of(
                        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                        "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                        "REF_GENERATION")),
                        "getTables must expose the 10 JDBC-standard columns");
                boolean found = false;
                while (rs.next()) {
                    if ("md_probe".equals(rs.getString("TABLE_NAME"))) {
                        found = true;
                        assertEquals("default", rs.getString("TABLE_CAT"));
                    }
                }
                assertTrue(found, "the freshly created table must appear in getTables");
            }
        }
    }

    @Test
    void getColumnsReturnsColumnMetadataForCreatedTable() throws Exception {
        try (Connection conn = connect()) {
            createTable(conn);
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getColumns("default", null, "md_probe", null)) {
                assertTrue(columnLabels(rs).containsAll(List.of(
                        "TABLE_CAT", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "DECIMAL_DIGITS", "NULLABLE", "ORDINAL_POSITION",
                        "IS_NULLABLE")),
                        "getColumns must expose the JDBC-standard columns");
                Map<String, String> colToType = new LinkedHashMap<>();
                Map<String, Integer> colToJdbc = new LinkedHashMap<>();
                while (rs.next()) {
                    colToType.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
                    colToJdbc.put(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"));
                }
                assertEquals(List.of("id", "name", "amount"), new ArrayList<>(colToType.keySet()),
                        "columns returned in ordinal order");
                assertEquals("UInt32", colToType.get("id"));
                assertTrue(colToType.get("name").contains("String"));
                assertEquals(Types.DECIMAL, colToJdbc.get("amount").intValue(),
                        "Decimal(10,2) DATA_TYPE resolves to java.sql.Types.DECIMAL");
                assertEquals(Types.BIGINT, colToJdbc.get("id").intValue(),
                        "UInt32 maps to BIGINT (its range exceeds a signed 32-bit int)");
            }
        }
    }

    @Test
    void getSchemasAndCatalogsExposeSpecColumns() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getSchemas()) {
                assertTrue(columnLabels(rs).containsAll(List.of("TABLE_SCHEM", "TABLE_CATALOG")));
                // DEVIATION (reference: jdbc-v2 DatabaseMetaDataTest#testGetSchemas,
                // where the 'default' schema is listed): this driver models ClickHouse
                // databases as JDBC *catalogs* and has no schema layer, so getSchemas
                // is always empty — the databases appear in getCatalogs below instead.
                assertFalse(rs.next(), "no schema layer: getSchemas returns no rows");
            }
            try (ResultSet rs = md.getCatalogs()) {
                assertTrue(columnLabels(rs).contains("TABLE_CAT"));
                boolean sawDefault = false;
                while (rs.next()) {
                    if ("default".equals(rs.getString("TABLE_CAT"))) {
                        sawDefault = true;
                    }
                }
                assertTrue(sawDefault, "the 'default' database appears among catalogs");
            }
        }
    }

    @Test
    void getTableTypesListsTableAndView() throws Exception {
        try (Connection conn = connect()) {
            try (ResultSet rs = conn.getMetaData().getTableTypes()) {
                assertTrue(columnLabels(rs).contains("TABLE_TYPE"));
                List<String> types = new ArrayList<>();
                while (rs.next()) {
                    types.add(rs.getString("TABLE_TYPE"));
                }
                assertTrue(types.contains("TABLE"), "TABLE must be an advertised table type");
            }
        }
    }

    @Test
    void getTypeInfoReturnsNonEmptySpecShapedResultSet() throws Exception {
        try (Connection conn = connect()) {
            try (ResultSet rs = conn.getMetaData().getTypeInfo()) {
                assertTrue(columnLabels(rs).containsAll(List.of(
                        "TYPE_NAME", "DATA_TYPE", "PRECISION", "NULLABLE", "CASE_SENSITIVE",
                        "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT",
                        "MINIMUM_SCALE", "MAXIMUM_SCALE", "NUM_PREC_RADIX")),
                        "getTypeInfo must expose the JDBC-standard type-info columns");
                assertTrue(rs.next(), "getTypeInfo must advertise at least one type");
                assertNotNull(rs.getString("TYPE_NAME"));
            }
        }
    }

    @Test
    void getPrimaryKeysExposesSpecColumns() throws Exception {
        try (Connection conn = connect()) {
            createTable(conn);
            try (ResultSet rs = conn.getMetaData().getPrimaryKeys("default", null, "md_probe")) {
                assertTrue(columnLabels(rs).containsAll(List.of(
                        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ",
                        "PK_NAME")),
                        "getPrimaryKeys must expose the 6 JDBC-standard columns");
                // DEVIATION (reference: jdbc-v2 DatabaseMetaDataTest#testGetPrimaryKeys,
                // which reports the sorting key with KEY_SEQ ordering): ClickHouse has
                // no enforced primary-key constraint — the MergeTree ORDER BY is a
                // storage detail — so this driver deliberately reports none, even for
                // md_probe (ORDER BY id).
                assertFalse(rs.next(), "no enforced PKs are ever reported");
            }
        }
    }

    /**
     * getTables REMARKS carries the table comment (reference: jdbc-v1
     * ClickHouseDatabaseMetaDataTest#testTableComment).
     */
    @Test
    void getTablesRemarksCarriesTableComment() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS md_comment_probe");
                st.execute("CREATE TABLE md_comment_probe (s String) ENGINE = Memory "
                        + "COMMENT 'table comments'");
            }
            try (ResultSet rs = conn.getMetaData()
                    .getTables("default", null, "md_comment_probe", null)) {
                assertTrue(rs.next());
                assertEquals("table comments", rs.getString("REMARKS"));
                assertFalse(rs.next(), "exactly one match");
            }
        }
    }

    /**
     * getTables type filtering (reference: jdbc-v2 DatabaseMetaDataTest#testGetTables +
     * testTableTypes; jdbc-v1 testGetTables). This driver classifies only TABLE vs VIEW
     * (see {@link #getTablesClassifiesMemoryAndSystemTablesAsTableAndViewsAsView}), so
     * the filter operates on those two values; the reference's finer-grained types
     * (SYSTEM TABLE, MEMORY TABLE, ...) match nothing here.
     */
    @Test
    void getTablesTypeFilterRestrictsResults() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP VIEW IF EXISTS md_filter_v");
                st.execute("DROP TABLE IF EXISTS md_filter_t");
                st.execute("CREATE TABLE md_filter_t (v Int32) ENGINE = Memory");
                st.execute("CREATE VIEW md_filter_v AS SELECT v FROM md_filter_t");
            }
            DatabaseMetaData md = conn.getMetaData();

            try (ResultSet rs = md.getTables("default", null, "md_filter_%",
                    new String[] {"VIEW"})) {
                assertTrue(rs.next(), "the view matches the VIEW filter");
                assertEquals("md_filter_v", rs.getString("TABLE_NAME"));
                assertEquals("VIEW", rs.getString("TABLE_TYPE"));
                assertFalse(rs.next(), "the plain table is filtered out");
            }

            try (ResultSet rs = md.getTables("default", null, "md_filter_%",
                    new String[] {"TABLE"})) {
                assertTrue(rs.next(), "the table matches the TABLE filter");
                assertEquals("md_filter_t", rs.getString("TABLE_NAME"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                assertFalse(rs.next(), "the view is filtered out");
            }

            try (ResultSet rs = md.getTables("default", null, "md_filter_%",
                    new String[] {"TABLE", "VIEW"})) {
                List<String> names = new ArrayList<>();
                while (rs.next()) {
                    names.add(rs.getString("TABLE_NAME"));
                }
                // Spec ordering is TABLE_TYPE first, so TABLE rows precede VIEW rows.
                assertEquals(List.of("md_filter_t", "md_filter_v"), names);
            }

            // DEVIATION: the reference classifies system.numbers as 'SYSTEM TABLE';
            // here that type never matches anything.
            try (ResultSet rs = md.getTables("system", null, "numbers",
                    new String[] {"SYSTEM TABLE"})) {
                assertFalse(rs.next(), "SYSTEM TABLE is not a type this driver emits");
            }
        }
    }

    /**
     * getColumns per-type matrix (reference: jdbc-v2
     * DatabaseMetaDataTest#testGetColumnsWithTable and jdbc-v1 testGetColumns' size
     * matrix). Asserts DATA_TYPE, TYPE_NAME, ordinal, nullability, and DECIMAL_DIGITS
     * per column type, and pins this driver's deviations: COLUMN_SIZE is always 0 (the
     * reference derives byte/digit widths per type), NUM_PREC_RADIX is always 10, and
     * CHAR_OCTET_LENGTH is always NULL.
     */
    @Test
    void getColumnsPerTypeMatrix() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS md_col_matrix");
                st.execute("CREATE TABLE md_col_matrix ("
                        + "c1 Int64, c2 UInt128, c3 String, c4 Float32, c5 FixedString(10), "
                        + "c6 Decimal(10, 2), c7 Nullable(Decimal(5, 4)), c8 Date, "
                        + "c9 DateTime, c10 DateTime64(5), c11 Enum8('a' = 1, 'b' = 2)) "
                        + "ENGINE = MergeTree ORDER BY tuple()");
            }
            // column -> {TYPE_NAME, DATA_TYPE, nullable, DECIMAL_DIGITS (null = SQL NULL)}
            String[][] expected = {
                {"c1", "Int64"},
                {"c2", "UInt128"},
                {"c3", "String"},
                {"c4", "Float32"},
                {"c5", "FixedString(10)"},
                {"c6", "Decimal(10, 2)"},
                {"c7", "Nullable(Decimal(5, 4))"},
                {"c8", "Date"},
                {"c9", "DateTime"},
                {"c10", "DateTime64(5)"},
                {"c11", "Enum8('a' = 1, 'b' = 2)"},
            };
            // The c2 (UInt128) DATA_TYPE is not asserted here: the wide-int mapping
            // (NUMERIC, consistent across metadata surfaces) is covered by
            // wideIntDataTypeIsConsistentAcrossMetadataSurfaces.
            int[] expectedJdbc = {Types.BIGINT, Integer.MIN_VALUE, Types.VARCHAR, Types.REAL,
                    Types.VARCHAR, Types.DECIMAL, Types.DECIMAL, Types.DATE,
                    Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR};

            try (ResultSet rs = conn.getMetaData()
                    .getColumns("default", null, "md_col_matrix", null)) {
                for (int i = 0; i < expected.length; i++) {
                    assertTrue(rs.next(), "row for " + expected[i][0]);
                    assertEquals(expected[i][0], rs.getString("COLUMN_NAME"));
                    assertEquals(expected[i][1], rs.getString("TYPE_NAME"));
                    if (expectedJdbc[i] != Integer.MIN_VALUE) {
                        assertEquals(expectedJdbc[i], rs.getInt("DATA_TYPE"),
                                "DATA_TYPE for " + expected[i][1]);
                    }
                    assertEquals(i + 1, rs.getInt("ORDINAL_POSITION"));

                    boolean nullable = expected[i][1].startsWith("Nullable(");
                    assertEquals(nullable ? 1 : 0, rs.getInt("NULLABLE"));
                    assertEquals(nullable ? "YES" : "NO", rs.getString("IS_NULLABLE"));

                    // DEVIATION: COLUMN_SIZE is a constant 0, not the per-type width
                    // (reference reports e.g. 8 for Int64, 10 for FixedString(10)).
                    assertEquals(0, rs.getInt("COLUMN_SIZE"));
                    // DEVIATION: NUM_PREC_RADIX is a constant 10 (reference: 2 for
                    // ints, NULL for non-numerics).
                    assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
                    // DEVIATION: CHAR_OCTET_LENGTH is always NULL.
                    rs.getObject("CHAR_OCTET_LENGTH");
                    assertTrue(rs.wasNull(), "CHAR_OCTET_LENGTH is NULL for " + expected[i][0]);
                }
                assertFalse(rs.next(), "no extra columns");
            }

            // DECIMAL_DIGITS follows system.columns.numeric_scale for decimals.
            try (ResultSet rs = conn.getMetaData()
                    .getColumns("default", null, "md_col_matrix", "c6")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("DECIMAL_DIGITS"), "Decimal(10, 2) scale");
            }
            try (ResultSet rs = conn.getMetaData()
                    .getColumns("default", null, "md_col_matrix", "c7")) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt("DECIMAL_DIGITS"), "Nullable(Decimal(5, 4)) scale");
            }
        }
    }

    /**
     * Per-type getTypeInfo details (reference: jdbc-v2 DatabaseMetaDataTest#testGetTypeInfo
     * + testFindNestedTypes). Verifies the advertised inventory and the per-row precision /
     * signedness / radix values, and pins the deviations from the reference:
     * <ul>
     *   <li>NULLABLE is {@code typeNullable} for every type (any CH type can be wrapped in
     *       {@code Nullable}); the reference reports {@code typeNoNulls} for concrete types.
     *   <li>LITERAL_PREFIX/SUFFIX are always NULL (reference quotes string-like types).
     *   <li>Nested/composite types (Map, Tuple, Nested, ...) are not advertised at all
     *       (reference lists them).
     * </ul>
     */
    @Test
    void getTypeInfoPerTypeDetails() throws Exception {
        try (Connection conn = connect()) {
            Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
            try (ResultSet rs = conn.getMetaData().getTypeInfo()) {
                while (rs.next()) {
                    String name = rs.getString("TYPE_NAME");
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("DATA_TYPE", rs.getInt("DATA_TYPE"));
                    row.put("PRECISION", rs.getInt("PRECISION"));
                    row.put("UNSIGNED", rs.getBoolean("UNSIGNED_ATTRIBUTE"));
                    row.put("RADIX", rs.getInt("NUM_PREC_RADIX"));
                    byName.put(name, row);

                    // Global DATA_TYPE ordering is asserted in getTypeInfoIsSortedByDataType.

                    assertEquals(DatabaseMetaData.typeNullable, rs.getShort("NULLABLE"),
                            name + ": every CH type is Nullable-wrappable");
                    assertEquals(DatabaseMetaData.typeSearchable, rs.getShort("SEARCHABLE"), name);
                    assertFalse(rs.getBoolean("FIXED_PREC_SCALE"), name);
                    assertFalse(rs.getBoolean("AUTO_INCREMENT"), name);
                    assertEquals(0, rs.getShort("MINIMUM_SCALE"), name);
                    assertEquals(0, rs.getShort("MAXIMUM_SCALE"), name);
                    assertNull(rs.getString("LITERAL_PREFIX"), name);
                    assertNull(rs.getString("LITERAL_SUFFIX"), name);
                    assertNull(rs.getString("CREATE_PARAMS"), name);
                }
            }

            assertTrue(byName.keySet().containsAll(List.of(
                    "Int8", "UInt8", "Int16", "UInt16", "Int32", "UInt32", "Int64", "UInt64",
                    "Float32", "Float64", "Decimal", "String", "FixedString", "UUID",
                    "Date", "Date32", "DateTime", "DateTime64", "Enum8", "Enum16", "Array")),
                    "the major scalar families are advertised; saw " + byName.keySet());
            // DEVIATION: no nested/composite types in the inventory.
            for (String missing : List.of("Map", "Tuple", "Nested")) {
                assertFalse(byName.containsKey(missing),
                        missing + " is (deliberately) not advertised");
            }

            assertEquals(Types.TINYINT, byName.get("Int8").get("DATA_TYPE"));
            assertEquals(3, byName.get("Int8").get("PRECISION"));
            assertEquals(false, byName.get("Int8").get("UNSIGNED"));
            assertEquals(10, byName.get("Int8").get("RADIX"));

            assertEquals(Types.NUMERIC, byName.get("UInt64").get("DATA_TYPE"));
            assertEquals(20, byName.get("UInt64").get("PRECISION"));
            assertEquals(true, byName.get("UInt64").get("UNSIGNED"));

            assertEquals(Types.DECIMAL, byName.get("Decimal").get("DATA_TYPE"));
            assertEquals(76, byName.get("Decimal").get("PRECISION"),
                    "Decimal advertises its max precision (Decimal256)");

            assertEquals(Types.TIMESTAMP, byName.get("DateTime64").get("DATA_TYPE"));
            assertEquals(Types.ARRAY, byName.get("Array").get("DATA_TYPE"));
            assertEquals(Types.OTHER, byName.get("UUID").get("DATA_TYPE"));
        }
    }

    /**
     * {@link DatabaseMetaData#getTypeInfo()} is globally ordered by DATA_TYPE, per
     * the JDBC spec: the union chain is wrapped in a subselect so the ORDER BY
     * applies to the whole result rather than (as ClickHouse binds a bare ORDER BY
     * after UNION ALL) only the last SELECT (was knownBug 30).
     */
    @Test
    void getTypeInfoIsSortedByDataType() throws Exception {
        try (Connection conn = connect();
                ResultSet rs = conn.getMetaData().getTypeInfo()) {
            int previous = Integer.MIN_VALUE;
            String previousName = "(start)";
            while (rs.next()) {
                int dataType = rs.getInt("DATA_TYPE");
                String name = rs.getString("TYPE_NAME");
                assertTrue(dataType >= previous,
                        "getTypeInfo must be ordered by DATA_TYPE, but " + name + " ("
                                + dataType + ") follows " + previousName + " ("
                                + previous + ")");
                previous = dataType;
                previousName = name;
            }
        }
    }

    /**
     * The two metadata surfaces agree on the JDBC type of a wide-integer column:
     * {@code getColumns().DATA_TYPE} for an {@code Int128}/{@code UInt256} column
     * equals {@code ResultSetMetaData.getColumnType}, namely {@link Types#NUMERIC} —
     * the {@code multiIf} in {@code ChDatabaseMetaData.getColumns} is aligned with
     * {@code JdbcValues.sqlType} (was knownBug 31; the jdbc-v2 mapping: the value
     * range demands an unscaled BigInteger-backed NUMERIC, not DECIMAL).
     */
    @Test
    void wideIntDataTypeIsConsistentAcrossMetadataSurfaces() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS md_wide_int");
                st.execute("CREATE TABLE md_wide_int (w Int128, u UInt256) ENGINE = Memory");
            }
            int rsmdW;
            int rsmdU;
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT w, u FROM md_wide_int WHERE 1 = 0")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                rsmdW = rsmd.getColumnType(1);
                rsmdU = rsmd.getColumnType(2);
            }
            assertEquals(Types.NUMERIC, rsmdW, "Int128 result metadata type");
            assertEquals(Types.NUMERIC, rsmdU, "UInt256 result metadata type");
            try (ResultSet rs = conn.getMetaData()
                    .getColumns("default", null, "md_wide_int", null)) {
                assertTrue(rs.next());
                assertEquals("w", rs.getString("COLUMN_NAME"));
                assertEquals(rsmdW, rs.getInt("DATA_TYPE"),
                        "Int128: getColumns().DATA_TYPE must match ResultSetMetaData.getColumnType");
                assertTrue(rs.next());
                assertEquals("u", rs.getString("COLUMN_NAME"));
                assertEquals(rsmdU, rs.getInt("DATA_TYPE"),
                        "UInt256: getColumns().DATA_TYPE must match ResultSetMetaData.getColumnType");
                assertFalse(rs.next());
            }
        }
    }

    /**
     * TABLE_TYPE classification (reference: jdbc-v2
     * DatabaseMetaDataTest#testGetTablesReturnKnownTableTypes). DEVIATION: this driver
     * classifies only {@code TABLE} vs {@code VIEW} (engines matching {@code %View});
     * it does NOT report the reference's finer-grained {@code SYSTEM TABLE} /
     * {@code MEMORY TABLE} types, so both a Memory-engine table and a system table are
     * plain {@code TABLE}.
     */
    @Test
    void getTablesClassifiesMemoryAndSystemTablesAsTableAndViewsAsView() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS md_type_memory");
                st.execute("CREATE TABLE md_type_memory (v Int32) ENGINE = Memory");
                st.execute("DROP VIEW IF EXISTS md_type_view");
                st.execute("CREATE VIEW md_type_view AS SELECT v FROM md_type_memory");
            }
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getTables("default", null, "md_type_memory", null)) {
                assertTrue(rs.next(), "the Memory-engine table must be listed");
                assertEquals("TABLE", rs.getString("TABLE_TYPE"),
                        "Memory engine classifies as plain TABLE (no MEMORY TABLE type)");
            }
            try (ResultSet rs = md.getTables("system", null, "numbers", null)) {
                assertTrue(rs.next(), "system.numbers must be listed");
                assertEquals("TABLE", rs.getString("TABLE_TYPE"),
                        "system tables classify as plain TABLE (no SYSTEM TABLE type)");
                assertEquals("numbers", rs.getString("TABLE_NAME"));
            }
            try (ResultSet rs = md.getTables("default", null, "md_type_view", null)) {
                assertTrue(rs.next(), "the view must be listed");
                assertEquals("VIEW", rs.getString("TABLE_TYPE"),
                        "View engines classify as VIEW");
            }
        }
    }

    /**
     * Empty schema-pattern semantics (reference: jdbc-v2
     * DatabaseMetaDataTest#testGetColumnsWithEmptySchema, which returns no rows).
     * DEVIATION: this driver maps ClickHouse databases to JDBC <em>catalogs</em> and
     * ignores {@code schemaPattern} entirely (ClickHouse has no schema layer), so an
     * empty schema pattern does not filter anything and the columns are still returned.
     */
    @Test
    void getColumnsIgnoresEmptySchemaPattern() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getColumns("system", "", "numbers", null)) {
                assertTrue(rs.next(),
                        "schemaPattern is ignored, so system.numbers columns are returned");
                assertEquals("system", rs.getString("TABLE_CAT"));
                assertEquals("number", rs.getString("COLUMN_NAME"));
            }
        }
    }

    /**
     * Server version metadata matches the live server's {@code version()} (reference:
     * jdbc-v2 DatabaseMetaDataTest#testGetServerVersions / testGetDatabaseMajorVersion /
     * testGetDatabaseMinorVersion). ({@code getUserName()} is asserted separately by
     * {@link #getUserNameReturnsAuthenticatedUser()}.)
     */
    @Test
    void serverVersionMetadataMatchesLiveServer() throws Exception {
        try (Connection conn = connect()) {
            String serverVersion;
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT version()")) {
                assertTrue(rs.next());
                serverVersion = rs.getString(1);
            }
            DatabaseMetaData md = conn.getMetaData();
            assertEquals(serverVersion, md.getDatabaseProductVersion(),
                    "getDatabaseProductVersion must be the server's version() string");
            String[] parts = serverVersion.split("\\.");
            assertEquals(Integer.parseInt(parts[0]), md.getDatabaseMajorVersion(),
                    "major version (ClickHouse's calendar year)");
            assertEquals(Integer.parseInt(parts[1]), md.getDatabaseMinorVersion(),
                    "minor version");
            assertTrue(md.getDatabaseMajorVersion() >= 21,
                    "ClickHouse major versions are years, >= 21");
            assertTrue(md.getDatabaseMinorVersion() > 0, "minor version is always > 0");
        }
    }

    /**
     * {@code DatabaseMetaData.getUserName()} returns the user the session
     * authenticated as: it queries {@code SELECT currentUser()}, so the answer is
     * right regardless of how the credentials were supplied (was knownBug 29; JDBC
     * spec, jdbc-v2 {@code DatabaseMetaDataTest#testGetUserName}).
     */
    @Test
    void getUserNameReturnsAuthenticatedUser() throws Exception {
        try (Connection conn = connect()) {
            assertEquals("default", conn.getMetaData().getUserName(),
                    "getUserName must report the authenticated user");
        }
    }

    /**
     * The schema/catalog terms are fixed (reference: jdbc-v2 getSchemaTerm; its
     * configurable {@code schema_term} property is N/A for this driver).
     */
    @Test
    void schemaAndCatalogTermsAreFixed() throws Exception {
        try (Connection conn = connect()) {
            DatabaseMetaData md = conn.getMetaData();
            assertEquals("schema", md.getSchemaTerm());
            assertEquals("database", md.getCatalogTerm(),
                    "a ClickHouse database is modelled as the JDBC catalog");
        }
    }

    /**
     * {@code DatabaseMetaData.getUserName()} succeeds even while another
     * {@link ResultSet} on the same connection is mid-stream (tools routinely
     * interleave metadata calls with an open cursor): while a streaming result holds
     * the {@code ConnectionGuard}, the method answers from the connection's
     * configured user instead of probing the busy shared connection. (was knownBug 43)
     */
    @Test
    void getUserNameSucceedsWhileAnotherResultSetIsStreaming() throws Exception {
        try (Connection conn = connect();
                Statement st = conn.createStatement();
                // A lazy multi-block scan: the core QueryResult holds the
                // ConnectionGuard until the stream is fully drained or closed, so after
                // reading only the first row the connection is genuinely busy. (Kept to
                // 10M rows so ResultSet.close()'s drain stays quick.)
                ResultSet rs = st.executeQuery(
                        "SELECT number FROM system.numbers LIMIT 10000000")) {
            assertTrue(rs.next(), "the streaming result must be mid-stream");
            assertEquals(0L, rs.getLong(1));
            assertEquals("default", conn.getMetaData().getUserName(),
                    "getUserName must not need the busy shared connection while a "
                    + "result set is streaming");
        }
    }
}
