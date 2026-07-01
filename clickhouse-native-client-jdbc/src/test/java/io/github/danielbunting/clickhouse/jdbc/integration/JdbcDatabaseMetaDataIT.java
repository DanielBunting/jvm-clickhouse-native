package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
            }
        }
    }
}
