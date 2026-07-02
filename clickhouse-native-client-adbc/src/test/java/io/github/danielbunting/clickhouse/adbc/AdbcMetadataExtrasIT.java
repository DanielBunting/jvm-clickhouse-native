package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Live catalog-metadata coverage beyond {@code AdbcMetadataIT}/{@code AdbcGetObjectsDepthIT},
 * ported from the JDBC module's {@code JdbcDatabaseMetaDataIT} catalog queries: LIKE pattern
 * filtering against real system tables, view classification, exotic identifiers, and column
 * order/type fidelity through {@code getTableSchema}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcMetadataExtrasIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("table-name LIKE patterns filter getObjects against the live catalog")
    void tablePatternFiltersLiveCatalog(BufferAllocator allocator) throws Exception {
        String db = "objx_pat_" + System.nanoTime();
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE DATABASE `" + db + "`");
            try {
                execDdl(conn, "CREATE TABLE `" + db + "`.orders_live (id Int64) ENGINE = Memory");
                execDdl(conn, "CREATE TABLE `" + db + "`.orders_hist (id Int64) ENGINE = Memory");
                execDdl(conn, "CREATE TABLE `" + db + "`.users (id Int64) ENGINE = Memory");

                assertEquals(List.of("orders_hist", "orders_live"),
                        tableNames(conn, db, "orders%"), "% must match the orders pair only");
                assertEquals(List.of("users"), tableNames(conn, db, "user_"),
                        "_ must match exactly one character");
                assertEquals(List.of("orders_hist", "orders_live", "users"),
                        tableNames(conn, db, null), "no pattern lists everything");
            } finally {
                execDdl(conn, "DROP DATABASE IF EXISTS `" + db + "`");
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a live CREATE VIEW classifies as VIEW; its source table stays TABLE")
    void viewClassificationLive(BufferAllocator allocator) throws Exception {
        String db = "objx_view_" + System.nanoTime();
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE DATABASE `" + db + "`");
            try {
                execDdl(conn, "CREATE TABLE `" + db + "`.src (id Int64) ENGINE = Memory");
                execDdl(conn, "CREATE VIEW `" + db + "`.v AS SELECT id FROM `" + db + "`.src");

                Map<String, String> types = tableTypes(conn, db);
                assertEquals("TABLE", types.get("src"));
                assertEquals("VIEW", types.get("v"));

                assertEquals(List.of("v"), new ArrayList<>(
                        tableTypes(conn, db, new String[] {"VIEW"}).keySet()),
                        "a VIEW-only filter must drop the source table");
            } finally {
                execDdl(conn, "DROP DATABASE IF EXISTS `" + db + "`");
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("exotic table and column identifiers survive getTableSchema quoting")
    void exoticIdentifiersInGetTableSchema(BufferAllocator allocator) throws Exception {
        String table = "we`ird ta'ble-" + System.nanoTime();
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE `" + table.replace("`", "``") + "` "
                    + "(`col with space` Int64, `col'quote` String) ENGINE = Memory");
            try {
                Schema schema = conn.getTableSchema(null, "default", table);
                assertEquals(List.of("col with space", "col'quote"),
                        schema.getFields().stream().map(f -> f.getName()).toList());
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS `" + table.replace("`", "``") + "`");
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("getTableSchema preserves declaration order and full type fidelity via metadata")
    void getTableSchemaColumnOrderAndTypes(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("objx_fidelity");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (z Nullable(String), "
                    + "a LowCardinality(String), m Map(String, Array(Int32)), "
                    + "t DateTime64(3, 'UTC')) ENGINE = Memory");
            try {
                Schema schema = conn.getTableSchema(null, null, table);
                assertEquals(List.of("z", "a", "m", "t"),
                        schema.getFields().stream().map(f -> f.getName()).toList(),
                        "declaration order, not alphabetical");
                assertEquals("Nullable(String)", chType(schema, 0));
                assertEquals("LowCardinality(String)", chType(schema, 1));
                assertEquals("Map(String, Array(Int32))", chType(schema, 2));
                assertEquals("DateTime64(3, 'UTC')", chType(schema, 3));
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static String chType(Schema schema, int index) {
        String type = schema.getFields().get(index).getMetadata().get("clickhouse.type");
        assertNotNull(type, "every mapped field must carry its source type");
        return type;
    }

    private static List<String> tableNames(AdbcConnection conn, String db, String pattern)
            throws Exception {
        return new ArrayList<>(tableTypes(conn, db, null, pattern).keySet());
    }

    private static Map<String, String> tableTypes(AdbcConnection conn, String db) throws Exception {
        return tableTypes(conn, db, null, null);
    }

    private static Map<String, String> tableTypes(AdbcConnection conn, String db, String[] typeFilter)
            throws Exception {
        return tableTypes(conn, db, typeFilter, null);
    }

    /** table name → table type for {@code db}, via getObjects at TABLES depth. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> tableTypes(
            AdbcConnection conn, String db, String[] typeFilter, String tablePattern)
            throws Exception {
        Map<String, String> out = new java.util.TreeMap<>();
        try (ArrowReader reader = conn.getObjects(
                GetObjectsDepth.TABLES, null, db, tablePattern, typeFilter, null)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ListVector schemas = (ListVector) root.getVector("catalog_db_schemas");
            while (reader.loadNextBatch()) {
                for (int c = 0; c < root.getRowCount(); c++) {
                    List<?> schemaList = (List<?>) schemas.getObject(c);
                    if (schemaList == null) {
                        continue;
                    }
                    for (Object schemaObj : schemaList) {
                        Map<String, ?> schema = (Map<String, ?>) schemaObj;
                        if (!db.equals(String.valueOf(schema.get("db_schema_name")))) {
                            continue;
                        }
                        List<?> tables = (List<?>) schema.get("db_schema_tables");
                        if (tables == null) {
                            continue;
                        }
                        for (Object tableObj : tables) {
                            Map<String, ?> table = (Map<String, ?>) tableObj;
                            out.put(String.valueOf(table.get("table_name")),
                                    String.valueOf(table.get("table_type")));
                        }
                    }
                }
            }
        }
        assertTrue(out != null);
        return out;
    }

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }
}
