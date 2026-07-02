package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@code getObjects} honours the requested {@link GetObjectsDepth}: each level populates the
 * nested GET_OBJECTS structure exactly down to the requested depth and leaves deeper levels null.
 * {@link AdbcMetadataIT} covers the {@code ALL} depth; this pins CATALOGS / DB_SCHEMAS / TABLES.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcGetObjectsDepthIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("CATALOGS depth lists the catalog but leaves db_schemas null")
    void catalogsDepthStopsAtCatalog(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            boolean sawCatalog = false;
            try (ArrowReader reader =
                    connection.getObjects(GetObjectsDepth.CATALOGS, null, null, null, null, null)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                ListVector schemas = (ListVector) root.getVector("catalog_db_schemas");
                while (reader.loadNextBatch()) {
                    for (int c = 0; c < root.getRowCount(); c++) {
                        sawCatalog = true;
                        assertTrue(schemas.isNull(c), "CATALOGS depth must leave catalog_db_schemas null");
                    }
                }
            }
            assertTrue(sawCatalog, "the single (unnamed) catalog row should be present");
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("DB_SCHEMAS depth lists the database but leaves its tables null")
    void dbSchemasDepthStopsAtSchema(BufferAllocator allocator) throws Exception {
        String db = "objdepth_schemas_" + System.nanoTime();
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE DATABASE `" + db + "`");
            try {
                try (ArrowReader reader =
                        connection.getObjects(GetObjectsDepth.DB_SCHEMAS, null, db, null, null, null)) {
                    Map<String, ?> schema = findSchema(reader, db);
                    assertNotNull(schema, "the created database should be listed at DB_SCHEMAS depth");
                    assertNull(schema.get("db_schema_tables"), "DB_SCHEMAS depth must leave db_schema_tables null");
                }
            } finally {
                execDdl(connection, "DROP DATABASE IF EXISTS `" + db + "`");
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("TABLES depth lists the table but leaves its columns null")
    void tablesDepthStopsAtTable(BufferAllocator allocator) throws Exception {
        String db = "objdepth_tables_" + System.nanoTime();
        String table = "t";
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE DATABASE `" + db + "`");
            try {
                execDdl(connection, "CREATE TABLE `" + db + "`.`" + table
                        + "` (id Int64, name String) ENGINE = MergeTree ORDER BY id");

                try (ArrowReader reader =
                        connection.getObjects(GetObjectsDepth.TABLES, null, db, table, null, null)) {
                    Map<String, ?> schema = findSchema(reader, db);
                    assertNotNull(schema, "the database should be listed");
                    List<?> tables = (List<?>) schema.get("db_schema_tables");
                    assertNotNull(tables, "TABLES depth must populate db_schema_tables");
                    Map<String, ?> tableMap = findTable(tables, table);
                    assertNotNull(tableMap, "the created table should be listed at TABLES depth");
                    assertEquals("TABLE", String.valueOf(tableMap.get("table_type")));
                    assertNull(tableMap.get("table_columns"), "TABLES depth must leave table_columns null");
                }
            } finally {
                execDdl(connection, "DROP DATABASE IF EXISTS `" + db + "`");
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers ---------------------------------------------------------------------------

    /** Returns the db-schema struct (as a Map) whose {@code db_schema_name} equals {@code db}, or null. */
    @SuppressWarnings("unchecked")
    private static Map<String, ?> findSchema(ArrowReader reader, String db) throws Exception {
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
                    if (db.equals(String.valueOf(schema.get("db_schema_name")))) {
                        return schema;
                    }
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> findTable(List<?> tables, String table) {
        for (Object tableObj : tables) {
            Map<String, ?> tableMap = (Map<String, ?>) tableObj;
            if (table.equals(String.valueOf(tableMap.get("table_name")))) {
                return tableMap;
            }
        }
        return null;
    }

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }
}
