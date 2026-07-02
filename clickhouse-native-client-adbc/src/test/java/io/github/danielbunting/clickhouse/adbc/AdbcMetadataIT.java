package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Slice 5: connection metadata over ClickHouse system tables. */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcMetadataIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("getTableSchema maps system.columns to the same Arrow schema as the reader")
    void getTableSchemaMatchesReaderMapping(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_meta");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            try (AdbcStatement ddl = connection.createStatement()) {
                ddl.setSqlQuery("CREATE TABLE " + table + " ("
                        + "id Int64, name String, score Nullable(Float64), tags Array(String)) "
                        + "ENGINE = MergeTree ORDER BY id");
                ddl.executeUpdate();
            }

            Schema schema = connection.getTableSchema(null, "default", table);
            Schema expected = ClickHouseArrowTypes.schema(
                    List.of("id", "name", "score", "tags"),
                    List.of("Int64", "String", "Nullable(Float64)", "Array(String)"));
            assertEquals(expected, schema);
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("getTableSchema on a missing table raises NOT_FOUND")
    void getTableSchemaMissingTable(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            AdbcException ex = assertThrows(AdbcException.class,
                    () -> connection.getTableSchema(null, "default", "no_such_table_" + System.nanoTime()));
            assertEquals(AdbcStatusCode.NOT_FOUND, ex.getStatus());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("getTableTypes lists TABLE and VIEW")
    void getTableTypes(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            List<String> types = new ArrayList<>();
            try (ArrowReader reader = connection.getTableTypes()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    VarCharVector v = (VarCharVector) root.getVector("table_type");
                    for (int r = 0; r < root.getRowCount(); r++) {
                        types.add(new String(v.get(r), StandardCharsets.UTF_8));
                    }
                }
            }
            assertEquals(List.of("TABLE", "VIEW"), types);
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("getObjects lists the created table and its columns under its database")
    void getObjectsListsTableAndColumns(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_objects");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            try (AdbcStatement ddl = connection.createStatement()) {
                ddl.setSqlQuery("CREATE TABLE " + table
                        + " (id Int64, name String) ENGINE = MergeTree ORDER BY id");
                ddl.executeUpdate();
            }

            Map<String, List<String>> tableColumns = new HashMap<>();
            try (ArrowReader reader = connection.getObjects(
                    AdbcConnection.GetObjectsDepth.ALL, null, "default", table, null, null)) {
                collectObjects(reader, tableColumns);
            }

            assertTrue(tableColumns.containsKey(table), "created table should be listed");
            assertEquals(List.of("id", "name"), tableColumns.get(table));
        } finally {
            database.close();
        }
    }

    /** Walks the nested GET_OBJECTS structure, collecting table → column-name lists. */
    @SuppressWarnings("unchecked")
    private void collectObjects(ArrowReader reader, Map<String, List<String>> out) throws Exception {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
            org.apache.arrow.vector.complex.ListVector schemas =
                    (org.apache.arrow.vector.complex.ListVector) root.getVector("catalog_db_schemas");
            for (int c = 0; c < root.getRowCount(); c++) {
                List<?> schemaList = schemas.getObject(c);
                if (schemaList == null) {
                    continue;
                }
                for (Object schemaObj : schemaList) {
                    Map<String, ?> schema = (Map<String, ?>) schemaObj;
                    List<?> tables = (List<?>) schema.get("db_schema_tables");
                    if (tables == null) {
                        continue;
                    }
                    for (Object tableObj : tables) {
                        Map<String, ?> tableMap = (Map<String, ?>) tableObj;
                        String tableName = String.valueOf(tableMap.get("table_name"));
                        List<?> columns = (List<?>) tableMap.get("table_columns");
                        List<String> names = new ArrayList<>();
                        if (columns != null) {
                            for (Object columnObj : columns) {
                                names.add(String.valueOf(((Map<String, ?>) columnObj).get("column_name")));
                            }
                        }
                        out.put(tableName, names);
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("getInfo reports vendor and driver identity")
    void getInfo(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            Map<Long, String> info = new HashMap<>();
            try (ArrowReader reader = connection.getInfo()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    UInt4Vector names = (UInt4Vector) root.getVector("info_name");
                    DenseUnionVector values = (DenseUnionVector) root.getVector("info_value");
                    for (int r = 0; r < root.getRowCount(); r++) {
                        info.put((long) names.get(r), values.getObject(r).toString());
                    }
                }
            }
            assertEquals("ClickHouse", info.get((long) AdbcInfoCode.VENDOR_NAME.getValue()));
            assertEquals(ChAdbcDriver.DRIVER_NAME, info.get((long) AdbcInfoCode.DRIVER_NAME.getValue()));
            assertTrue(info.containsKey((long) AdbcInfoCode.VENDOR_VERSION.getValue()),
                    "vendor version should be present");
        } finally {
            database.close();
        }
    }
}
