package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ScriptedConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Offline unit coverage for the {@code getObjects} catalog logic ({@link GetObjectsBuilder},
 * exercised through {@link ChAdbcConnection#getObjects} with scripted {@code system.*} results):
 * depth truncation, LIKE-pattern forwarding and escaping, table-type filtering and engine
 * classification. The ADBC analogue of the JDBC module's {@code JdbcDatabaseMetaDataIT} catalog
 * queries — testable without a server because the builder consumes plain SQL results.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class GetObjectsBuilderTest {

    /** A core scripted with two databases, three tables (one a view), and columns for db1.t1/t2. */
    private static ScriptedConnection scriptedCatalog() {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.databases", SystemTableBlocks.databases("db1", "db2"));
        core.respondTo("system.tables", SystemTableBlocks.tables(
                new String[] {"db1", "t1", "MergeTree"},
                new String[] {"db1", "v1", "View"},
                new String[] {"db2", "t2", "ReplacingMergeTree"}));
        core.respondTo("system.columns", SystemTableBlocks.columns(
                new Object[] {"db1", "t1", "id", "Int64", 1},
                new Object[] {"db1", "t1", "name", "String", 2},
                new Object[] {"db2", "t2", "value", "Float64", 1}));
        return core;
    }

    // ---- depth truncation ----------------------------------------------------------------------

    @Test
    @DisplayName("CATALOGS depth reports the single unnamed catalog and issues no queries")
    void catalogsDepthStopsAtCatalog(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.CATALOGS, null, null, null, null, null)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            assertEquals(1, root.getRowCount());
            assertEquals("", String.valueOf(((VarCharVector) root.getVector("catalog_name")).getObject(0)));
            assertTrue(((ListVector) root.getVector("catalog_db_schemas")).isNull(0),
                    "CATALOGS depth must leave catalog_db_schemas null");
            assertTrue(core.queried.isEmpty(), "catalog-only depth needs no system queries");
        }
    }

    @Test
    @DisplayName("DB_SCHEMAS depth lists databases with null table lists, querying only system.databases")
    void dbSchemasDepthStopsAtSchemas(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
            List<Map<String, ?>> schemas = readSchemas(reader);
            assertEquals(List.of("db1", "db2"), schemaNames(schemas));
            for (Map<String, ?> schema : schemas) {
                assertNull(schema.get("db_schema_tables"), "DB_SCHEMAS depth must leave tables null");
            }
            assertEquals(1, core.queried.size(), "only system.databases may be queried");
            assertTrue(core.queried.get(0).contains("system.databases"));
        }
    }

    @Test
    @DisplayName("TABLES depth lists tables with null column lists")
    void tablesDepthStopsAtTables(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.TABLES, null, null, null, null, null)) {
            List<Map<String, ?>> schemas = readSchemas(reader);
            Map<String, ?> table = findTable(findSchema(schemas, "db1"), "t1");
            assertNotNull(table);
            assertEquals("TABLE", String.valueOf(table.get("table_type")));
            assertNull(table.get("table_columns"), "TABLES depth must leave columns null");
            assertEquals(2, core.queried.size(), "system.columns must not be queried at TABLES depth");
        }
    }

    @Test
    @DisplayName("ALL depth populates columns with their ordinal positions, attached to the right table")
    void allDepthPopulatesColumns(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.ALL, null, null, null, null, null)) {
            List<Map<String, ?>> schemas = readSchemas(reader);

            List<?> t1Columns = (List<?>) findTable(findSchema(schemas, "db1"), "t1").get("table_columns");
            assertNotNull(t1Columns, "ALL depth must populate table_columns");
            assertEquals(List.of("id", "name"), columnNames(t1Columns));
            assertEquals(1, ((Map<?, ?>) t1Columns.get(0)).get("ordinal_position"));
            assertEquals(2, ((Map<?, ?>) t1Columns.get(1)).get("ordinal_position"));

            List<?> t2Columns = (List<?>) findTable(findSchema(schemas, "db2"), "t2").get("table_columns");
            assertEquals(List.of("value"), columnNames(t2Columns), "columns must attach to their own table");
        }
    }

    // ---- pattern forwarding & escaping ----------------------------------------------------------

    @Test
    @DisplayName("the db-schema pattern is forwarded into every system-table LIKE clause")
    void dbSchemaPatternForwarded(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.ALL, null, "foo%", null, null, null)) {
            reader.loadNextBatch();
            for (String sql : core.queried) {
                assertTrue(sql.contains("LIKE 'foo%'"), sql);
            }
        }
    }

    @Test
    @DisplayName("single quotes in patterns are SQL-escaped (no injection through metadata filters)")
    void patternQuotesAreEscaped(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(
                        GetObjectsDepth.ALL, null, "fo'o", "ta'ble", null, "co'l")) {
            reader.loadNextBatch();
            String columnsSql = core.queried.get(2);
            assertTrue(columnsSql.contains("fo''o"), columnsSql);
            assertTrue(columnsSql.contains("ta''ble"), columnsSql);
            assertTrue(columnsSql.contains("co''l"), columnsSql);
        }
    }

    @Test
    @DisplayName("the column pattern is forwarded to the system.columns query only")
    void columnPatternForwarded(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.ALL, null, null, null, null, "id%")) {
            reader.loadNextBatch();
            assertTrue(core.queried.get(2).contains("name LIKE 'id%'"), core.queried.get(2));
        }
    }

    // ---- table-type filtering & engine classification --------------------------------------------

    @Test
    @DisplayName("a TABLE-only type filter drops views")
    void tableTypeFilterKeepsTablesOnly(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(
                        GetObjectsDepth.TABLES, null, null, null, new String[] {"TABLE"}, null)) {
            List<Map<String, ?>> schemas = readSchemas(reader);
            Map<String, ?> db1 = findSchema(schemas, "db1");
            assertNotNull(findTable(db1, "t1"));
            assertNull(findTable(db1, "v1"), "the View-engined table must be filtered out");
        }
    }

    @Test
    @DisplayName("a VIEW-only type filter drops plain tables")
    void tableTypeFilterKeepsViewsOnly(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = scriptedCatalog();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(
                        GetObjectsDepth.TABLES, null, null, null, new String[] {"VIEW"}, null)) {
            List<Map<String, ?>> schemas = readSchemas(reader);
            Map<String, ?> db1 = findSchema(schemas, "db1");
            assertNull(findTable(db1, "t1"));
            Map<String, ?> view = findTable(db1, "v1");
            assertNotNull(view);
            assertEquals("VIEW", String.valueOf(view.get("table_type")));
        }
    }

    @Test
    @DisplayName("a MaterializedView engine classifies as VIEW")
    void materializedViewClassifiesAsView(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.databases", SystemTableBlocks.databases("db1"));
        core.respondTo("system.tables", SystemTableBlocks.tables(
                new String[] {"db1", "mv", "MaterializedView"}));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.TABLES, null, null, null, null, null)) {
            Map<String, ?> table = findTable(findSchema(readSchemas(reader), "db1"), "mv");
            assertEquals("VIEW", String.valueOf(table.get("table_type")));
        }
    }

    // ---- catalog pattern ------------------------------------------------------------------------

    @Test
    @DisplayName("a catalog pattern that cannot match the unnamed catalog yields zero rows and no queries")
    void catalogPatternMismatchYieldsNoRows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.ALL, "nope", null, null, null, null)) {
            reader.loadNextBatch();
            assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
            assertTrue(core.queried.isEmpty());
        }
    }

    @Test
    @DisplayName("'%' matches the unnamed catalog; '_' (exactly one char) does not")
    void catalogPatternLikeSemantics(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.databases", SystemTableBlocks.databases("db1"));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            try (ArrowReader reader =
                    connection.getObjects(GetObjectsDepth.DB_SCHEMAS, "%", null, null, null, null)) {
                reader.loadNextBatch();
                assertEquals(1, reader.getVectorSchemaRoot().getRowCount(), "'%' must match ''");
            }
            try (ArrowReader reader =
                    connection.getObjects(GetObjectsDepth.DB_SCHEMAS, "_", null, null, null, null)) {
                reader.loadNextBatch();
                assertEquals(0, reader.getVectorSchemaRoot().getRowCount(), "'_' must not match ''");
            }
        }
    }

    @Test
    @DisplayName("an empty server still reports the catalog with an empty schema list")
    void emptyServerListsCatalogWithNoSchemas(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.databases", SystemTableBlocks.databases());
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                ArrowReader reader = connection.getObjects(GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
            List<Map<String, ?>> schemas = readSchemas(reader);
            assertTrue(schemas.isEmpty(), "no databases -> empty (not null) schema list");
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Loads the single GET_OBJECTS batch and returns the catalog's db-schema structs as maps. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, ?>> readSchemas(ArrowReader reader) throws Exception {
        List<Map<String, ?>> schemas = new ArrayList<>();
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
            ListVector schemasVector = (ListVector) root.getVector("catalog_db_schemas");
            for (int c = 0; c < root.getRowCount(); c++) {
                List<?> schemaList = (List<?>) schemasVector.getObject(c);
                if (schemaList == null) {
                    continue;
                }
                for (Object schema : schemaList) {
                    schemas.add((Map<String, ?>) schema);
                }
            }
        }
        return schemas;
    }

    private static List<String> schemaNames(List<Map<String, ?>> schemas) {
        return schemas.stream().map(s -> String.valueOf(s.get("db_schema_name"))).toList();
    }

    private static Map<String, ?> findSchema(List<Map<String, ?>> schemas, String name) {
        return schemas.stream()
                .filter(s -> name.equals(String.valueOf(s.get("db_schema_name"))))
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> findTable(Map<String, ?> schema, String name) {
        assertNotNull(schema, "schema not found");
        List<?> tables = (List<?>) schema.get("db_schema_tables");
        if (tables == null) {
            return null;
        }
        return tables.stream()
                .map(t -> (Map<String, ?>) t)
                .filter(t -> name.equals(String.valueOf(t.get("table_name"))))
                .findFirst()
                .orElse(null);
    }

    private static List<String> columnNames(List<?> columns) {
        return columns.stream()
                .map(c -> String.valueOf(((Map<?, ?>) c).get("column_name")))
                .toList();
    }
}
