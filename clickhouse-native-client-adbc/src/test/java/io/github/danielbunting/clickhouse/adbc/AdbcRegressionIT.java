package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.BlockMaterializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Regression tests for the code-review findings. Each fails on the pre-fix code and passes once
 * the corresponding fix lands (TDD red→green).
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcRegressionIT extends AdbcRoundTripBase {

    // ---- #1: IPv6 read crashes on IPv4-mapped addresses ------------------------------------

    @Test
    @DisplayName("#1 IPv6 read handles an IPv4-mapped address (toIPv6('1.2.3.4'))")
    void ipv6Ipv4MappedRoundTrips(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toIPv6('1.2.3.4') AS ip");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertTrue(reader.loadNextBatch());
                byte[] bytes = ((FixedSizeBinaryVector) root.getVector("ip")).get(0);
                byte[] expected = new byte[16];
                expected[10] = (byte) 0xff;
                expected[11] = (byte) 0xff;
                expected[12] = 1;
                expected[13] = 2;
                expected[14] = 3;
                expected[15] = 4;
                assertArrayEquals(expected, bytes, "::ffff:1.2.3.4");
            }
        } finally {
            database.close();
        }
    }

    // ---- #2: null into a non-nullable column on ingest -------------------------------------

    @Test
    @DisplayName("#2 ingesting null into a non-nullable column fails with a clear error")
    void nullIntoNonNullableColumnFailsClearly(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("reg_nn");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE TABLE " + table + " (id Int64) ENGINE = MergeTree ORDER BY id");

            try (VectorSchemaRoot root =
                    VectorSchemaRoot.create(ClickHouseArrowTypes.schema(List.of("id"), List.of("Int64")), allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                id.setSafe(0, 1);
                id.setNull(1); // physical null in a non-nullable column
                id.setValueCount(2);
                root.setRowCount(2);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    AdbcException ex = assertThrows(AdbcException.class, ingest::executeUpdate);
                    assertTrue(
                            ex.getMessage() != null && ex.getMessage().contains("non-nullable column"),
                            "expected a clear null-not-allowed message, got: " + ex.getMessage());
                }
            }
        } finally {
            database.close();
        }
    }

    // ---- #4: FixedSizeBinary ingest into a FixedString column -------------------------------

    @Test
    @DisplayName("#4 a FixedSizeBinary column ingests into FixedString without ClassCastException")
    void fixedStringIngestRoundTrips(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("reg_fs");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE TABLE " + table + " (f FixedString(4)) ENGINE = MergeTree ORDER BY f");

            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("f"), List.of("FixedString(4)")), allocator)) {
                FixedSizeBinaryVector f = (FixedSizeBinaryVector) root.getVector("f");
                f.setSafe(0, new byte[] {'a', 'b', 0, 0});
                f.setValueCount(1);
                root.setRowCount(1);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertEquals(1, ingest.executeUpdate().getAffectedRows());
                }
            }
        } finally {
            database.close();
        }

        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
            assertEquals("ab", firstString(core, "SELECT f FROM " + table));
        }
    }

    // ---- #5: unsupported types surface as AdbcException(NOT_IMPLEMENTED) --------------------

    @Test
    @DisplayName("#5 querying an unsupported type raises AdbcException(NOT_IMPLEMENTED), not a raw RuntimeException")
    void unsupportedTypeRaisesAdbcException(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            // Wide integers (and JSON/Variant/Dynamic/Time/Interval/…) are now mapped to Arrow, so the
            // original `toInt128(1)` example is no longer unsupported. An AggregateFunction state is
            // recognised but undecodable by the core, which raises UnsupportedTypeException → the ADBC
            // bridge maps that to NOT_IMPLEMENTED (not a raw RuntimeException, not a generic IO error).
            statement.setSqlQuery("SELECT uniqState(number) AS x FROM numbers(3)");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        } finally {
            database.close();
        }
    }

    // ---- #6: subset ingest lets omitted columns take their DEFAULT -------------------------

    @Test
    @DisplayName("#6 ingesting a column subset applies the server DEFAULT for omitted columns")
    void subsetIngestUsesServerDefault(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("reg_def");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE TABLE " + table
                    + " (id Int64, tag String DEFAULT 'auto') ENGINE = MergeTree ORDER BY id");

            try (VectorSchemaRoot root =
                    VectorSchemaRoot.create(ClickHouseArrowTypes.schema(List.of("id"), List.of("Int64")), allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                id.setSafe(0, 7);
                id.setValueCount(1);
                root.setRowCount(1);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertEquals(1, ingest.executeUpdate().getAffectedRows());
                }
            }
        } finally {
            database.close();
        }

        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
            assertEquals("auto", firstString(core, "SELECT tag FROM " + table));
        }
    }

    // ---- #7: read→CREATE round trip preserves the source ClickHouse type --------------------

    @Test
    @DisplayName("#7 read→CREATE ingest preserves Date (not Date32) and UUID (not FixedString)")
    void createIngestPreservesSourceType(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("reg_rt");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        // The reader pins its own connection for the duration of the scan, so ingest must run on a
        // second connection (it only reads the in-memory Arrow root, not the read connection).
        try (AdbcConnection readConnection = database.connect();
                AdbcConnection ingestConnection = database.connect();
                AdbcStatement read = readConnection.createStatement()) {
            read.setSqlQuery("SELECT toDate('2020-05-01') AS d, "
                    + "toUUID('00112233-4455-6677-8899-aabbccddeeff') AS id");
            try (AdbcStatement.QueryResult result = read.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertTrue(reader.loadNextBatch());
                try (AdbcStatement ingest = ingestConnection.bulkIngest(table, BulkIngestMode.CREATE)) {
                    ingest.bind(root);
                    assertEquals(1, ingest.executeUpdate().getAffectedRows());
                }
            }
        } finally {
            database.close();
        }

        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
            Map<String, String> types = columnTypes(core, table);
            assertEquals("Date", types.get("d"));
            assertEquals("UUID", types.get("id"));
            assertEquals("2020-05-01", firstString(core, "SELECT toString(d) FROM " + table));
            assertEquals(
                    "00112233-4455-6677-8899-aabbccddeeff",
                    firstString(core, "SELECT toString(id) FROM " + table));
        }
    }

    // ---- #8: getObjects attributes columns correctly for identifiers that contain spaces ----
    // (The original finding misread the join separator as a space; it is collision-proof, but the
    //  source file was NUL-corrupted — see SourceHygieneTest. This guards the behaviour stays right
    //  now that the key is a structured (database, table) pair.)

    @Test
    @DisplayName("#8 getObjects attributes columns to the right table when identifiers contain spaces")
    void getObjectsSpacedIdentifiers(BufferAllocator allocator) throws Exception {
        String base = "rg" + System.nanoTime();
        String dbPlain = base;
        String dbSpaced = base + " a";
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect();
                ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
            core.execute("CREATE DATABASE `" + dbPlain + "`");
            core.execute("CREATE TABLE `" + dbPlain + "`.`a b` (colB Int64) ENGINE = MergeTree ORDER BY colB");
            core.execute("CREATE DATABASE `" + dbSpaced + "`");
            core.execute("CREATE TABLE `" + dbSpaced + "`.`b` (colA Int64) ENGINE = MergeTree ORDER BY colA");
            try {
                Map<String, List<String>> byQualified = new LinkedHashMap<>();
                try (ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.ALL, null, base + "%", null, null, null)) {
                    collectObjects(reader, byQualified);
                }
                assertEquals(List.of("colB"), byQualified.get(dbPlain + "::a b"));
                assertEquals(List.of("colA"), byQualified.get(dbSpaced + "::b"));
            } finally {
                core.execute("DROP DATABASE IF EXISTS `" + dbPlain + "`");
                core.execute("DROP DATABASE IF EXISTS `" + dbSpaced + "`");
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers ---------------------------------------------------------------------------

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }

    private static String firstString(ClickHouseConnection core, String sql) {
        try (QueryResult result = core.query(sql)) {
            for (Object[] row : BlockMaterializer.materialize(result)) {
                return String.valueOf(row[0]);
            }
        }
        return null;
    }

    private static Map<String, String> columnTypes(ClickHouseConnection core, String table) {
        Map<String, String> out = new HashMap<>();
        try (QueryResult result = core.query(
                "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '" + table + "'")) {
            for (Object[] row : BlockMaterializer.materialize(result)) {
                out.put(String.valueOf(row[0]), String.valueOf(row[1]));
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void collectObjects(ArrowReader reader, Map<String, List<String>> out) throws Exception {
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
                    String schemaName = String.valueOf(schema.get("db_schema_name"));
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
                        out.put(schemaName + "::" + tableName, names);
                    }
                }
            }
        }
    }
}
