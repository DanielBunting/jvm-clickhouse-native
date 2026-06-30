package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Error-path coverage for the write bridge ({@link ArrowToBlock} via {@code bulkIngest}). The
 * happy round trip lives in {@link AdbcIngestRoundTripIT}; this pins the conditions that must fail
 * loudly (as an {@link AdbcException}, never a raw crash or silent corruption) and the degenerate
 * empty-root case that must succeed as a no-op.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcIngestErrorsIT extends AdbcRoundTripBase {

    @Test
    @DisplayName("ingesting a zero-row root is a no-op that reports zero affected rows")
    void emptyRootIngestsNoRows(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("ingest_empty");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE TABLE " + table + " (id Int64) ENGINE = MergeTree ORDER BY id");

            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("id"), List.of("Int64")), allocator)) {
                root.setRowCount(0);
                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertEquals(0, ingest.executeUpdate().getAffectedRows(), "no rows ingested");
                }
            }
        } finally {
            database.close();
        }

        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
            assertEquals(0L, core.executeScalar("SELECT count() FROM " + table));
        }
    }

    @Test
    @DisplayName("a bound column absent from the target table fails with an AdbcException")
    void boundColumnAbsentFromTargetFails(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("ingest_badcol");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE TABLE " + table + " (id Int64) ENGINE = MergeTree ORDER BY id");

            // The root names a column the table does not have; the named-column INSERT must reject it.
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("ghost"), List.of("Int64")), allocator)) {
                BigIntVector ghost = (BigIntVector) root.getVector("ghost");
                ghost.setSafe(0, 1);
                ghost.setValueCount(1);
                root.setRowCount(1);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertThrows(AdbcException.class, ingest::executeUpdate);
                }
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("an Arrow type that does not match the target column fails with an AdbcException")
    void typeMismatchOnIngestFails(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("ingest_badtype");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            execDdl(connection, "CREATE TABLE " + table + " (id Int64) ENGINE = MergeTree ORDER BY id");

            // Non-numeric String data bound to an Int64 column: the integer codec cannot accept the
            // boxed String, so the ingest must fail loudly rather than corrupt the column. (Numeric
            // Arrow types are coerced by width, so the mismatch here is deliberately a String.)
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("id"), List.of("String")), allocator)) {
                VarCharVector id = (VarCharVector) root.getVector("id");
                id.setSafe(0, "not-a-number".getBytes(StandardCharsets.UTF_8));
                id.setValueCount(1);
                root.setRowCount(1);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertThrows(AdbcException.class, ingest::executeUpdate);
                }
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("APPEND into a table that does not exist fails with an AdbcException")
    void appendToMissingTableFails(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("ingest_notable");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("id", "label"), List.of("Int64", "String")), allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector label = (VarCharVector) root.getVector("label");
                id.setSafe(0, 1);
                label.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
                id.setValueCount(1);
                label.setValueCount(1);
                root.setRowCount(1);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertThrows(AdbcException.class, ingest::executeUpdate);
                }
            }
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
}
