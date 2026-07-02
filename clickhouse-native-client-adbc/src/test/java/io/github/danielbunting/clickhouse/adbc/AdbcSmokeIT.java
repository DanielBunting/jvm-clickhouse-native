package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Slice 2: driver/database/connection/statement skeleton + the streaming Arrow reader. */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcSmokeIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("SELECT round-trips Int64 + String columns through the ADBC reader")
    void selectLiteralsRoundTrip(BufferAllocator allocator) throws Exception {
        AdbcDriver driver = new ChAdbcDriver(allocator);
        try (AdbcDatabase database = driver.open(connectParams());
                AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(number) AS n, toString(number) AS s FROM numbers(3)");

            List<Long> ns = new ArrayList<>();
            List<String> ss = new ArrayList<>();
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    BigIntVector n = (BigIntVector) root.getVector("n");
                    VarCharVector s = (VarCharVector) root.getVector("s");
                    for (int r = 0; r < root.getRowCount(); r++) {
                        ns.add(n.get(r));
                        ss.add(new String(s.get(r), StandardCharsets.UTF_8));
                    }
                }
            }

            assertEquals(List.of(0L, 1L, 2L), ns);
            assertEquals(List.of("0", "1", "2"), ss);
        }
    }

    @Test
    @DisplayName("A connection URI parameter resolves the same server")
    void connectsViaUri(BufferAllocator allocator) throws Exception {
        AdbcDriver driver = new ChAdbcDriver(allocator);
        try (AdbcDatabase database = driver.open(uriParams());
                AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(42) AS answer");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertTrue(reader.loadNextBatch());
                assertEquals(42L, ((BigIntVector) root.getVector("answer")).get(0));
            }
        }
    }

    @Test
    @DisplayName("An empty result still exposes the column schema and yields zero rows")
    void emptyResultExposesSchemaWithNoRows(BufferAllocator allocator) throws Exception {
        AdbcDriver driver = new ChAdbcDriver(allocator);
        try (AdbcDatabase database = driver.open(connectParams());
                AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(number) AS n FROM numbers(10) WHERE number < 0");

            long total = 0;
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                // The schema is known up front from the result header, before any batch is read.
                assertEquals("n", root.getSchema().getFields().get(0).getName());
                while (reader.loadNextBatch()) {
                    total += root.getRowCount();
                }
            }
            assertEquals(0, total, "an empty result must surface no rows");
        }
    }

    @Test
    @DisplayName("A multi-block result yields multiple Arrow batches (streaming, not buffered)")
    void streamsMultipleBatches(BufferAllocator allocator) throws Exception {
        int rowCount = 200_000;
        AdbcDriver driver = new ChAdbcDriver(allocator);
        try (AdbcDatabase database = driver.open(connectParams());
                AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(number) AS n FROM numbers(" + rowCount + ")");

            int batches = 0;
            long total = 0;
            long checksum = 0;
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    batches++;
                    BigIntVector n = (BigIntVector) root.getVector("n");
                    int rows = root.getRowCount();
                    total += rows;
                    for (int r = 0; r < rows; r++) {
                        checksum += n.get(r);
                    }
                }
            }

            assertEquals(rowCount, total, "every row must arrive");
            assertTrue(batches > 1, "expected >1 Arrow batch for a multi-block result, got " + batches);
            assertEquals((long) rowCount * (rowCount - 1) / 2, checksum, "sum of 0..N-1");
        }
    }
}
