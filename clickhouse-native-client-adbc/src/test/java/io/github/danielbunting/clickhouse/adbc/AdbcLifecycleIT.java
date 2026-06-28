package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Slice 6: lifecycle, close ownership, cancellation, and unsupported-transaction stances. */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcLifecycleIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("Double-close of every level is safe")
    void doubleCloseIsSafe(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        AdbcConnection connection = database.connect();
        AdbcStatement statement = connection.createStatement();
        statement.setSqlQuery("SELECT toInt64(1) AS a");

        AdbcStatement.QueryResult result = statement.executeQuery();
        ArrowReader reader = result.getReader();
        assertTrue(reader.loadNextBatch());

        result.close();
        result.close(); // idempotent
        statement.close();
        statement.close();
        connection.close();
        connection.close();
        database.close();
        database.close();
    }

    @Test
    @DisplayName("Closing a reader mid-stream frees all Arrow buffers")
    void closingMidStreamFreesBuffers(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(number) AS n FROM numbers(2000000)");

            AdbcStatement.QueryResult result = statement.executeQuery();
            ArrowReader reader = result.getReader();
            // Pull a single batch out of a multi-block result, then abandon the rest.
            assertTrue(reader.loadNextBatch());
            result.close();
        } finally {
            database.close();
        }
        // The ArrowAllocatorExtension asserts zero outstanding bytes at teardown.
    }

    @Test
    @DisplayName("cancel() during a large scan is accepted and cleans up")
    void cancelDuringScan(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect();
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(number) AS n FROM numbers(500000000)");

            AdbcStatement.QueryResult result = statement.executeQuery();
            ArrowReader reader = result.getReader();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(root.getFieldVectors().size() == 1);
            reader.loadNextBatch();

            connection.cancel();
            // Draining after cancel must terminate (no hang) and free buffers on close.
            try {
                while (reader.loadNextBatch()) {
                    // discard remaining batches until the cancelled stream ends
                }
            } catch (Exception expectedAfterCancel) {
                // A cancelled stream may surface as an IO/cancelled error; that's acceptable.
            }
            result.close();
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("Transactions are autocommit-only; the rest is NOT_IMPLEMENTED")
    void transactionsAreAutocommitOnly(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            assertTrue(connection.getAutoCommit());
            connection.setAutoCommit(true); // no-op, allowed

            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED,
                    assertThrows(AdbcException.class, () -> connection.setAutoCommit(false)).getStatus());
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED,
                    assertThrows(AdbcException.class, connection::commit).getStatus());
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED,
                    assertThrows(AdbcException.class, connection::rollback).getStatus());
        } finally {
            database.close();
        }
    }
}
