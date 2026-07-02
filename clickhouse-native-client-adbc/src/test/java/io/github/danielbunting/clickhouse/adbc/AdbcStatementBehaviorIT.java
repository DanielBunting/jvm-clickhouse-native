package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Statement/streaming behavior against a live server, ported from the JDBC module's
 * {@code JdbcStatementIT}: DDL and INSERT…SELECT through executeUpdate, odd SQL shapes,
 * cross-thread cancel unblocking a streaming reader, sequential statement reuse, large-stream
 * row accounting, and early reader abandonment leaving the connection usable.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcStatementBehaviorIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("DDL and INSERT…SELECT run through executeUpdate; row counts stay -1 (protocol has none)")
    void ddlAndInsertSelectViaExecuteUpdate(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_beh_ddl");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            assertEquals(-1, execDdl(conn, "CREATE TABLE " + table + " (n Int64) ENGINE = Memory"));
            try {
                assertEquals(-1, execDdl(conn,
                        "INSERT INTO " + table + " SELECT number FROM numbers(10)"));
                assertEquals(List.of(10L), readLongs(conn, "SELECT count() FROM " + table));
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("comments, trailing semicolons and unicode identifiers pass through verbatim")
    void oddSqlShapesExecute(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            assertEquals(List.of(7L), readLongs(conn,
                    "SELECT /* block ? comment */ toInt64(7) AS `значение` -- trailing ?\n;"));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("connection.cancel() from another thread unblocks a streaming reader")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void cancelDuringLongScanUnblocksReader(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            // ~50 rows/second: without a cancel this scan would run for ~200 seconds.
            statement.setSqlQuery("SELECT number, sleepEachRow(0.02) FROM system.numbers "
                    + "LIMIT 10000 SETTINGS max_block_size = 1");

            AtomicReference<Throwable> failure = new AtomicReference<>();
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch(), "the stream must be live before the cancel");

                Thread canceller = new Thread(() -> {
                    try {
                        Thread.sleep(200);
                        conn.cancel();
                    } catch (Exception e) {
                        failure.set(e);
                    }
                }, "adbc-cancel");
                canceller.start();

                // The read loop must terminate promptly — either by a thrown cancellation
                // error or by the server ending the stream after the cancel.
                try {
                    while (reader.loadNextBatch()) {
                        // draining
                    }
                } catch (RuntimeException expected) {
                    // A cancellation surfacing as an exception is an accepted outcome.
                }
                canceller.join();
                assertEquals(null, failure.get(), "the cancelling thread must not fail");
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("many sequential statements on one connection share the native session cleanly")
    void sequentialStatementsShareConnection(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            for (int i = 0; i < 25; i++) {
                assertEquals(List.of((long) i), readLongs(conn, "SELECT toInt64(" + i + ")"),
                        "statement round " + i);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a large result streams multiple batches whose row counts sum exactly")
    void largeStreamRowAccounting(BufferAllocator allocator) throws Exception {
        int rows = 500_000;
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(number) AS n FROM numbers(" + rows + ")");
            long total = 0;
            long checksum = 0;
            int batches = 0;
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    batches++;
                    BigIntVector n = (BigIntVector) root.getVector("n");
                    for (int r = 0; r < root.getRowCount(); r++) {
                        checksum += n.get(r);
                    }
                    total += root.getRowCount();
                }
            }
            assertEquals(rows, total, "every row must arrive exactly once");
            assertEquals((long) rows * (rows - 1) / 2, checksum, "no row content lost or repeated");
            assertTrue(batches > 1, "a big scan must stream, not buffer (got " + batches + " batch)");
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("abandoning a reader mid-stream leaves the connection usable for the next query")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void earlyReaderCloseLeavesConnectionUsable(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            try (AdbcStatement statement = conn.createStatement()) {
                statement.setSqlQuery("SELECT toInt64(number) AS n FROM numbers(5000000)");
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    ArrowReader reader = result.getReader();
                    assertTrue(reader.loadNextBatch(), "read only the first batch, then abandon");
                }
            }
            assertEquals(List.of(1L), readLongs(conn, "SELECT toInt64(1)"),
                    "the session must survive an abandoned stream");
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a second executeQuery on the same statement replaces the first completed stream")
    void statementReexecutesAfterDrainedQuery(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(1) AS n");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch());
                assertEquals(1L, ((BigIntVector) reader.getVectorSchemaRoot().getVector("n")).get(0));
            }
            statement.setSqlQuery("SELECT toInt64(2) AS n");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch());
                assertEquals(2L, ((BigIntVector) reader.getVectorSchemaRoot().getVector("n")).get(0));
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("executeQuery on an INSERT…SELECT (no result) yields an empty stream and applies the write")
    void executeQueryOnInsertSelectYieldsEmptyStream(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_beh_insq");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (n Int64) ENGINE = Memory");
            try (AdbcStatement statement = conn.createStatement()) {
                statement.setSqlQuery("INSERT INTO " + table + " SELECT number FROM numbers(3)");
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    while (result.getReader().loadNextBatch()) {
                        // drain whatever the server sends for a resultless statement
                    }
                }
                assertEquals(List.of(3L), readLongs(conn, "SELECT count() FROM " + table),
                        "the INSERT routed through the query path must still apply");
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static long execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            return ddl.executeUpdate().getAffectedRows();
        }
    }

    /** Drains a one-integer-column query into longs ({@code count()} returns UInt64). */
    private static List<Long> readLongs(AdbcConnection adbc, String sql) throws Exception {
        java.util.ArrayList<Long> out = new java.util.ArrayList<>();
        try (AdbcStatement statement = adbc.createStatement()) {
            statement.setSqlQuery(sql);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    org.apache.arrow.vector.BaseIntVector vector =
                            (org.apache.arrow.vector.BaseIntVector) root.getVector(0);
                    for (int r = 0; r < root.getRowCount(); r++) {
                        out.add(vector.getValueAsLong(r));
                    }
                }
            }
        }
        return out;
    }
}
