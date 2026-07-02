package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.ServerException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Server and client errors surface as {@link AdbcException} with the ClickHouse error code as
 * the vendor code and the core {@link ServerException} in the cause chain — ported from the JDBC
 * module's {@code JdbcErrorHandlingIT} (whose contract is {@code SQLException.getErrorCode()}).
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcErrorHandlingIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("malformed SQL raises IO with SYNTAX_ERROR (62) as the vendor code")
    void malformedSqlRaisesIoWithSyntaxError(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT FROM WHERE");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertEquals(62, ex.getVendorCode(), "SYNTAX_ERROR (62): " + ex.getMessage());
            assertNotNull(serverException(ex), "the core ServerException must be in the cause chain");
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a missing table raises IO with UNKNOWN_TABLE (60) naming the table")
    void missingTableRaisesIoWithUnknownTable(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT * FROM no_such_table_xyz");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertEquals(60, ex.getVendorCode(), ex.getMessage());
            assertTrue(ex.getMessage().contains("no_such_table_xyz"),
                    "the failure must name the missing table: " + ex.getMessage());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a missing database raises IO with UNKNOWN_DATABASE (81) at executeQuery time")
    void missingDatabaseRaisesUnknownDatabase(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT * FROM no_such_db_xyz.unknown_table");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(81, ex.getVendorCode(), ex.getMessage());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a failing DDL through executeUpdate carries the server code too")
    void executeUpdateCarriesVendorCode(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("DROP TABLE definitely_absent_table_xyz");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertEquals(60, ex.getVendorCode(), ex.getMessage());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("connecting to an unroutable endpoint fails as IO within the connect timeout")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void connectTimeoutToUnroutableHost(BufferAllocator allocator) throws Exception {
        // 10.255.255.1 is a blackhole address: SYN packets vanish, so only the client-side
        // connect timeout terminates the attempt.
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://10.255.255.1:9000/default?connectTimeout=2");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(params);
        try {
            AdbcException ex = assertThrows(AdbcException.class, () -> database.connect().close());
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("DIVERGENCE: a mid-stream server abort surfaces from loadNextBatch as the raw core exception")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void midStreamTimeoutSurfacesFromLoadNextBatch(BufferAllocator allocator) throws Exception {
        // sleepEachRow(0.05) x 100 with max_block_size=1 streams the first 1-row blocks well
        // before the 1s deadline (so executeQuery succeeds), then the server aborts mid-stream.
        // Unlike JDBC (which wraps mid-stream aborts into SQLTimeoutException), the ADBC reader
        // surfaces the raw ClickHouseException from loadNextBatch — pinned as the contract, with
        // the TIMEOUT_EXCEEDED (159) code proving the scenario.
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            execDdl(conn, "SET max_execution_time = 1");
            statement.setSqlQuery("SELECT number, sleepEachRow(0.05) FROM system.numbers LIMIT 100 "
                    + "SETTINGS max_block_size = 1");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                ClickHouseException ex = assertThrows(ClickHouseException.class, () -> {
                    while (reader.loadNextBatch()) {
                        // drain until the server-side abort arrives mid-stream
                    }
                });
                ServerException server = serverException(ex);
                assertNotNull(server, "sanity: the abort must be a server exception: " + ex);
                assertEquals(159, server.code(),
                        "sanity: the mid-stream abort is TIMEOUT_EXCEEDED (159): " + ex.getMessage());
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("the connection stays usable after a server error (clean failures don't poison)")
    void connectionUsableAfterServerError(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            try (AdbcStatement statement = conn.createStatement()) {
                statement.setSqlQuery("SELECT * FROM no_such_table_xyz");
                assertThrows(AdbcException.class, statement::executeQuery);
            }
            try (AdbcStatement statement = conn.createStatement()) {
                statement.setSqlQuery("SELECT toInt64(1) AS n");
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    assertTrue(result.getReader().loadNextBatch(),
                            "a clean server error must not poison the session");
                }
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** First core {@link ServerException} in the cause chain, or {@code null}. */
    private static ServerException serverException(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ServerException se) {
                return se;
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
