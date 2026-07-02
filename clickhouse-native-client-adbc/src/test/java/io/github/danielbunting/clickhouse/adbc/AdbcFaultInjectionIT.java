package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.FaultInjectingProxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Socket-level robustness via a byte-level TCP proxy ({@link FaultInjectingProxy}, shared from
 * the core module's test fixtures) in front of the ClickHouse container: a connection killed
 * mid-stream or pre-handshake must fail promptly as an error — never hang — and the failure must
 * not poison subsequent fresh connections. The ADBC analogue of the core module's
 * {@code PingFaultInjectionIT} robustness approach (the JDBC suite induces faults server-side
 * instead; the proxy covers the harder no-cooperating-server half).
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcFaultInjectionIT extends AdbcIntegrationTest {

    private static Map<String, Object> proxiedParams(FaultInjectingProxy proxy) {
        Map<String, Object> params = new java.util.HashMap<>();
        params.put(AdbcParams.PARAM_HOST, proxy.host());
        params.put(AdbcParams.PARAM_PORT, proxy.port());
        return params;
    }

    @Test
    @DisplayName("killing the connection mid-stream surfaces an error from loadNextBatch, not a hang")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void killMidStreamSurfacesError(BufferAllocator allocator) throws Exception {
        try (FaultInjectingProxy proxy = new FaultInjectingProxy(
                CLICKHOUSE.getHost(), CLICKHOUSE.getMappedPort(NATIVE_PORT))) {
            AdbcDatabase database = new ChAdbcDriver(allocator).open(proxiedParams(proxy));
            try (AdbcConnection conn = database.connect();
                    AdbcStatement statement = conn.createStatement()) {
                // Slow trickle so the stream is guaranteed live when the proxy dies.
                statement.setSqlQuery("SELECT number, sleepEachRow(0.02) FROM system.numbers "
                        + "LIMIT 100000 SETTINGS max_block_size = 1");
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    ArrowReader reader = result.getReader();
                    assertTrue(reader.loadNextBatch(), "the stream must be live before the kill");

                    proxy.close();

                    assertThrows(Exception.class, () -> {
                        while (reader.loadNextBatch()) {
                            // must terminate with an error once the socket is gone
                        }
                    }, "a dead socket mid-stream must fail the read, not hang it");
                }
            } catch (Exception ignored) {
                // Closing the statement/connection over the dead socket may itself throw
                // (AdbcException or a raw core ConnectionException); the contract under test
                // is the read failing promptly above — assertion errors still propagate.
            } finally {
                closeQuietly(database);
            }
        }
    }

    @Test
    @DisplayName("a connection attempt against a dead endpoint fails as IO")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void killBeforeHandshakeFailsConnect(BufferAllocator allocator) throws Exception {
        FaultInjectingProxy proxy = new FaultInjectingProxy(
                CLICKHOUSE.getHost(), CLICKHOUSE.getMappedPort(NATIVE_PORT));
        Map<String, Object> params = proxiedParams(proxy);
        proxy.close();

        AdbcDatabase database = new ChAdbcDriver(allocator).open(params);
        try {
            AdbcException ex = assertThrows(AdbcException.class, () -> database.connect().close());
            assertEquals(org.apache.arrow.adbc.core.AdbcStatusCode.IO, ex.getStatus());
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a fresh direct connection works after a proxied one was killed")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void freshConnectionWorksAfterKill(BufferAllocator allocator) throws Exception {
        try (FaultInjectingProxy proxy = new FaultInjectingProxy(
                CLICKHOUSE.getHost(), CLICKHOUSE.getMappedPort(NATIVE_PORT))) {
            AdbcDatabase proxied = new ChAdbcDriver(allocator).open(proxiedParams(proxy));
            try {
                AdbcConnection conn = proxied.connect();
                proxy.close();
                try {
                    conn.close();
                } catch (Exception ignored) {
                    // Closing over the dead socket may fail; irrelevant to the contract.
                }
            } finally {
                closeQuietly(proxied);
            }
        }

        AdbcDatabase direct = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = direct.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(1) AS n");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch());
                assertEquals(List.of(1L),
                        List.of(((BigIntVector) reader.getVectorSchemaRoot().getVector("n")).get(0)),
                        "the poisoned proxied connection must not affect fresh connections");
            }
        } finally {
            direct.close();
        }
    }

    /** Closes a database, swallowing failures from underlying dead sockets. */
    private static void closeQuietly(AdbcDatabase database) {
        try {
            database.close();
        } catch (Exception ignored) {
            // A dead child socket may fail allocator-order close; irrelevant here.
        }
    }
}
