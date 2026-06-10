package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ConcurrentConnectionUseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Proves the fail-fast guard turns concurrent/overlapping use of a single connection
 * into a clear {@link ConcurrentConnectionUseException} instead of silent protocol
 * corruption, and that the connection is reusable once the open result is closed.
 */
@Tag("integration")
class ConnectionGuardIT extends IntegrationTestBase {

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    @Test
    void overlappingUseOnSameThreadThrowsThenRecovers() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE IF NOT EXISTS guard_t (n UInt32) ENGINE = MergeTree ORDER BY n");
            conn.execute("INSERT INTO guard_t VALUES (1),(2),(3)");

            // Open a lazy query and do NOT consume it — the connection stays in use.
            QueryResult open = conn.query("SELECT n FROM guard_t");
            try {
                assertThrows(ConcurrentConnectionUseException.class,
                        () -> conn.executeScalar("SELECT count() FROM guard_t"));
            } finally {
                open.close();
            }

            // After closing the result, the connection works again (no corruption).
            assertEquals(3L, conn.executeScalar("SELECT count() FROM guard_t"));
            conn.execute("DROP TABLE IF EXISTS guard_t");
        }
    }

    @Test
    void secondThreadFailsFastWhileStreaming() throws Exception {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE IF NOT EXISTS guard_t2 (n UInt32) ENGINE = MergeTree ORDER BY n");
            conn.execute("INSERT INTO guard_t2 SELECT number FROM numbers(100000)");

            QueryResult result = conn.query("SELECT n FROM guard_t2");
            Iterator<Block> it = result.blocks();
            it.hasNext(); // pull the first block; the connection is mid-stream

            AtomicReference<Throwable> err = new AtomicReference<>();
            Thread other = new Thread(() -> {
                try {
                    conn.executeScalar("SELECT 1");
                } catch (Throwable e) {
                    err.set(e);
                }
            });
            other.start();
            other.join();
            assertInstanceOf(ConcurrentConnectionUseException.class, err.get());

            result.close();
            assertEquals(100000L, conn.executeScalar("SELECT count() FROM guard_t2"));
            conn.execute("DROP TABLE IF EXISTS guard_t2");
        }
    }
}
