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

    /**
     * {@code ping()} while the connection has an in-flight operation (a lazy,
     * unconsumed {@link QueryResult} holding the guard) never throws, honoring the
     * {@link ClickHouseConnection#ping()} contract: a busy connection answers
     * {@code true} — an active stream proves the connection is alive, and the probe
     * must not touch the socket mid-stream. (was knownBug 41)
     */
    @Test
    void pingNeverThrowsWhileConnectionBusy() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE IF NOT EXISTS guard_ping_t (n UInt32) ENGINE = MergeTree ORDER BY n");
            conn.execute("INSERT INTO guard_ping_t SELECT number FROM numbers(100000)");

            // Open a lazy query mid-consumption — the guard is held, the connection busy.
            QueryResult open = conn.query("SELECT n FROM guard_ping_t");
            Iterator<Block> it = open.blocks();
            it.hasNext(); // pull the first block; the connection is mid-stream
            try {
                boolean alive = org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                        conn::ping,
                        "ping() is documented 'Never throws' — a busy connection must "
                                + "yield a boolean, not ConcurrentConnectionUseException");
                org.junit.jupiter.api.Assertions.assertTrue(alive,
                        "a busy connection is alive by definition: ping() answers true "
                                + "without touching the mid-stream socket");
            } finally {
                open.close();
            }

            // Once idle again, ping is an honest liveness probe and the connection works.
            assertEquals(true, conn.ping(), "idle connection should ping true");
            assertEquals(100000L, conn.executeScalar("SELECT count() FROM guard_ping_t"));
            conn.execute("DROP TABLE IF EXISTS guard_ping_t");
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
