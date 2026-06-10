package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link ClickHouseConnectionPool} supports correct concurrent insert/query
 * across many threads, enforces borrow timeouts, and shuts down cleanly.
 */
@Tag("integration")
class ConnectionPoolIT extends IntegrationTestBase {

    /** Row type for the concurrent bulk-insert test (field names match columns). */
    public record PoolRow(long id, long threadId) {
    }

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    @Test
    void concurrentInsertAndQueryAcrossThreads() throws Exception {
        int threads = 16;
        int perThread = 500;
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.create(config(), 8)) {
            pool.useConnection(c -> c.execute(
                    "CREATE TABLE IF NOT EXISTS pool_t (id UInt64, threadId UInt64) "
                    + "ENGINE = MergeTree ORDER BY id"));

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final long tid = t;
                    futures.add(exec.submit(() -> pool.useConnection(c -> {
                        List<PoolRow> rows = new ArrayList<>(perThread);
                        for (int i = 0; i < perThread; i++) {
                            rows.add(new PoolRow(tid * 100_000L + i, tid));
                        }
                        try (BulkInserter<PoolRow> ins = c.createBulkInserter("pool_t", PoolRow.class)) {
                            ins.init();
                            ins.addRange(rows);
                            ins.complete();
                        }
                        // Also issue a query in the same borrow to exercise both paths.
                        c.executeScalar("SELECT count() FROM pool_t");
                    })));
                }
                for (Future<?> f : futures) {
                    f.get();
                }
            } finally {
                exec.shutdown();
            }

            long total = pool.withConnection(c -> c.executeScalar("SELECT count() FROM pool_t"));
            assertEquals((long) threads * perThread, total);
            pool.useConnection(c -> c.execute("DROP TABLE IF EXISTS pool_t"));
        }
    }

    @Test
    void borrowTimesOutWhenExhausted() {
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                .size(1).borrowTimeout(Duration.ofMillis(200)).build()) {
            ClickHouseConnection held = pool.borrow(); // takes the only connection
            try {
                assertThrows(ClickHouseException.class, pool::borrow);
            } finally {
                held.close(); // return it
            }
            try (ClickHouseConnection c = pool.borrow()) {
                assertEquals(1L, c.executeScalar("SELECT 1"));
            }
        }
    }

    @Test
    void closeShutsPoolAndBlocksBorrow() {
        ClickHouseConnectionPool pool = ClickHouseConnectionPool.create(config(), 2);
        pool.close();
        assertThrows(ClickHouseException.class, pool::borrow);
        pool.close(); // idempotent
    }
}
