package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration coverage for {@link ClickHouseConnectionPool} behaviours not exercised by the
 * concurrency/resilience ITs: a thread interrupted while blocked in {@code borrow()} must
 * throw {@link ClickHouseException} and re-assert the interrupt flag, and a pooled
 * connection's {@code close()} must be idempotent (return its permit exactly once).
 *
 * <p>Not covered here: a socket that dies while idle in the pool needs fault injection to
 * simulate deterministically — that path belongs with the Toxiproxy chaos ITs; the
 * abandoned-dirty-stream variant is already covered by {@code PoolResilienceIT}.
 */
@Tag("integration")
class PoolBehaviorIT extends IntegrationTestBase {

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    @Test
    @Timeout(15)
    void interruptedWhileBorrowing_throwsAndPreservesInterruptFlag() throws Exception {
        // Long borrow timeout so the waiter is genuinely blocked (not timed out) when interrupted.
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                .size(1).borrowTimeout(Duration.ofSeconds(30)).build()) {
            ClickHouseConnection held = pool.borrow(); // take the only permit
            try {
                AtomicReference<Throwable> error = new AtomicReference<>();
                AtomicBoolean interruptFlagSet = new AtomicBoolean();
                Thread waiter = new Thread(() -> {
                    try {
                        pool.borrow(); // blocks: no permit available
                    } catch (Throwable t) {
                        error.set(t);
                        // borrow() re-asserts the interrupt status before throwing.
                        interruptFlagSet.set(Thread.currentThread().isInterrupted());
                    }
                }, "pool-borrow-waiter");

                waiter.start();
                // Give the waiter time to reach the blocking tryAcquire, then interrupt it.
                Thread.sleep(300);
                waiter.interrupt();
                waiter.join(5_000);

                assertInstanceOf(ClickHouseException.class, error.get(),
                        "an interrupted borrow surfaces as ClickHouseException");
                assertTrue(interruptFlagSet.get(),
                        "borrow() restores the thread's interrupt flag before throwing");
            } finally {
                held.close();
            }
        }
    }

    @Test
    void pooledConnectionDoubleClose_returnsPermitExactlyOnce() {
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                .size(1).build()) {
            assertEquals(1, pool.available(), "one permit before borrow");

            ClickHouseConnection c = pool.borrow();
            assertEquals(0, pool.available(), "permit taken by the borrow");

            c.close();
            assertEquals(1, pool.available(), "first close returns the permit");
            c.close(); // idempotent — must NOT release a second permit
            assertEquals(1, pool.available(),
                    "second close is a no-op; the pool never exceeds its size");

            // The slot is still usable after the double close.
            try (ClickHouseConnection again = pool.borrow()) {
                assertEquals(1L, again.executeScalar("SELECT 1"));
            }
        }
    }

    /**
     * A pooled connection is a full {@link ClickHouseConnection}: the combined
     * {@code (sql, params, settings)} overloads and {@code ping()} delegate to the
     * borrowed underlying connection rather than falling back to the interface
     * defaults (which would throw {@link UnsupportedOperationException} for a
     * genuinely combined call). Proven end-to-end: the bound parameter and the
     * per-query setting both reach the server through the pooled wrapper.
     */
    @Test
    void pooledConnectionDelegatesCombinedOverloadsAndPing() {
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                .size(1).build()) {
            try (ClickHouseConnection conn = pool.borrow()) {
                assertTrue(conn.ping(), "ping() delegates to the live underlying connection");

                io.github.danielbunting.clickhouse.QueryParameters params =
                        io.github.danielbunting.clickhouse.QueryParameters.of(
                                java.util.Map.of("n", 5));
                java.util.Map<String, String> settings =
                        java.util.Map.of("max_block_size", "1000");

                try (io.github.danielbunting.clickhouse.QueryResult r = conn.query(
                        "SELECT {n:UInt32} + toUInt32(getSetting('max_block_size')) AS v",
                        params, settings)) {
                    long v = -1;
                    var blocks = r.blocks();
                    while (blocks.hasNext()) {
                        var block = blocks.next();
                        if (block.rowCount() > 0) {
                            v = ((Number) block.column(0).value(0)).longValue();
                        }
                    }
                    assertEquals(1005L, v,
                            "the combined query overload must delegate both legs, not "
                                    + "hit the interface default");
                }

                // The combined execute overload also delegates (it would throw
                // UnsupportedOperationException on the interface default).
                conn.execute("SELECT {n:UInt32}", params, settings);

                assertTrue(conn.ping(), "the pooled connection stays healthy afterwards");
            }
        }
    }
}
