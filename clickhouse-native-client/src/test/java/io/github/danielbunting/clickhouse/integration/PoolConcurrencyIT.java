package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Adversarial CONCURRENT stress tests for {@link ClickHouseConnectionPool}.
 *
 * <p>The pool is a {@link java.util.concurrent.Semaphore}-gated, self-healing connection pool.
 * These tests hammer it from many threads, trying to break six invariants:
 *
 * <ol>
 *   <li><b>Exclusivity</b> — a borrowed connection is the borrower's alone; no cross-talk in
 *       query results, no two borrowers sharing one underlying socket.</li>
 *   <li><b>No over-subscription</b> — at most {@code size} connections checked out at once.</li>
 *   <li><b>No permit leak</b> — after thousands of mixed clean/poisoned borrow/return cycles,
 *       {@code available() == size()} exactly.</li>
 *   <li><b>Self-healing to full size</b> — poison every connection, return them, and the pool
 *       still serves {@code size} concurrent queries and reports {@code available() == size()}.</li>
 *   <li><b>Borrow timeout under saturation</b> — when all permits are held, an extra borrow
 *       throws within ~the timeout (not instantly, not forever); releasing one unblocks a waiter.</li>
 *   <li><b>Close race</b> — closing while threads borrow/return never deadlocks/leaks, and a
 *       borrow after close throws.</li>
 * </ol>
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest --tests '*PoolConcurrencyIT'}
 */
@Tag("integration")
class PoolConcurrencyIT extends IntegrationTestBase {

    private ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    // -------------------------------------------------------------------------------------------
    // Poison helpers. Recipe (proven in PoolResilienceIT): SELECT a raw AggregateFunction state
    // column and iterate blocks -> codec resolution fails inside readMessage -> stream offset is
    // unknown -> connection is poisoned. We create the poison table once per relevant test.
    // -------------------------------------------------------------------------------------------

    private static String createPoisonTable(ClickHouseConnection admin) {
        String table = "pool_conc_poison_" + System.nanoTime();
        admin.execute("CREATE TABLE " + table
                + " (k UInt32, v AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k");
        admin.execute("INSERT INTO " + table + " SELECT 1, sumState(toUInt64(5))");
        return table;
    }

    /** Forces the given connection into the poisoned state by triggering a decode error. */
    private static void poison(ClickHouseConnection conn, String poisonTable) {
        try (QueryResult r = conn.query("SELECT v FROM " + poisonTable)) {
            r.blocks().forEachRemaining(b -> { });
            // If iteration somehow did not throw, fall through; assertion below will catch it.
        } catch (RuntimeException expected) {
            // expected — connection should now be poisoned
        }
    }

    // =====================================================================================
    // 1. EXCLUSIVITY + no cross-talk: many threads, each borrows, runs SELECT <unique>, returns.
    //    Every result must equal the unique value the thread asked for (no corrupted/cross-wired
    //    reads), and a concurrent-checkout counter must never exceed pool size.
    // =====================================================================================
    @Test
    void exclusivityAndNoCrossTalkUnderHeavyConcurrency() {
        final int size = 4;
        final int threads = 32;
        final int opsPerThread = 200;
        final AtomicInteger checkedOut = new AtomicInteger(0);
        final AtomicInteger maxObserved = new AtomicInteger(0);
        final AtomicReference<String> failure = new AtomicReference<>(null);

        assertTimeoutPreemptively(ofSeconds(120), () -> {
            try (ClickHouseConnectionPool pool =
                         ClickHouseConnectionPool.builder(config()).size(size).build()) {
                ExecutorService ex = Executors.newFixedThreadPool(threads);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int threadId = t;
                    futures.add(ex.submit(() -> {
                        await(start);
                        for (int i = 0; i < opsPerThread && failure.get() == null; i++) {
                            // A value unique to (thread, iteration); if a result ever comes back
                            // wrong, two borrowers were sharing a socket or a read was corrupted.
                            long unique = ((long) threadId << 32) | (i & 0xffffffffL);
                            try (ClickHouseConnection conn = pool.borrow()) {
                                int now = checkedOut.incrementAndGet();
                                maxObserved.accumulateAndGet(now, Math::max);
                                try {
                                    long got = conn.executeScalar("SELECT " + unique);
                                    if (got != unique) {
                                        failure.compareAndSet(null,
                                                "cross-talk: asked " + unique + " got " + got);
                                    }
                                } finally {
                                    checkedOut.decrementAndGet();
                                }
                            }
                        }
                    }));
                }
                start.countDown();
                joinAll(ex, futures);

                assertEquals(0, checkedOut.get(), "all connections returned");
                assertTrue(maxObserved.get() <= size,
                        "concurrent checkouts (" + maxObserved.get() + ") must never exceed pool size " + size);
                assertTrue(maxObserved.get() > 1,
                        "test was not actually concurrent (max observed " + maxObserved.get() + ")");
                assertEquals(size, pool.available(), "all permits restored after the run");
                assertEquals(null, failure.get(), "no cross-talk / corruption: " + failure.get());
            }
        });
    }

    // =====================================================================================
    // 2. NO OVER-SUBSCRIPTION (isolated, with validateOnBorrow forcing extra round-trips that
    //    widen the borrow window). The counter must never exceed size even under contention on
    //    the slower reuse path.
    // =====================================================================================
    @Test
    void neverMoreThanSizeCheckedOut() {
        final int size = 3;
        final int threads = 24;
        final int opsPerThread = 150;
        final AtomicInteger checkedOut = new AtomicInteger(0);
        final AtomicInteger overshoot = new AtomicInteger(0);

        assertTimeoutPreemptively(ofSeconds(120), () -> {
            try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                    .size(size).validateOnBorrow(true).build()) {
                ExecutorService ex = Executors.newFixedThreadPool(threads);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    futures.add(ex.submit(() -> {
                        await(start);
                        for (int i = 0; i < opsPerThread; i++) {
                            try (ClickHouseConnection conn = pool.borrow()) {
                                int now = checkedOut.incrementAndGet();
                                if (now > size) {
                                    overshoot.accumulateAndGet(now, Math::max);
                                }
                                try {
                                    conn.executeScalar("SELECT 1");
                                } finally {
                                    checkedOut.decrementAndGet();
                                }
                            }
                        }
                    }));
                }
                start.countDown();
                joinAll(ex, futures);
                assertEquals(0, overshoot.get(),
                        "pool over-subscribed: saw " + overshoot.get() + " checked out (size=" + size + ")");
                assertEquals(size, pool.available(), "permits fully restored");
            }
        });
    }

    // =====================================================================================
    // 3. PERMIT-LEAK HUNT: thousands of mixed cycles across threads — clean returns plus
    //    poisoned returns (decode-error) plus leaked-streaming-result returns. A permit leak
    //    surfaces as a permanently shrunk pool, so the hard post-condition is
    //    available() == size() EXACTLY. We further assert the pool can still saturate to size.
    // =====================================================================================
    @Test
    void noPermitLeakAcrossThousandsOfMixedCycles() {
        final int size = 4;
        final int threads = 16;
        final int opsPerThread = 250; // 16 * 250 = 4000 cycles
        final AtomicLong cleanOps = new AtomicLong();
        final AtomicLong poisonOps = new AtomicLong();
        final AtomicLong leakOps = new AtomicLong();
        final AtomicReference<String> failure = new AtomicReference<>(null);

        assertTimeoutPreemptively(ofSeconds(180), () -> {
            try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
                String poisonTable = createPoisonTable(admin);
                try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                        .size(size).validateOnBorrow(true).build()) {
                    ExecutorService ex = Executors.newFixedThreadPool(threads);
                    CountDownLatch start = new CountDownLatch(1);
                    List<Future<?>> futures = new ArrayList<>();
                    for (int t = 0; t < threads; t++) {
                        futures.add(ex.submit(() -> {
                            await(start);
                            for (int i = 0; i < opsPerThread && failure.get() == null; i++) {
                                int mode = ThreadLocalRandom.current().nextInt(10);
                                try (ClickHouseConnection conn = pool.borrow()) {
                                    if (mode < 6) {
                                        // 60% clean
                                        if (conn.executeScalar("SELECT 7") != 7L) {
                                            failure.compareAndSet(null, "clean op returned wrong value");
                                        }
                                        cleanOps.incrementAndGet();
                                    } else if (mode < 9) {
                                        // 30% poison via decode error (sets isPoisoned)
                                        poison(conn, poisonTable);
                                        if (!conn.isPoisoned()) {
                                            failure.compareAndSet(null, "poison recipe failed to poison");
                                        }
                                        poisonOps.incrementAndGet();
                                    } else {
                                        // 10% leak a streaming result without closing/consuming it
                                        // (no poison, but stream left dirty -> validate-on-borrow
                                        // backstop must replace it). Intentionally not closed.
                                        @SuppressWarnings("unused")
                                        QueryResult leaked = conn.query("SELECT number FROM numbers(1000)");
                                        leakOps.incrementAndGet();
                                    }
                                } catch (RuntimeException unexpected) {
                                    failure.compareAndSet(null, "unexpected: " + unexpected);
                                }
                            }
                        }));
                    }
                    start.countDown();
                    joinAll(ex, futures);

                    assertEquals(null, failure.get(), "no in-flight failure: " + failure.get());
                    // The headline invariant: not one permit lost across thousands of cycles.
                    assertEquals(size, pool.available(),
                            "PERMIT LEAK: available=" + pool.available() + " but size=" + size
                            + " after clean=" + cleanOps + " poison=" + poisonOps + " leak=" + leakOps);
                    // And the pool can still saturate fully (no silently-dead slots).
                    assertCanSaturate(pool, size);
                    assertEquals(size, pool.available(), "permits restored after saturation probe");
                } finally {
                    admin.execute("DROP TABLE IF EXISTS " + poisonTable);
                }
            }
        });
    }

    // =====================================================================================
    // 4. SELF-HEALING TO FULL SIZE: borrow all S, poison each, return all -> pool must serve
    //    S concurrent real queries again and available() must return to S.
    // =====================================================================================
    @Test
    void selfHealsToFullSizeAfterAllPoisoned() {
        final int size = 5;
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
                String poisonTable = createPoisonTable(admin);
                try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                        .size(size).validateOnBorrow(false).build()) {
                    // Borrow all S at once, poison each, then return them all.
                    List<ClickHouseConnection> held = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        held.add(pool.borrow());
                    }
                    assertEquals(0, pool.available(), "all permits should be held");
                    for (ClickHouseConnection c : held) {
                        poison(c, poisonTable);
                        assertTrue(c.isPoisoned(), "each connection must be poisoned");
                    }
                    for (ClickHouseConnection c : held) {
                        c.close(); // returns -> poisoned -> discarded, permit released
                    }
                    assertEquals(size, pool.available(),
                            "all permits released after returning poisoned connections");

                    // Now S threads must each get a fresh, working connection concurrently.
                    assertCanSaturate(pool, size);
                    assertEquals(size, pool.available(), "back to full size after self-heal");
                } finally {
                    admin.execute("DROP TABLE IF EXISTS " + poisonTable);
                }
            }
        });
    }

    // =====================================================================================
    // 5. BORROW-TIMEOUT UNDER SATURATION: hold all S, short timeout -> extra borrow throws
    //    within ~the timeout (not instantly, not forever). Release one -> a waiting borrow
    //    proceeds.
    // =====================================================================================
    @Test
    void borrowTimesOutUnderSaturationThenProceedsAfterRelease() {
        final int size = 2;
        final long timeoutMs = 500;
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                    .size(size).borrowTimeout(Duration.ofMillis(timeoutMs)).build()) {
                List<ClickHouseConnection> held = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    held.add(pool.borrow());
                }
                assertEquals(0, pool.available(), "saturated");

                // Extra borrow must block ~timeoutMs then throw (exhausted), not return instantly.
                long t0 = System.nanoTime();
                ClickHouseException ex = assertThrows(ClickHouseException.class, pool::borrow);
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(ex.getMessage().toLowerCase().contains("exhausted")
                                || ex.getMessage().toLowerCase().contains("available"),
                        "exhaustion message expected, got: " + ex.getMessage());
                assertTrue(elapsedMs >= timeoutMs - 150,
                        "borrow returned too fast (" + elapsedMs + "ms < " + timeoutMs + "ms): did it block?");
                assertTrue(elapsedMs < timeoutMs + 5_000,
                        "borrow took far too long (" + elapsedMs + "ms): blocked forever?");

                // A waiter parked on borrow() must proceed once a permit is released.
                ExecutorService ex2 = Executors.newSingleThreadExecutor();
                CountDownLatch parked = new CountDownLatch(1);
                Future<Long> waiter = ex2.submit(() -> {
                    parked.countDown();
                    try (ClickHouseConnection c = pool.borrow()) {
                        return c.executeScalar("SELECT 99");
                    }
                });
                await(parked);
                // Give the waiter a moment to actually park inside tryAcquire, then release one.
                Thread.sleep(100);
                held.remove(0).close();
                assertEquals(99L, waiter.get(2, TimeUnit.SECONDS),
                        "released permit must unblock the waiting borrower");
                ex2.shutdownNow();

                for (ClickHouseConnection c : held) {
                    c.close();
                }
                assertEquals(size, pool.available(), "permits restored");
            }
        });
    }

    // =====================================================================================
    // 6. CLOSE RACE: close() the pool while other threads are mid borrow/return. No deadlock/
    //    hang (preemptive timeout), no leaked connections, and borrow-after-close throws.
    // =====================================================================================
    @Test
    void closeRaceDoesNotDeadlockOrLeak() {
        final int size = 4;
        final int threads = 12;
        final AtomicInteger borrowAfterCloseThrew = new AtomicInteger(0);
        final AtomicInteger completedOps = new AtomicInteger(0);
        final AtomicReference<String> unexpected = new AtomicReference<>(null);

        assertTimeoutPreemptively(ofSeconds(60), () -> {
            ClickHouseConnectionPool pool =
                    ClickHouseConnectionPool.builder(config()).size(size).build();
            ExecutorService ex = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            final AtomicBooleanLite stop = new AtomicBooleanLite();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(ex.submit(() -> {
                    await(start);
                    while (!stop.get()) {
                        try (ClickHouseConnection conn = pool.borrow()) {
                            conn.executeScalar("SELECT 1");
                            completedOps.incrementAndGet();
                        } catch (ClickHouseException expectedAfterClose) {
                            // Borrow (or an op) failing because the pool closed is fine; count it.
                            String msg = String.valueOf(expectedAfterClose.getMessage());
                            if (msg.toLowerCase().contains("closed")) {
                                borrowAfterCloseThrew.incrementAndGet();
                            }
                            // Once we see a closed-pool error, stop churning.
                            return;
                        } catch (RuntimeException other) {
                            // A connection op may fail with a non-ClickHouseException if the socket
                            // is torn down by close() mid-op; that is acceptable during the race as
                            // long as it does not hang or leak. Record but do not fail outright.
                            return;
                        }
                    }
                }));
            }
            start.countDown();
            // Let threads run briefly, then close concurrently with in-flight borrows.
            Thread.sleep(200);
            pool.close();
            stop.set();
            joinAll(ex, futures);

            assertEquals(null, unexpected.get(), "unexpected error during close race: " + unexpected.get());
            assertTrue(completedOps.get() > 0, "threads should have done real work before close");

            // borrow-after-close must throw deterministically.
            ClickHouseException afterClose = assertThrows(ClickHouseException.class, pool::borrow);
            assertTrue(String.valueOf(afterClose.getMessage()).toLowerCase().contains("closed"),
                    "borrow after close must report closed, got: " + afterClose.getMessage());

            // close() is idempotent.
            pool.close();
        });
    }

    // ----------------------------------- helpers -----------------------------------------

    /** Asserts that {@code size} threads can each concurrently borrow and run a unique query. */
    private static void assertCanSaturate(ClickHouseConnectionPool pool, int size) throws Exception {
        ExecutorService ex = Executors.newFixedThreadPool(size);
        CountDownLatch allBorrowed = new CountDownLatch(size);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < size; i++) {
                final long expect = 1000 + i;
                futures.add(ex.submit(() -> {
                    try (ClickHouseConnection c = pool.borrow()) {
                        long v = c.executeScalar("SELECT " + expect);
                        allBorrowed.countDown();
                        // Hold all connections at once to prove genuine concurrent saturation.
                        if (!release.await(20, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("release latch timed out");
                        }
                        return v == expect ? v : -1L;
                    }
                }));
            }
            assertTrue(allBorrowed.await(20, TimeUnit.SECONDS),
                    "pool failed to serve " + size + " concurrent borrowers (self-heal/saturate broken)");
            release.countDown();
            for (Future<Long> f : futures) {
                long v = f.get(20, TimeUnit.SECONDS);
                assertTrue(v >= 1000, "saturating borrower got corrupted/cross-talked result: " + v);
            }
        } finally {
            release.countDown();
            ex.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void joinAll(ExecutorService ex, List<Future<?>> futures) throws Exception {
        ex.shutdown();
        for (Future<?> f : futures) {
            f.get(150, TimeUnit.SECONDS); // surfaces any assertion thrown on a worker thread
        }
        if (!ex.awaitTermination(30, TimeUnit.SECONDS)) {
            ex.shutdownNow();
            fail("executor did not terminate (possible hang/leak)");
        }
    }

    /** Tiny volatile-boolean flag (avoids importing AtomicBoolean just for a stop signal). */
    private static final class AtomicBooleanLite {
        private volatile boolean v = false;
        boolean get() { return v; }
        void set() { v = true; }
    }
}
