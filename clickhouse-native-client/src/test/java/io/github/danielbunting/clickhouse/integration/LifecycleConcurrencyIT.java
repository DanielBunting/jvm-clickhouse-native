package io.github.danielbunting.clickhouse.integration;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ConcurrentConnectionUseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Adversarial lifecycle/concurrency tests for a single {@link ClickHouseConnection},
 * its {@code ConnectionGuard}, and the operation lifecycle of {@code QueryResult},
 * {@code query(sql, Class)} streams, {@code queryAsync}, and {@code BulkInserter}.
 *
 * <p>Hunts five invariant families (see method groupings below):
 * <ol>
 *   <li><b>Guard rejection must not poison.</b> A rejected concurrent op never touched
 *       the wire, so the connection must stay un-poisoned and fully usable.</li>
 *   <li><b>queryAsync lifecycle.</b> A future yields a usable result; the guard is
 *       released on result close; a bad-SQL async completes exceptionally without
 *       poisoning.</li>
 *   <li><b>Stream releases guard on close.</b> Full consume and early (partial) close
 *       both free the connection.</li>
 *   <li><b>BulkInserter lifecycle.</b> init→add→complete frees the guard for reuse;
 *       double-close is idempotent; using the connection while an inserter is open
 *       rejects; complete()+close() does not poison.</li>
 *   <li><b>Idempotent / ordering edge cases.</b> ops after close(), exactly-once guard
 *       release on the exception path.</li>
 * </ol>
 *
 * <p>Distinct from {@link ConnectionGuardIT} (same-thread overlap + second-thread
 * while streaming) — this goes after poisoning, async, partial-close and bulk
 * lifecycle, none of which that file touches.
 */
@Tag("integration")
class LifecycleConcurrencyIT extends IntegrationTestBase {

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    private static String uniqueTable(String prefix) {
        return prefix + "_" + System.nanoTime();
    }

    /** Seeds a small MergeTree table with {@code n} rows (column {@code n UInt64}). */
    private static void seed(ClickHouseConnection conn, String table, long n) {
        conn.execute("DROP TABLE IF EXISTS " + table);
        conn.execute("CREATE TABLE " + table + " (n UInt64) ENGINE = MergeTree ORDER BY n");
        conn.execute("INSERT INTO " + table + " SELECT number FROM numbers(" + n + ")");
    }

    // ------------------------------------------------------------------
    // 1. GUARD REJECTION MUST NOT POISON
    // ------------------------------------------------------------------

    /** (1a) Two sync ops racing from two threads: one wins, the loser is rejected and the
     * connection is neither poisoned nor corrupted afterward. */
    @Test
    void twoSyncOpsRacing_loserRejected_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_race");
                seed(conn, table, 200_000);

                int threads = 8;
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(threads);
                AtomicInteger rejected = new AtomicInteger();
                AtomicInteger succeeded = new AtomicInteger();
                AtomicReference<Throwable> unexpected = new AtomicReference<>();

                for (int i = 0; i < threads; i++) {
                    Thread t = new Thread(() -> {
                        try {
                            start.await();
                            conn.executeScalar("SELECT count() FROM " + table);
                            succeeded.incrementAndGet();
                        } catch (ConcurrentConnectionUseException e) {
                            rejected.incrementAndGet();
                        } catch (Throwable e) {
                            unexpected.set(e);
                        } finally {
                            done.countDown();
                        }
                    });
                    t.setDaemon(true);
                    t.start();
                }
                start.countDown();
                assertTrue(done.await(20, TimeUnit.SECONDS), "threads did not finish");

                // At least one must have been rejected (otherwise nothing raced) and at least
                // one must have succeeded. The exact split is timing-dependent.
                assertTrue(rejected.get() >= 1,
                        "expected at least one ConcurrentConnectionUseException; got none");
                assertTrue(succeeded.get() >= 1, "expected at least one success");
                if (unexpected.get() != null) {
                    fail("unexpected exception in racing thread", unexpected.get());
                }

                // A rejected op never reached the wire => connection must NOT be poisoned.
                assertFalse(conn.isPoisoned(),
                        "guard-rejected op must not poison the connection");
                // ...and the connection must still serve a real query.
                assertEquals(200_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (1b) queryAsync overlapping an in-flight sync op: one of them is rejected with
     * ConcurrentConnectionUseException, and the connection survives un-poisoned. */
    @Test
    void queryAsyncOverlappingSyncOp_oneRejected_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_async_overlap");
                seed(conn, table, 300_000);

                // Hold the connection with an open, only-partially-consumed stream so the
                // overlap window is deterministic (the guard is held until we close it).
                QueryResult held = conn.query("SELECT n FROM " + table);
                Iterator<Block> it = held.blocks();
                it.hasNext(); // pull first block; connection is mid-stream and guard held

                CompletableFuture<QueryResult> future = conn.queryAsync("SELECT n FROM " + table);

                // The async op must be rejected (guard held by the open stream). It may also
                // race the release if we close concurrently — so close only AFTER asserting.
                ExecutionException ee = assertThrows(ExecutionException.class,
                        () -> future.get(20, TimeUnit.SECONDS));
                assertInstanceOf(ConcurrentConnectionUseException.class, ee.getCause(),
                        "overlapping queryAsync must be rejected by the guard");

                held.close(); // releases the guard

                assertFalse(conn.isPoisoned(),
                        "guard-rejected async op must not poison the connection");
                assertEquals(300_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (1c) Two queryAsync calls fired back-to-back: at most one can hold the connection at a
     * time. Whichever loses is rejected; neither poisons the connection. */
    @Test
    void twoQueryAsync_atMostOneHolds_loserRejected_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_two_async");
                seed(conn, table, 300_000);

                CompletableFuture<QueryResult> f1 = conn.queryAsync("SELECT n FROM " + table);
                CompletableFuture<QueryResult> f2 = conn.queryAsync("SELECT n FROM " + table);

                // Wait for both to settle (success or failure) without throwing yet.
                CompletableFuture<Void> both =
                        CompletableFuture.allOf(f1.handle((r, e) -> null), f2.handle((r, e) -> null));
                both.get(20, TimeUnit.SECONDS);

                List<QueryResult> winners = new ArrayList<>();
                int rejected = 0;
                for (CompletableFuture<QueryResult> f : List.of(f1, f2)) {
                    try {
                        winners.add(f.get(1, TimeUnit.SECONDS));
                    } catch (ExecutionException e) {
                        assertInstanceOf(ConcurrentConnectionUseException.class, e.getCause(),
                                "the losing queryAsync must fail with ConcurrentConnectionUseException");
                        rejected++;
                    }
                }

                // Both could have serialized cleanly (1 acquired+released before the other ran),
                // OR they overlapped and exactly one was rejected. Either is correct; never two
                // winners holding simultaneously without a reject is implied by guard semantics.
                // Close any results we got so the guard is freed.
                for (QueryResult r : winners) {
                    r.close();
                }
                assertTrue(winners.size() + rejected == 2, "every future must settle");

                assertFalse(conn.isPoisoned(),
                        "neither async op should have poisoned (a reject never hits the wire; "
                        + "a clean stream leaves the wire in spec)");
                assertEquals(300_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (1d) A second op attempted while a query(sql, Class) stream is open and only partially
     * consumed is rejected; the connection is not poisoned and is reusable after the stream
     * is closed. */
    @Test
    void secondOpWhileMappedStreamOpen_rejected_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_mapped_open");
                seed(conn, table, 200_000);

                Stream<Row> stream = conn.query("SELECT n FROM " + table, Row.class);
                Iterator<Row> it = stream.iterator();
                assertTrue(it.hasNext());
                it.next(); // consume one row; the rest of the stream stays open, guard held

                assertThrows(ConcurrentConnectionUseException.class,
                        () -> conn.executeScalar("SELECT count() FROM " + table));
                assertFalse(conn.isPoisoned(),
                        "rejected op while a mapped stream is open must not poison");

                stream.close(); // releases the guard (drains the rest)

                assertFalse(conn.isPoisoned());
                assertEquals(200_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    // ------------------------------------------------------------------
    // 2. queryAsync LIFECYCLE
    // ------------------------------------------------------------------

    /** (2) A queryAsync future yields a usable QueryResult; closing it releases the guard and
     * a subsequent sync op on the same connection succeeds. */
    @Test
    void queryAsync_resultUsable_guardReleasedOnClose() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_async_ok");
                seed(conn, table, 5_000);

                CompletableFuture<QueryResult> future = conn.queryAsync("SELECT n FROM " + table);
                QueryResult result = future.get(20, TimeUnit.SECONDS);
                assertNotNull(result);

                long count = 0;
                Iterator<Block> it = result.blocks();
                while (it.hasNext()) {
                    count += it.next().rowCount();
                }
                assertEquals(5_000L, count, "async stream must yield all rows");
                result.close();

                // Guard released -> a subsequent op works without ConcurrentConnectionUseException.
                assertFalse(conn.isPoisoned());
                assertEquals(5_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (2) A queryAsync with bad SQL completes exceptionally with a server EXCEPTION, does NOT
     * poison the connection (clean server-side error keeps the wire in spec), and the
     * connection stays usable. */
    @Test
    void queryAsyncBadSql_completesExceptionally_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                CompletableFuture<QueryResult> future =
                        conn.queryAsync("SELECT this_column_does_not_exist FROM numbers(1)");

                ExecutionException ee = assertThrows(ExecutionException.class,
                        () -> future.get(20, TimeUnit.SECONDS));
                assertInstanceOf(ServerException.class, ee.getCause(),
                        "bad-SQL async must complete with a server EXCEPTION");

                assertFalse(conn.isPoisoned(),
                        "a clean server query exception must NOT poison the connection");
                // Guard must have been released on the exception path => connection reusable.
                assertEquals(1L, conn.executeScalar("SELECT 1"));
            }
        });
    }

    // ------------------------------------------------------------------
    // 3. STREAM RELEASES GUARD ON CLOSE
    // ------------------------------------------------------------------

    /** (3) Fully consume a stream, then close: the next op works (guard released on exhaustion
     * + close, idempotently). */
    @Test
    void streamFullyConsumedThenClosed_nextOpWorks() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_full_consume");
                seed(conn, table, 50_000);

                QueryResult result = conn.query("SELECT n FROM " + table);
                long count = 0;
                Iterator<Block> it = result.blocks();
                while (it.hasNext()) {
                    count += it.next().rowCount();
                }
                assertEquals(50_000L, count);
                result.close(); // idempotent with the on-exhaustion release

                assertFalse(conn.isPoisoned());
                assertEquals(50_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (3) Open a large stream, consume only one block, then close early (partial). Characterize
     * the contract: close() drains the rest off the wire and releases the guard, so the
     * connection must NOT be poisoned and must be fully reusable. If early-close left it
     * silently dirty-but-reusable, the follow-up query below would corrupt or hang — this
     * pins the TRUE behavior. */
    @Test
    void streamPartiallyConsumedThenClosedEarly_connectionStaysUsable() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_partial");
                // Large enough to span many blocks so close() must drain real data.
                seed(conn, table, 1_000_000);

                QueryResult result = conn.query("SELECT n FROM " + table);
                Iterator<Block> it = result.blocks();
                assertTrue(it.hasNext());
                it.next(); // exactly one block; the rest is still on the wire

                result.close(); // must drain the remaining blocks to END_OF_STREAM

                // TRUE behavior: drain-on-close keeps the wire in spec, so NOT poisoned.
                assertFalse(conn.isPoisoned(),
                        "early-close drains the stream, so the connection must not be poisoned");
                // And a real follow-up query must return the correct answer (proving the wire
                // was left at a clean packet boundary, not silently dirty).
                assertEquals(1_000_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (3) Early-close of a mapped query(sql, Class) stream (via try-with-resources / Stream.close)
     * also drains and releases, leaving the connection usable. */
    @Test
    void mappedStreamClosedEarly_connectionStaysUsable() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_mapped_partial");
                seed(conn, table, 1_000_000);

                try (Stream<Row> stream = conn.query("SELECT n FROM " + table, Row.class)) {
                    Iterator<Row> it = stream.iterator();
                    assertTrue(it.hasNext());
                    it.next(); // one row, then leave the stream — close drains the rest
                }

                assertFalse(conn.isPoisoned());
                assertEquals(1_000_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    // ------------------------------------------------------------------
    // 4. BULK INSERTER LIFECYCLE
    // ------------------------------------------------------------------

    record Row(long n) {}

    /** (4) init -> add -> complete, then REUSE the same connection for a query. The guard must be
     * released on complete() so the follow-up query is not rejected. */
    @Test
    void bulkInserterCompleteReleasesGuard_connectionReusable() {
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_bulk_reuse");
                conn.execute("DROP TABLE IF EXISTS " + table);
                conn.execute("CREATE TABLE " + table + " (n UInt64) ENGINE = MergeTree ORDER BY n");

                List<Row> rows = new ArrayList<>();
                for (int i = 0; i < 1_000; i++) {
                    rows.add(new Row(i));
                }
                try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                    inserter.init();
                    inserter.addRange(rows);
                    inserter.complete();
                    // complete() released the guard; using the connection mid-inserter (before
                    // close) must already be permitted now.
                    assertEquals(1_000L, conn.executeScalar("SELECT count() FROM " + table));
                }

                assertFalse(conn.isPoisoned(),
                        "complete()+close() (clean path) must not poison the connection");
                assertEquals(1_000L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (4) Double close() on an inserter (after complete()) is idempotent: no throw, no spurious
     * poison. complete() then close() must not poison. */
    @Test
    void bulkInserterDoubleCloseAfterComplete_idempotent_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_bulk_dblclose");
                conn.execute("DROP TABLE IF EXISTS " + table);
                conn.execute("CREATE TABLE " + table + " (n UInt64) ENGINE = MergeTree ORDER BY n");

                BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class);
                inserter.init();
                inserter.add(new Row(1));
                inserter.add(new Row(2));
                inserter.complete();
                inserter.close();
                inserter.close(); // second close must be a no-op
                inserter.close(); // and a third

                assertFalse(conn.isPoisoned(),
                        "complete() then repeated close() must not poison");
                assertEquals(2L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (4) Using the connection for another op WHILE an inserter is open (init() called, not yet
     * complete()) must be rejected by the guard. After completing the inserter the connection
     * is reusable and not poisoned. */
    @Test
    void opWhileInserterOpen_rejected_thenReusableAfterComplete() {
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                String table = uniqueTable("life_bulk_open");
                conn.execute("DROP TABLE IF EXISTS " + table);
                conn.execute("CREATE TABLE " + table + " (n UInt64) ENGINE = MergeTree ORDER BY n");

                try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                    inserter.init(); // guard now held by the inserter
                    inserter.add(new Row(7));

                    assertThrows(ConcurrentConnectionUseException.class,
                            () -> conn.executeScalar("SELECT 1"),
                            "an op while an inserter is open and uncompleted must be rejected");
                    // The rejected op never hit the wire => inserter can still complete cleanly.
                    inserter.complete();
                }

                assertFalse(conn.isPoisoned());
                assertEquals(1L, conn.executeScalar("SELECT count() FROM " + table));
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    /** (4) An inserter closed WITHOUT complete() (after init started the INSERT) leaves the wire
     * mid-stream; per the documented contract this poisons the connection so a pool discards
     * it. This pins that contract. */
    @Test
    void bulkInserterClosedWithoutComplete_poisons() {
        String table = uniqueTable("life_bulk_abort");
        assertTimeoutPreemptively(ofSeconds(60), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                conn.execute("DROP TABLE IF EXISTS " + table);
                conn.execute("CREATE TABLE " + table + " (n UInt64) ENGINE = MergeTree ORDER BY n");

                BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class);
                inserter.init();
                inserter.add(new Row(1));
                inserter.close(); // abandon WITHOUT complete()

                assertTrue(conn.isPoisoned(),
                        "an inserter abandoned mid-INSERT must poison the connection (wire desynced)");
                // close() still released the guard, so a fresh acquire does not deadlock —
                // it fails fast on the (now desynced) connection rather than hanging. Reading
                // the leftover INSERT-stream state surfaces as a protocol/EOF error.
                assertThrows(RuntimeException.class,
                        () -> conn.executeScalar("SELECT 1"),
                        "a poisoned (mid-INSERT) connection must not silently succeed");
                // No cleanup query here: the wire is desynced, so any further op on THIS
                // connection would also fail. Drop the table over a fresh connection.
            }
            try (ClickHouseConnection cleanup = ClickHouseConnection.open(config())) {
                cleanup.execute("DROP TABLE IF EXISTS " + table);
            }
        });
    }

    // ------------------------------------------------------------------
    // 5. IDEMPOTENT / ORDERING EDGE CASES
    // ------------------------------------------------------------------

    /** (5) Operations after close() must fail fast (closed connection), not hang or silently
     * succeed. */
    @Test
    void opsAfterClose_failFast() {
        assertTimeoutPreemptively(ofSeconds(20), () -> {
            ClickHouseConnection conn = ClickHouseConnection.open(config());
            assertEquals(1L, conn.executeScalar("SELECT 1"));
            conn.close();

            assertThrows(RuntimeException.class, () -> conn.executeScalar("SELECT 1"),
                    "executeScalar after close() must fail fast");
            assertThrows(RuntimeException.class, () -> conn.execute("SELECT 1"),
                    "execute after close() must fail fast");
            assertThrows(RuntimeException.class, () -> conn.query("SELECT 1"),
                    "query after close() must fail fast");
        });
    }

    /** (5) close() of a connection mid-stream must not hang. The open result is abandoned; the
     * socket is torn down. (Closing the QueryResult afterwards may then error, which is fine —
     * the point is that connection.close() itself returns promptly.) */
    @Test
    void closeConnectionMidStream_doesNotHang() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            ClickHouseConnection conn = ClickHouseConnection.open(config());
            String table = uniqueTable("life_close_midstream");
            seed(conn, table, 500_000);

            QueryResult result = conn.query("SELECT n FROM " + table);
            Iterator<Block> it = result.blocks();
            assertTrue(it.hasNext());
            it.next(); // mid-stream

            conn.close(); // must return promptly even with the socket mid-stream

            // Draining a now-closed connection must fail fast, not hang. Either a clean
            // close-drain (no further reads needed) or a connection error is acceptable;
            // we only require it does not block. Swallow whatever it throws.
            try {
                result.close();
            } catch (RuntimeException ignored) {
                // expected: reading from a closed socket fails fast
            }
        });
    }

    /** (5) The guard is released exactly once even when the streaming result hits a server
     * EXCEPTION mid-iteration: the failing iterator releases the guard, the connection is not
     * poisoned (clean server error), and the connection is reusable. Uses throwIf to force a
     * server-side exception partway through producing rows. */
    @Test
    void streamServerExceptionMidIteration_releasesGuardOnce_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                // throwIf(cond, msg) raises a server exception once `number` reaches the
                // threshold, after the first block(s) have already streamed back.
                QueryResult result = conn.query(
                        "SELECT throwIf(number >= 500000, 'boom') AS n FROM numbers(1000000)");
                ServerException thrown = null;
                try {
                    Iterator<Block> it = result.blocks();
                    while (it.hasNext()) {
                        it.next();
                    }
                    fail("expected a server exception mid-stream");
                } catch (ServerException e) {
                    thrown = e;
                }
                assertNotNull(thrown, "throwIf must surface as a ServerException");

                // The iterator's terminal EXCEPTION path runs markReleased() exactly once.
                // A clean server error keeps the wire in spec => not poisoned, and reusable.
                assertFalse(conn.isPoisoned(),
                        "a server exception mid-stream must not poison the connection");

                // Closing after the exception is idempotent (guard already released) and safe.
                result.close();

                assertEquals(42L, conn.executeScalar("SELECT 42"),
                        "connection must be reusable after a mid-stream server exception");
            }
        });
    }

    /** (5) executeScalar against a clean server error (bad SQL) does not poison and releases the
     * guard — back-to-back failing then succeeding calls all work on the same connection. */
    @Test
    void syncBadSqlReleasesGuard_notPoisoned() {
        assertTimeoutPreemptively(ofSeconds(20), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                for (int i = 0; i < 3; i++) {
                    assertThrows(ServerException.class,
                            () -> conn.executeScalar("SELECT nope_no_such_col FROM numbers(1)"));
                    assertFalse(conn.isPoisoned(),
                            "a clean server error must not poison (iteration " + i + ")");
                }
                // After repeated failures the guard is free and the wire is in spec.
                assertEquals(1L, conn.executeScalar("SELECT 1"));
            }
        });
    }
}
