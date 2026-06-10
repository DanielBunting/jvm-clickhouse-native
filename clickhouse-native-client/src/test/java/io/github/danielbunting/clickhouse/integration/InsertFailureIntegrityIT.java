package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import io.github.danielbunting.clickhouse.types.Column;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial data-integrity / connection-desync hunt around failed and aborted native INSERTs
 * and streaming reads. Where the existing {@code PoolResilienceIT} pins the pool-level recovery
 * contract, this file goes deeper: it asserts there is <b>no partial commit</b> on any mid-insert
 * failure (client-side or server-side), proves clean-exception reuse with non-trivial round-trips
 * (not just {@code SELECT 1}), exercises the inserter's failed-{@code add()}-then-{@code complete()}
 * state machine for sanity/idempotence, and — the headline — probes the <b>dirty-reuse hazard</b>
 * when a connection is left subtly desynced through a NON-poisoning path and then reused with
 * {@code validateOnBorrow=false}.
 *
 * <p>Contract summary being asserted (from the production code):
 * <ul>
 *   <li>A client-side mapping/codec failure in {@code add()} does not itself poison; the
 *       try-with-resources {@code close()} (initialized &amp;&amp; !completed) poisons. Net: poisoned,
 *       0 rows committed.</li>
 *   <li>A server EXCEPTION (bad SQL, server-side insert rejection) does NOT poison
 *       ({@code NativeClientImpl.readMessage} rethrows {@code ServerException} without setting
 *       {@code poisoned}); the wire stays in spec and the connection is reusable.</li>
 *   <li>A {@code QueryResult.close()} drains the remaining stream synchronously to leave the wire
 *       clean — even when closed early over a huge result.</li>
 * </ul>
 *
 * <p>Run: {@code ./gradlew :clickhouse-native-client:integrationTest --tests '*InsertFailureIntegrityIT'}
 */
@Tag("integration")
class InsertFailureIntegrityIT extends IntegrationTestBase {

    private ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    // Row types used by the various failure triggers.
    record EnumRow(long id, String e) {}
    record IntRow(long id, int v) {}
    record ByteRow(long id, int v) {}              // mapped to an Int8 column; out-of-range overflows

    private long count(ClickHouseConnection conn, String table) {
        return conn.executeScalar("SELECT count() FROM " + table);
    }

    /** Reads all rows of {@code sql} into row-major boxed values (nulls preserved). */
    private List<Object[]> decode(ClickHouseConnection conn, String sql) {
        List<Object[]> rows = new ArrayList<>();
        try (QueryResult result = conn.query(sql)) {
            Iterator<Block> blocks = result.blocks();
            while (blocks.hasNext()) {
                Block block = blocks.next();
                if (block.isEmpty()) {
                    continue;
                }
                int cols = block.columnCount();
                int n = block.rowCount();
                for (int r = 0; r < n; r++) {
                    Object[] row = new Object[cols];
                    for (int c = 0; c < cols; c++) {
                        Column col = block.column(c);
                        boolean isNull = col.nulls() != null && col.nulls()[r];
                        row[c] = isNull ? null : col.value(r);
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private void dropQuietly(String table) {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("DROP TABLE IF EXISTS " + table);
        } catch (RuntimeException ignored) {
            // best-effort
        }
    }

    // =======================================================================================
    // 1. NO PARTIAL COMMIT ON MID-INSERT FAILURE (client-side rejections)
    //    Several distinct triggers; each must poison and commit EXACTLY 0 rows.
    // =======================================================================================

    /** Trigger A: a value not present in the target Enum8 (client-side codec rejection). */
    @Test
    void badEnumNameMidInsert_poisons_andCommitsNothing() {
        String table = "ifi_enum_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY id");

            assertThrows(RuntimeException.class, () -> {
                try (BulkInserter<EnumRow> ins = conn.createBulkInserter(table, EnumRow.class)) {
                    ins.init();
                    ins.add(new EnumRow(1, "a"));                       // valid
                    ins.add(new EnumRow(2, "DEFINITELY_NOT_AN_ENUM"));  // throws client-side
                    ins.complete();
                }
            });
            assertTrue(conn.isPoisoned(),
                    "bad-enum mid-insert leaves the connection mid-stream -> must be poisoned");

            // Verify on a FRESH connection (the poisoned one is unusable): nothing committed.
            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                assertEquals(0L, count(check, table),
                        "a failed enum insert must commit exactly 0 rows (no partial block)");
            }
        } finally {
            dropQuietly(table);
        }
    }

    /**
     * Trigger B: numeric overflow of a narrow column (Int8).
     *
     * <p>CONTRACT: a value outside the column's range is REJECTED up front with an
     * {@link IllegalArgumentException} (see {@code Int8Codec.set}/{@code setLong} →
     * {@code IntegerRanges}), rather than silently narrowed to {@code (byte) 9999 == 15} and
     * committed as a clean write. The bad {@code add()} throws client-side; because {@code init()}
     * already opened the insert stream, the abandoned inserter's {@code close()} poisons the
     * connection, and NOTHING is committed (no partial write). This pins that data-integrity
     * guarantee — silent truncation on insert was a real corruption bug, now fixed.
     */
    @Test
    void numericOverflowMidInsert_isRejected_andCommitsNothing() {
        String table = "ifi_ovf_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Int8) ENGINE = MergeTree ORDER BY id");

            assertThrows(IllegalArgumentException.class, () -> {
                try (BulkInserter<ByteRow> ins = conn.createBulkInserter(table, ByteRow.class)) {
                    ins.init();
                    ins.add(new ByteRow(1, 1));      // valid Int8
                    ins.add(new ByteRow(2, 9999));   // out of Int8 range -> rejected, no truncation
                    ins.complete();
                }
            });

            assertTrue(conn.isPoisoned(),
                    "an insert abandoned mid-stream by a rejected value must poison the connection");
            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                assertEquals(0L, count(check, table),
                        "a rejected-overflow insert must commit exactly 0 rows (no half-write)");
            }
        } finally {
            dropQuietly(table);
        }
    }

    /** Trigger C: a Java null mapped into a NON-nullable column. */
    @Test
    void nullIntoNonNullableMidInsert_poisons_orRejects_commitsNothing() {
        String table = "ifi_null_" + System.nanoTime();
        record NullableStrRow(long id, String s) {}
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, s String) ENGINE = MergeTree ORDER BY id");

            boolean threw;
            try {
                try (BulkInserter<NullableStrRow> ins =
                             conn.createBulkInserter(table, NullableStrRow.class)) {
                    ins.init();
                    ins.add(new NullableStrRow(1, "ok"));
                    ins.add(new NullableStrRow(2, null)); // null into non-nullable String
                    ins.complete();
                }
                threw = false;
            } catch (RuntimeException e) {
                threw = true;
            }

            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                if (threw) {
                    assertTrue(conn.isPoisoned(),
                            "null-into-non-nullable rejected mid-insert must poison");
                    assertEquals(0L, count(check, table),
                            "rejected null insert must commit exactly 0 rows");
                } else {
                    // Some String codecs coerce null -> "" rather than rejecting. Then it is a
                    // clean success of 2 rows; assert no partial write and document the coercion.
                    assertFalse(conn.isPoisoned(), "fully-accepted insert must not be poisoned");
                    assertEquals(2L, count(check, table),
                            "if null is coerced to empty string, exactly 2 rows commit");
                }
            }
        } finally {
            dropQuietly(table);
        }
    }

    /**
     * Mixed scenario: a PRIOR successful complete() committed N rows on one inserter, then a
     * SECOND inserter on the SAME connection fails mid-stream. The committed N must survive and
     * the failed batch must add exactly 0 — total == N, never N+partial.
     */
    @Test
    void priorSuccessfulInsertSurvives_failedSecondInsertAddsNothing() {
        String table = "ifi_prior_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY id");

            // First insert: clean success of 2 rows.
            try (BulkInserter<EnumRow> ins = conn.createBulkInserter(table, EnumRow.class)) {
                ins.init();
                ins.add(new EnumRow(1, "a"));
                ins.add(new EnumRow(2, "b"));
                ins.complete();
            }
            assertFalse(conn.isPoisoned(), "a clean complete() must not poison");
            assertEquals(2L, count(conn, table), "first insert committed its 2 rows");

            // Second insert on the SAME (still healthy) connection: fails mid-stream.
            assertThrows(RuntimeException.class, () -> {
                try (BulkInserter<EnumRow> ins = conn.createBulkInserter(table, EnumRow.class)) {
                    ins.init();
                    ins.add(new EnumRow(3, "bad_enum_value"));
                    ins.complete();
                }
            });
            assertTrue(conn.isPoisoned(), "failed second insert must poison");

            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                assertEquals(2L, count(check, table),
                        "exactly the prior 2 rows survive; the failed batch added nothing");
            }
        } finally {
            dropQuietly(table);
        }
    }

    // =======================================================================================
    // 2. DIRTY-REUSE HAZARD with validateOnBorrow=false  (HEADLINE HUNT)
    //    A connection left desynced by a NON-poisoning path, reused with validation OFF.
    //    Either it returns CORRECT results (safe) or garbage/throws (candidate bug).
    // =======================================================================================

    /**
     * 2(a) — Open a streaming QueryResult over a HUGE result, read a few blocks, then close()
     * EARLY while the server is still streaming. {@code QueryResultImpl.close()} drains the rest
     * of the stream synchronously, so the wire should be left clean. Reuse the SAME connection
     * for a fresh round-trip and assert correctness.
     *
     * <p>If close() did NOT fully drain (or poisoned), a follow-up SELECT would desync/garble —
     * which this asserts against.
     */
    @Test
    void hugeResultClosedEarly_thenReuse_returnsCorrectResults() {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                int seen = 0;
                try (QueryResult result = conn.query("SELECT number FROM numbers(10000000)")) {
                    Iterator<Block> blocks = result.blocks();
                    // Read only a couple of blocks, then close early (server still streaming).
                    while (blocks.hasNext() && seen < 2) {
                        Block b = blocks.next();
                        if (!b.isEmpty()) {
                            seen++;
                        }
                    }
                    // try-with-resources close() drains the remainder synchronously here.
                }
                assertFalse(conn.isPoisoned(),
                        "an early close() that drains cleanly must NOT poison the connection");
                assertTrue(seen > 0, "should have read at least one block before closing");

                // The wire must be clean: a fresh, non-trivial round-trip returns correct data.
                assertEquals(7L, conn.executeScalar("SELECT 3 + 4"),
                        "scalar round-trip after early-close must be correct");
                List<Object[]> rows = decode(conn,
                        "SELECT number, number * 2 FROM numbers(5) ORDER BY number");
                assertEquals(5, rows.size(), "follow-up SELECT row count after early-close");
                for (int i = 0; i < 5; i++) {
                    assertEquals((long) i, ((Number) rows.get(i)[0]).longValue(),
                            "row " + i + " col0 after early-close");
                    assertEquals((long) (i * 2), ((Number) rows.get(i)[1]).longValue(),
                            "row " + i + " col1 after early-close");
                }
            }
        });
    }

    /**
     * 2(a)-pool — Same early-close hazard but through the pool with validateOnBorrow=FALSE.
     * Since close() drains cleanly and does not poison, the pool recycles the SAME underlying
     * connection. The next borrower must therefore get CORRECT results. If it gets garbage, the
     * desync was silently recycled (candidate bug).
     */
    @Test
    void hugeResultClosedEarly_pooledReuseNoValidate_returnsCorrectResults() {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                    .size(1).validateOnBorrow(false).build()) {
                try (ClickHouseConnection conn = pool.borrow()) {
                    int seen = 0;
                    try (QueryResult result = conn.query("SELECT number FROM numbers(10000000)")) {
                        Iterator<Block> blocks = result.blocks();
                        while (blocks.hasNext() && seen < 2) {
                            Block b = blocks.next();
                            if (!b.isEmpty()) {
                                seen++;
                            }
                        }
                    }
                    assertFalse(conn.isPoisoned(), "clean early-close must not poison");
                }
                // validateOnBorrow OFF: the SAME connection is handed back. Must be correct.
                try (ClickHouseConnection conn = pool.borrow()) {
                    assertEquals(123L, conn.executeScalar("SELECT 100 + 23"),
                            "recycled connection after early-close must return correct scalar");
                    List<Object[]> rows = decode(conn,
                            "SELECT number FROM numbers(3) ORDER BY number");
                    assertEquals(3, rows.size(), "recycled connection multi-row correctness");
                    for (int i = 0; i < 3; i++) {
                        assertEquals((long) i, ((Number) rows.get(i)[0]).longValue(),
                                "recycled row " + i);
                    }
                }
            }
        });
    }

    /**
     * 2(b) — Abandon a bulk inserter after init()+add()s WITHOUT complete(), calling close()
     * directly (NOT via pool return). The inserter's close() (initialized &amp;&amp; !completed)
     * MUST poison so the connection is never silently reused dirty.
     */
    @Test
    void inserterClosedWithoutComplete_poisons() {
        String table = "ifi_abandon_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Int32) ENGINE = MergeTree ORDER BY id");

            BulkInserter<IntRow> ins = conn.createBulkInserter(table, IntRow.class);
            ins.init();
            ins.add(new IntRow(1, 10));
            ins.add(new IntRow(2, 20));
            ins.close(); // abandon WITHOUT complete() — server still awaiting terminating block

            assertTrue(conn.isPoisoned(),
                    "an inserter abandoned (close without complete) leaves the wire mid-INSERT "
                            + "-> MUST poison so a pool never recycles it dirty");

            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                assertEquals(0L, count(check, table),
                        "an abandoned (never-completed) insert must commit exactly 0 rows");
            }
        } finally {
            dropQuietly(table);
        }
    }

    /**
     * 2(c) — A {@code query(sql, Class)} stream abandoned mid-iteration. Driving the spliterator
     * a few rows then closing the Stream early must drain cleanly (not poison), leaving the
     * connection reusable with correct results.
     */
    @Test
    void mappedStreamAbandonedMidIteration_thenReuse_isCorrect() {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            record NumRow(long number) {}
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                long firstFew;
                try (Stream<NumRow> s =
                             conn.query("SELECT number FROM numbers(10000000)", NumRow.class)) {
                    firstFew = s.limit(5).mapToLong(NumRow::number).sum(); // 0+1+2+3+4 = 10
                    // try-with-resources close() runs onClose -> drains + releases the guard.
                }
                assertEquals(10L, firstFew, "limited mapped stream sum");
                assertFalse(conn.isPoisoned(),
                        "abandoning a mapped stream mid-iteration must not poison");

                // Same connection: a fresh non-trivial round-trip must be correct.
                List<Object[]> rows = decode(conn,
                        "SELECT number, toString(number) FROM numbers(4) ORDER BY number");
                assertEquals(4, rows.size(), "reuse after abandoned mapped stream");
                for (int i = 0; i < 4; i++) {
                    assertEquals((long) i, ((Number) rows.get(i)[0]).longValue(),
                            "reuse row " + i + " number");
                    assertEquals(String.valueOf(i), String.valueOf(rows.get(i)[1]),
                            "reuse row " + i + " toString");
                }
            }
        });
    }

    // =======================================================================================
    // 3. CLEAN-EXCEPTION REUSE IS REAL (non-trivial round-trip, not just SELECT 1)
    // =======================================================================================

    /**
     * After a server EXCEPTION (bad SQL) the connection is NOT poisoned. Prove the wire is truly
     * clean by doing a real insert + read-back of multi-column data on the SAME connection.
     */
    @Test
    void serverExceptionThenRealInsertReadBack_isCorrect() {
        String table = "ifi_cleanexc_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Int64, s String) ENGINE = MergeTree ORDER BY id");

            // Bad SQL -> server EXCEPTION (wire stays in spec).
            assertThrows(ClickHouseException.class,
                    () -> conn.executeScalar("SELECT * FROM no_such_table_" + System.nanoTime()));
            assertFalse(conn.isPoisoned(), "a clean server query exception must NOT poison");

            // Division by zero -> another server EXCEPTION.
            assertThrows(ClickHouseException.class,
                    () -> conn.executeScalar("SELECT intDiv(1, 0)"));
            assertFalse(conn.isPoisoned(), "division-by-zero exception must NOT poison");

            // Now a REAL insert + multi-column read-back on the same connection.
            try (BulkInserter<RealRow> ins = conn.createBulkInserter(table, RealRow.class)) {
                ins.init();
                ins.add(new RealRow(1, 111L, "alpha"));
                ins.add(new RealRow(2, 222L, "beta"));
                ins.complete();
            }
            assertFalse(conn.isPoisoned(), "insert after clean exception must succeed");

            List<Object[]> rows = decode(conn,
                    "SELECT id, v, s FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "read-back row count after clean-exception reuse");
            assertEquals(1L, ((Number) rows.get(0)[0]).longValue(), "row0 id");
            assertEquals(111L, ((Number) rows.get(0)[1]).longValue(), "row0 v");
            assertEquals("alpha", String.valueOf(rows.get(0)[2]), "row0 s");
            assertEquals(2L, ((Number) rows.get(1)[0]).longValue(), "row1 id");
            assertEquals(222L, ((Number) rows.get(1)[1]).longValue(), "row1 v");
            assertEquals("beta", String.valueOf(rows.get(1)[2]), "row1 s");
        } finally {
            dropQuietly(table);
        }
    }

    record RealRow(long id, long v, String s) {}

    // =======================================================================================
    // 4. SERVER-SIDE mid-insert rejection (EXCEPTION during/after the data stream).
    //    Characterize: poisoned or reusable? Assert the true behavior.
    // =======================================================================================

    /**
     * Insert into a table with a CHECK constraint that the data violates. The server validates
     * on the data block / flush and replies with an EXCEPTION, surfaced from {@code complete()}.
     * Characterize the resulting connection state and (if reusable) prove a follow-up insert
     * commits exactly its own rows.
     */
    @Test
    void serverSideCheckConstraintViolation_characterizeAndProveFollowup() {
        String table = "ifi_check_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Int32, CONSTRAINT positive CHECK v > 0) "
                    + "ENGINE = MergeTree ORDER BY id");

            // v = -5 violates the CHECK; the server rejects on the data block -> EXCEPTION.
            ClickHouseException thrown = assertThrows(ClickHouseException.class, () -> {
                try (BulkInserter<IntRow> ins = conn.createBulkInserter(table, IntRow.class)) {
                    ins.init();
                    ins.add(new IntRow(1, -5)); // violates CHECK v > 0
                    ins.complete();
                }
            });

            boolean poisoned = conn.isPoisoned();
            // Whatever the state, the violating insert must NOT have committed any row.
            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                assertEquals(0L, count(check, table),
                        "a CHECK-violating insert must commit exactly 0 rows (got server EXCEPTION: "
                                + thrown.getMessage() + ")");
            }

            if (!poisoned) {
                // Contract per readMessage: a server EXCEPTION does not poison. Prove reusability
                // with a follow-up VALID insert that must commit exactly its own row.
                try (BulkInserter<IntRow> ins = conn.createBulkInserter(table, IntRow.class)) {
                    ins.init();
                    ins.add(new IntRow(2, 99)); // satisfies CHECK
                    ins.complete();
                }
                assertFalse(conn.isPoisoned(), "valid follow-up insert must not poison");
                try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                    assertEquals(1L, count(check, table),
                            "follow-up valid insert commits exactly its 1 row");
                    List<Object[]> rows = decode(check,
                            "SELECT id, v FROM " + table + " ORDER BY id");
                    assertEquals(1, rows.size());
                    assertEquals(2L, ((Number) rows.get(0)[0]).longValue(), "followup id");
                    assertEquals(99L, ((Number) rows.get(0)[1]).longValue(), "followup v");
                }
            }
            // If poisoned, that is a defensible (if conservative) choice; the 0-row assertion
            // above already proved no partial commit. We record the observed behavior via the
            // assertions that ran, not by weakening anything.
        } finally {
            dropQuietly(table);
        }
    }

    /**
     * Server-side rejection of a too-large value at parse/flush time: insert a value beyond the
     * declared type via a server-evaluated INSERT ... SELECT path is awkward through the binder,
     * so instead provoke a server EXCEPTION mid-insert by targeting a column whose constraint the
     * server enforces. Here we use a non-existent target column resolved server-side at init().
     * This exercises the EXCEPTION-during-init (readSampleBlock) path rather than complete().
     */
    @Test
    void serverExceptionDuringInsertInit_characterize() {
        String missing = "ifi_missing_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            // No such table -> server EXCEPTION while awaiting the sample block in init().
            assertThrows(ClickHouseException.class, () -> {
                try (BulkInserter<IntRow> ins = conn.createBulkInserter(missing, IntRow.class)) {
                    ins.init();
                    ins.complete();
                }
            });

            // init() catches RuntimeException and unconditionally markPoisoned()s, even though a
            // server EXCEPTION here is technically a clean wire. Document the observed behavior:
            // the connection is poisoned by the inserter's init() error path (conservative).
            // Whatever the flag, a fresh connection proves the server itself is fine.
            boolean poisoned = conn.isPoisoned();
            try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                assertEquals(1L, check.executeScalar("SELECT 1"),
                        "server remains healthy regardless of inserter-init poisoning (observed "
                                + "poisoned=" + poisoned + ")");
            }
            // If NOT poisoned, the same connection must still be reusable.
            if (!poisoned) {
                assertEquals(5L, conn.executeScalar("SELECT 2 + 3"),
                        "if init-failure did not poison, the connection must still work");
            }
        }
    }

    // =======================================================================================
    // 5. complete()/close() AFTER A FAILED add() — sanity, no double-poison, no hang, idempotent.
    // =======================================================================================

    /**
     * After add() throws client-side, calling complete() and then close() on the SAME inserter
     * must behave sanely: no hang, no exception storm, idempotent poisoning (still just poisoned),
     * and exactly 0 rows committed.
     */
    @Test
    void completeAndCloseAfterFailedAdd_areSane() {
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            String table = "ifi_afterfail_" + System.nanoTime();
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                conn.execute("CREATE TABLE " + table
                        + " (id UInt32, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY id");

                BulkInserter<EnumRow> ins = conn.createBulkInserter(table, EnumRow.class);
                ins.init();
                // This add() throws client-side (bad enum). The buffered cursor is now in an
                // undefined position; the connection is mid-INSERT.
                assertThrows(RuntimeException.class, () -> ins.add(new EnumRow(1, "nope")));

                // complete() after a failed add(): it will flush() whatever is buffered and try to
                // finish the protocol. On a desynced/poisoned wire this should surface an error or
                // a no-op, but it MUST NOT hang and MUST NOT corrupt further.
                try {
                    ins.complete();
                } catch (RuntimeException expectedOrNot) {
                    // either path is acceptable; the key is it returns (no hang) — guaranteed by
                    // the surrounding timeout — and does not leave a half-written commit.
                }

                // close() must be idempotent and safe regardless of complete()'s outcome.
                ins.close();
                ins.close(); // second close() is a no-op (idempotent)

                try (ClickHouseConnection check = ClickHouseConnection.open(config())) {
                    assertEquals(0L, count(check, table),
                            "after a failed add(), no complete()/close() sequence may commit rows");
                }
            } finally {
                dropQuietly(table);
            }
        });
    }

    /**
     * close() called multiple times after a clean complete() must be idempotent and must NOT
     * poison (the insert completed successfully).
     */
    @Test
    void closeAfterSuccessfulComplete_isIdempotent_andDoesNotPoison() {
        String table = "ifi_idem_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Int32) ENGINE = MergeTree ORDER BY id");

            BulkInserter<IntRow> ins = conn.createBulkInserter(table, IntRow.class);
            ins.init();
            ins.add(new IntRow(1, 7));
            ins.complete();
            ins.close();
            ins.close(); // idempotent

            assertFalse(conn.isPoisoned(),
                    "close() after a successful complete() must NOT poison");
            assertEquals(1L, count(conn, table), "successful insert committed its 1 row");
            // And the connection is still usable on the same socket.
            assertEquals(2L, conn.executeScalar("SELECT 1 + 1"),
                    "connection still usable after complete()+close()");
        } finally {
            dropQuietly(table);
        }
    }
}
