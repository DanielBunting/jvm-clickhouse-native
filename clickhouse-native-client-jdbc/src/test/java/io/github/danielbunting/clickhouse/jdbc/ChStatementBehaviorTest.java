package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Behavioral unit coverage for {@link ChStatement} ported from the official clickhouse-java
 * suites (v1 {@code ClickHouseStatementTest} and jdbc-v2 {@code StatementTest}): batch
 * semantics on success/failure, the {@code executeLarge*} surface, result-set identity,
 * result-set constants, and knob validation. Complements {@link ChStatementTest} (owned by
 * another change), which covers classification, wrappers and the unsupported-feature matrix.
 *
 * <p>Deliberate/defensible divergences from clickhouse-java are pinned as passing tests
 * with a comment naming the reference expectation. Spec bugs found during porting were
 * documented as failing {@code knownBug_}-prefixed tests and have since been fixed
 * (their tests now carry a "was knownBug N" note).
 */
class ChStatementBehaviorTest {

    /** Records executed/queried SQL; optionally fails {@code execute} on chosen statements. */
    private static final class ScriptedCore extends FakeCore {
        final List<String> executed = new ArrayList<>();
        final List<String> queried = new ArrayList<>();
        String failOn;

        @Override
        public void execute(String sql) {
            if (failOn != null && failOn.equals(sql)) {
                throw new ClickHouseException("scripted failure for: " + sql);
            }
            executed.add(sql);
        }

        @Override
        public QueryResult query(String sql) {
            queried.add(sql);
            return new QueryResult() {
                @Override
                public List<String> columnNames() {
                    return Collections.emptyList();
                }

                @Override
                public List<String> columnTypes() {
                    return Collections.emptyList();
                }

                @Override
                public Iterator<Block> blocks() {
                    return Collections.emptyIterator();
                }

                @Override
                public void close() {
                }
            };
        }
    }

    private static ChStatement connected(ScriptedCore core) {
        ChConnection c = new ChConnection(
                core, "jdbc:chnative://localhost:9000/default", new Properties());
        return new ChStatement(c);
    }

    // ---- executeQuery identity (v1 testExecuteQuery) -------------------------

    @Test
    void executeQueryReturnsSameInstanceAsGetResultSet() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        ResultSet rs = s.executeQuery("SELECT 1");
        assertSame(rs, s.getResultSet(), "executeQuery result must be getResultSet()'s object");
        assertEquals(-1, s.getUpdateCount());
        // A second query replaces the current result set.
        ResultSet rs2 = s.executeQuery("SELECT 2");
        assertSame(rs2, s.getResultSet());
        assertEquals(-1, s.getUpdateCount());
    }

    /**
     * DIVERGENCE (pinned) — jdbc-v2 {@code testExecuteQueryWithNoResultSetWhenExpected} and
     * v2 {@code testUnknownStatement} expect {@code executeQuery} on a non-query to throw.
     * This driver routes {@code executeQuery} straight to the core query path without
     * classifying, so an INSERT/DDL "query" executes and yields an (empty) result set.
     * Live confirmation: {@code JdbcSessionIT#executeQueryOnDdlReturnsEmptyResultSetInsteadOfThrowing}
     * and {@code JdbcStatementIT#oddSqlAndExecuteQueryOnInsert}.
     */
    @Test
    void executeQueryOnNonQueryRoutesToQueryPathAndYieldsResultSet() throws SQLException {
        ScriptedCore core = new ScriptedCore();
        ChStatement s = connected(core);
        ResultSet rs = s.executeQuery("INSERT INTO t VALUES (1)");
        assertNotNull(rs, "should be an SQLException once executeQuery classifies SQL");
        assertEquals(List.of("INSERT INTO t VALUES (1)"), core.queried);
        assertTrue(core.executed.isEmpty());
    }

    // ---- batch semantics (v1 testExecuteBatch / testBatchUpdate,
    //      v2 testExecuteUpdateBatchReuse) ------------------------------------

    @Test
    void emptyBatchExecutesToEmptyArray() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        assertEquals(0, s.executeBatch().length);
        // addBatch + clearBatch leaves nothing to run.
        s.addBatch("INSERT INTO t VALUES (1)");
        s.clearBatch();
        assertEquals(0, s.executeBatch().length);
    }

    /**
     * DIVERGENCE (pinned) — v1 {@code testBatchUpdate}/{@code testExecuteBatch} throw
     * {@code BatchUpdateException} when a query is found in the batch. This driver does not
     * classify batch entries: each one is routed to {@code core().execute(...)} and any
     * result rows are discarded, so a batched SELECT "succeeds".
     */
    @Test
    void queryInBatchIsExecutedNotRejected() throws SQLException {
        ScriptedCore core = new ScriptedCore();
        ChStatement s = connected(core);
        s.addBatch("SELECT 1");
        s.addBatch("INSERT INTO t VALUES (1)");
        int[] counts = s.executeBatch();
        assertEquals(2, counts.length);
        assertEquals(List.of("SELECT 1", "INSERT INTO t VALUES (1)"), core.executed);
        assertTrue(core.queried.isEmpty(), "batch entries ride the execute() path");
    }

    /** Batch results report {@link Statement#SUCCESS_NO_INFO}: the native protocol returns no per-statement counts (reference returns real row counts). */
    @Test
    void batchCountsAreSuccessNoInfo() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        s.addBatch("INSERT INTO t VALUES (1)");
        s.addBatch("INSERT INTO t VALUES (2), (3)");
        int[] counts = s.executeBatch();
        assertEquals(2, counts.length);
        assertEquals(Statement.SUCCESS_NO_INFO, counts[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, counts[1]);
    }

    /**
     * A mid-batch failure surfaces as an {@link SQLException} after executing the
     * earlier entries, and the batch is auto-cleared.
     *
     * <p>DIVERGENCES (pinned, deliberate): per jdbc-v2 {@code testExecuteUpdateBatchReuse}
     * the reference KEEPS the failed batch until {@code clearBatch()}, so a second
     * {@code executeBatch()} fails again; this driver clears the batch in a
     * {@code finally}, so the retry is an empty no-op (documented as intentional in
     * {@code ChJdbcIssuesTest#batchReuseAfterFailure}). There is also no
     * {@code continueBatchOnError} option (v1 {@code JdbcConfig.PROP_CONTINUE_BATCH}).
     * The exception's <em>type</em> is covered separately by
     * {@link #batchFailureThrowsBatchUpdateException()}.
     */
    @Test
    void batchFailureExecutesPrefixAndAutoClears() throws SQLException {
        ScriptedCore core = new ScriptedCore();
        core.failOn = "DROP TABLE non_existing_table";
        ChStatement s = connected(core);
        s.addBatch("INSERT INTO t VALUES (1)");
        s.addBatch("DROP TABLE non_existing_table");
        s.addBatch("INSERT INTO t VALUES (2)");
        assertThrows(SQLException.class, s::executeBatch);
        // Entries before the failure ran; the one after did not (no continue-on-error).
        assertEquals(List.of("INSERT INTO t VALUES (1)"), core.executed);
        // Pinned divergence: the failed batch is auto-cleared, so a retry is a no-op.
        assertEquals(0, s.executeBatch().length);
        // The statement remains usable.
        s.addBatch("INSERT INTO t VALUES (3)");
        assertEquals(1, s.executeBatch().length);
        assertEquals(List.of("INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (3)"),
                core.executed);
    }

    /**
     * A failed batch throws {@link java.sql.BatchUpdateException} — the
     * {@code SQLException} subclass carrying the update counts of the commands that
     * executed before the failure — per the JDBC contract (was knownBug 4; asserted
     * throughout v1 {@code ClickHouseStatementTest#testExecuteBatch}/{@code testBatchUpdate}).
     */
    @Test
    void batchFailureThrowsBatchUpdateException() {
        ScriptedCore core = new ScriptedCore();
        core.failOn = "DROP TABLE non_existing_table";
        ChStatement s = connected(core);
        try {
            s.addBatch("INSERT INTO t VALUES (1)");
            s.addBatch("DROP TABLE non_existing_table");
        } catch (SQLException e) {
            throw new AssertionError("addBatch must not fail", e);
        }
        assertThrows(java.sql.BatchUpdateException.class, s::executeBatch,
                "JDBC requires BatchUpdateException from a failed batch");
    }

    // ---- executeLarge* surface (v1 testExecuteBatch/testExecuteUpdate,
    //      v2 testMaxRows) -----------------------------------------------------
    //
    // ChStatement does not override the JDK's default "large" methods, so the
    // java.sql.Statement defaults apply: most throw UnsupportedOperationException
    // (a RuntimeException, NOT an SQLException — reference drivers implement these
    // and return long counts).

    @Test
    void largeVariantsUseJdkDefaults() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        assertThrows(UnsupportedOperationException.class, s::executeLargeBatch);
        assertThrows(UnsupportedOperationException.class,
                () -> s.executeLargeUpdate("INSERT INTO t VALUES (1)"));
        assertThrows(UnsupportedOperationException.class, s::getLargeUpdateCount);
        assertThrows(UnsupportedOperationException.class, () -> s.setLargeMaxRows(10L));
        // The only large-method the JDK implements: reports 0 (no limit), independent
        // of setMaxRows.
        s.setMaxRows(50);
        assertEquals(0L, s.getLargeMaxRows());
    }

    // ---- result-set constants & simple knobs (v2 testVariousSimpleMethods) ---

    @Test
    void resultSetConstantsMatchForwardOnlyReadOnlyContract() throws SQLException {
        ScriptedCore core = new ScriptedCore();
        ChConnection conn = new ChConnection(
                core, "jdbc:chnative://localhost:9000/default", new Properties());
        ChStatement s = new ChStatement(conn);
        assertEquals(ResultSet.CONCUR_READ_ONLY, s.getResultSetConcurrency());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, s.getResultSetType());
        // Pinned: reports CLOSE_CURSORS_AT_COMMIT (jdbc-v2 reports HOLD_CURSORS_OVER_COMMIT).
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, s.getResultSetHoldability());
        assertSame(conn, s.getConnection());
        assertEquals(0, s.getQueryTimeout(), "query timeout defaults to 0 = unlimited");
        assertEquals(ResultSet.FETCH_FORWARD, s.getFetchDirection());
    }

    /** Pinned: poolable is a stored-nothing no-op (jdbc-v2 round-trips the flag). */
    @Test
    void poolableIsAlwaysFalse() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        assertFalse(s.isPoolable());
        s.setPoolable(true);
        assertFalse(s.isPoolable(), "pinned: setPoolable is ignored");
    }

    /**
     * {@code closeOnCompletion()} is stored — {@code isCloseOnCompletion()} reports
     * {@code true} — and honored: the statement closes when its dependent result set
     * closes, via the {@code ChResultSet.close()} → {@code resultSetClosed} callback
     * (was knownBug 7; jdbc-v2 {@code StatementTest#testCloseOnCompletion} honors it).
     */
    @Test
    void closeOnCompletionIsStoredAndHonored() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        assertFalse(s.isCloseOnCompletion());
        s.closeOnCompletion();
        assertTrue(s.isCloseOnCompletion(),
                "closeOnCompletion() must be reflected by isCloseOnCompletion()");
        ResultSet rs = s.executeQuery("SELECT 1");
        rs.close();
        assertTrue(s.isClosed(),
                "statement must close once its dependent result set is closed");
    }

    /**
     * Pinned (deliberate): maxFieldSize reports 0 (no limit — this driver never
     * truncates values) and a positive setMaxFieldSize is accepted-and-ignored, which
     * the JDBC javadoc permits for drivers without a field-size limit. The
     * negative-argument validation is covered separately by
     * {@link #setMaxFieldSizeRejectsNegativeValues()}.
     */
    @Test
    void maxFieldSizeIsUnboundedAndPositiveSetterIsIgnored() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        assertEquals(0, s.getMaxFieldSize());
        s.setMaxFieldSize(300);
        assertEquals(0, s.getMaxFieldSize(), "pinned: value is not stored (no limit)");
    }

    /**
     * A negative {@code setMaxFieldSize} throws {@link SQLException}, mirroring the
     * validation in {@code setMaxRows}/{@code setFetchSize} (was knownBug 8; the JDBC
     * contract declares "SQLException ... if the condition max >= 0 is not satisfied";
     * jdbc-v2 {@code StatementTest#testMaxFieldSize} asserts the throw).
     */
    @Test
    void setMaxFieldSizeRejectsNegativeValues() {
        ChStatement s = connected(new ScriptedCore());
        assertThrows(SQLException.class, () -> s.setMaxFieldSize(-1),
                "negative maxFieldSize must be rejected per the JDBC contract");
    }

    // ---- knob validation (v2 testSetFetchSize / testMaxRows) -----------------

    @Test
    void fetchSizeZeroAllowedNegativeRejected() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        s.setFetchSize(10_000);
        assertEquals(10_000, s.getFetchSize());
        s.setFetchSize(0);
        assertEquals(0, s.getFetchSize());
        assertThrows(SQLException.class, () -> s.setFetchSize(-1));
        assertEquals(0, s.getFetchSize(), "failed set must not change the stored value");
    }

    @Test
    void maxRowsZeroAllowedNegativeRejected() throws SQLException {
        ChStatement s = connected(new ScriptedCore());
        s.setMaxRows(100);
        assertEquals(100, s.getMaxRows());
        s.setMaxRows(0);
        assertEquals(0, s.getMaxRows());
        assertThrows(SQLException.class, () -> s.setMaxRows(-1));
        assertEquals(0, s.getMaxRows());
    }
}
