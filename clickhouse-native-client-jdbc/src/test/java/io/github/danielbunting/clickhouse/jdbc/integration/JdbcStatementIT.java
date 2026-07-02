package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end {@link Statement} behavior against a real server, ported from the official
 * clickhouse-java suites (v1 {@code ClickHouseStatementTest}, jdbc-v2 {@code StatementTest}):
 * executeUpdate count semantics, statement batches (success/failure/reuse), mutations,
 * cross-thread {@code cancel()}, timeout/maxRows knobs vs server-side settings, CTEs,
 * DESCRIBE metadata, DDL via execute(), FixedString/Enum/SimpleAggregateFunction reads,
 * nested tuple/array shapes, temporal getString/getObject semantics and float extremes.
 *
 * <p>Where this driver's contract deviates from clickhouse-java (native protocol has no
 * update counts; maxRows/queryTimeout are advisory; executeQuery does not classify SQL),
 * the ACTUAL behavior is asserted and the deviation called out in a comment.
 */
@Tag("integration")
@Testcontainers
class JdbcStatementIT {

    /**
     * Opens the default user to remote hosts and grants access management so the DDL test
     * can CREATE USER.
     */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks>"
            + "<access_management>1</access_management>"
            + "</default></users></clickhouse>";

    private static final int NATIVE_PORT = 9000;

    /** A scan far too large to complete quickly, so a prompt cancel is guaranteed partial. */
    private static final long HUGE = 2_000_000_000L;

    @Container
    @SuppressWarnings("resource")
    private static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(ClickHouseImages.SERVER)
                    .withExposedPorts(NATIVE_PORT)
                    .withCopyToContainer(
                            Transferable.of(OPEN_DEFAULT_USER_XML),
                            "/etc/clickhouse-server/users.d/zz-open-default.xml")
                    .waitingFor(Wait.forListeningPort());

    private static String url() {
        return "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(url());
    }

    /** First core {@link ServerException} in the cause chain, or {@code null}. */
    private static ServerException serverException(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ServerException) {
                return (ServerException) c;
            }
        }
        return null;
    }

    /** Counts the rows of a query on a fresh statement. */
    private static int countRows(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) {
                n++;
            }
            return n;
        }
    }

    /**
     * Unwraps a decoded sequence value (the driver materializes arrays variously as
     * {@code List}, {@code Object[]} or primitive arrays depending on element type)
     * into a list of boxed elements, so structural assertions are representation-proof.
     */
    private static List<Object> elements(Object sequence) {
        if (sequence instanceof List<?> l) {
            return new java.util.ArrayList<>(l);
        }
        if (sequence instanceof Object[] a) {
            return java.util.Arrays.asList(a);
        }
        if (sequence != null && sequence.getClass().isArray()) {
            int n = java.lang.reflect.Array.getLength(sequence);
            java.util.ArrayList<Object> out = new java.util.ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(java.lang.reflect.Array.get(sequence, i));
            }
            return out;
        }
        throw new AssertionError("not a sequence: " + sequence);
    }

    /** Asserts that {@code sequence} holds exactly {@code expected} as (boxed) longs. */
    private static void assertIntSequence(Object sequence, long... expected) {
        List<Object> actual = elements(sequence);
        assertEquals(expected.length, actual.size(), "sequence length of " + actual);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], ((Number) actual.get(i)).longValue(),
                    "element " + i + " of " + actual);
        }
    }

    // ------------------------------------------------------------------
    // executeUpdate count semantics (jdbc-v2 testExecuteUpdateSimpleNumbers/
    // Floats/Booleans/Strings/Nulls)
    // ------------------------------------------------------------------

    /**
     * DIVERGENCE (pinned): jdbc-v2 returns the inserted-row count (3) from
     * {@code executeUpdate}. The native protocol carries no update counts, so this driver
     * always reports 0 — but the rows must genuinely land and read back typed.
     */
    @Test
    void executeUpdateReturnsZeroButRowsLand() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_update_counts");
            assertEquals(0, st.executeUpdate(
                    "CREATE TABLE stmt_update_counts (id UInt8, num Float32, flag Bool, "
                    + "words Nullable(String)) ENGINE = Memory"));
            // Reference asserts 3 here; our native driver reports 0 (no protocol counts).
            assertEquals(0, st.executeUpdate(
                    "INSERT INTO stmt_update_counts VALUES "
                    + "(0, 1.1, true, 'Hello'), (1, 2.2, false, NULL), (2, 3.3, true, 'ClickHouse')"));
            assertEquals(0, st.getUpdateCount());
            assertNull(st.getResultSet());

            try (ResultSet rs = st.executeQuery(
                    "SELECT num, flag, words FROM stmt_update_counts ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1.1f, rs.getFloat(1), 0.0001f);
                assertTrue(rs.getBoolean(2));
                assertEquals("Hello", rs.getString(3));
                assertFalse(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(2.2f, rs.getFloat(1), 0.0001f);
                assertFalse(rs.getBoolean(2));
                assertNull(rs.getString(3));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals("ClickHouse", rs.getString(3));
                assertFalse(rs.next());
            }
            // After a query, getUpdateCount reports -1 per the JDBC contract.
            assertEquals(-1, st.getUpdateCount());
        }
    }

    // ------------------------------------------------------------------
    // Date/DateTime inserts incl. NULL (jdbc-v2 testExecuteUpdateDates)
    // ------------------------------------------------------------------

    @Test
    void executeUpdateDatesIncludingNulls() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_dates");
            st.execute("CREATE TABLE stmt_dates (id UInt8, date Nullable(Date), "
                    + "datetime Nullable(DateTime)) ENGINE = Memory");
            assertEquals(0, st.executeUpdate("INSERT INTO stmt_dates VALUES "
                    + "(0, '2020-01-01', '2020-01-01 10:11:12'), "
                    + "(1, NULL, '2020-01-01 12:10:07'), "
                    + "(2, '2020-01-01', NULL)"));
            try (ResultSet rs = st.executeQuery(
                    "SELECT date, datetime FROM stmt_dates ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("2020-01-01", rs.getDate(1).toString());
                assertEquals("2020-01-01", rs.getDate(2).toString());
                assertTrue(rs.next());
                assertNull(rs.getDate(1));
                assertTrue(rs.wasNull());
                assertEquals("2020-01-01", rs.getDate(2).toString());
                assertTrue(rs.next());
                assertEquals("2020-01-01", rs.getDate(1).toString());
                assertNull(rs.getDate(2));
                assertFalse(rs.next());
            }
        }
    }

    // ------------------------------------------------------------------
    // Statement batches (v1 testExecuteBatch, jdbc-v2 testExecuteUpdateBatch/
    // testExecuteUpdateBatchReuse)
    // ------------------------------------------------------------------

    /**
     * Statement batches of DDL and multi-row INSERTs execute entry by entry. Counts are
     * {@link Statement#SUCCESS_NO_INFO} (reference returns real per-statement counts).
     */
    @Test
    void statementBatchExecutesDdlAndInserts() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_batch");
            st.addBatch("CREATE TABLE stmt_batch (id UInt8, num UInt8) ENGINE = Memory");
            st.addBatch("INSERT INTO stmt_batch VALUES (0, 1)");
            st.addBatch("INSERT INTO stmt_batch VALUES (1, 2), (2, 3)");
            int[] counts = st.executeBatch();
            assertEquals(3, counts.length);
            for (int c : counts) {
                assertEquals(Statement.SUCCESS_NO_INFO, c);
            }
            try (ResultSet rs = st.executeQuery("SELECT count(), sum(num) FROM stmt_batch")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals(6, rs.getInt(2));
            }
        }
    }

    /**
     * A failed statement batch throws, is auto-cleared, and the statement is reusable.
     *
     * <p>DIVERGENCE (pinned): jdbc-v2 {@code testExecuteUpdateBatchReuse} keeps the bad
     * entries until {@code clearBatch()} (a second {@code executeBatch} fails again); this
     * driver clears the batch in a {@code finally}, so the retry is an empty no-op.
     */
    @Test
    void statementBatchFailureAutoClearsAndStatementIsReusable() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_batch_reuse");
            st.execute("CREATE TABLE stmt_batch_reuse (id UInt8, num UInt8) ENGINE = Memory");
            st.addBatch("INSERT INTO stmt_batch_reuse VALUES (0, 'invalid')");
            assertThrows(SQLException.class, st::executeBatch);
            // Pinned divergence: batch already cleared, so this is a no-op (reference
            // would throw again until clearBatch()).
            assertEquals(0, st.executeBatch().length);

            st.addBatch("INSERT INTO stmt_batch_reuse VALUES (0, 1)");
            st.addBatch("INSERT INTO stmt_batch_reuse VALUES (1, 2), (2, 3)");
            assertEquals(2, st.executeBatch().length);
            try (ResultSet rs = st.executeQuery("SELECT count() FROM stmt_batch_reuse")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    // ------------------------------------------------------------------
    // Mutations (v1 testMutation)
    // ------------------------------------------------------------------

    /**
     * Lightweight DELETE and ALTER ... UPDATE mutations run through
     * {@code executeUpdate} (returning 0 — the native protocol reports no mutation
     * counts; the v1 reference rewrites {@code delete from}/{@code update} syntax and
     * returns 1). {@code continueBatchOnError} has no counterpart here.
     */
    @Test
    void mutationsExecuteViaExecuteUpdate() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_mutation");
            st.execute("CREATE TABLE stmt_mutation (a String, b UInt32) "
                    + "ENGINE = MergeTree ORDER BY tuple()");
            st.execute("INSERT INTO stmt_mutation VALUES ('1', 1), ('2', 2), ('3', 3)");

            assertEquals(0, st.executeUpdate(
                    "ALTER TABLE stmt_mutation UPDATE b = 22 WHERE b = 1 "
                    + "SETTINGS mutations_sync = 1"));
            assertEquals(1, scalar(conn, "SELECT count() FROM stmt_mutation WHERE b = 22"));

            assertEquals(0, st.executeUpdate(
                    "DELETE FROM stmt_mutation WHERE b = 2 SETTINGS mutations_sync = 1"));
            assertEquals(2, scalar(conn, "SELECT count() FROM stmt_mutation"));

            // A mutation against a missing table surfaces as an SQLException.
            assertThrows(SQLException.class, () -> st.executeUpdate(
                    "ALTER TABLE stmt_no_such_table UPDATE b = 1 WHERE b = 1"));
        }
    }

    private static long scalar(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    // ------------------------------------------------------------------
    // cancel() (v1 testCancelQuery, jdbc-v2 testConcurrentCancel)
    // ------------------------------------------------------------------

    /**
     * {@link Statement#cancel()} from another thread stops an in-flight streaming query
     * (the reading thread drains to end-of-stream rather than hanging) and the statement
     * remains usable afterwards.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void cancelStopsLongQueryAndStatementRemainsUsable() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            AtomicLong rowsRead = new AtomicLong();
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            Thread worker = new Thread(() -> {
                started.countDown();
                try (ResultSet rs = st.executeQuery(
                        "SELECT number FROM numbers(" + HUGE + ")")) {
                    while (rs.next()) {
                        rowsRead.incrementAndGet();
                    }
                } catch (Throwable t) {
                    workerError.set(t);
                }
            });
            worker.start();
            started.await();

            // Wait until rows are actually streaming, then cancel from this thread.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (rowsRead.get() == 0 && System.nanoTime() < deadline && worker.isAlive()) {
                Thread.sleep(20);
            }
            assertTrue(rowsRead.get() > 0, "query never started streaming");
            st.cancel();

            worker.join(TimeUnit.SECONDS.toMillis(60));
            assertFalse(worker.isAlive(), "cancel() must unblock the reading thread");
            assertNull(workerError.get(),
                    "cancelled stream drains cleanly (no exception): " + workerError.get());
            assertTrue(rowsRead.get() < HUGE,
                    "cancel stopped the scan early; read " + rowsRead.get());

            // The statement (and its connection) remain usable after the cancel.
            try (ResultSet rs = st.executeQuery("SELECT 5")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Cancelling a long-running INSERT ... SELECT (jdbc-v2 {@code testCancelInsertWithSession},
     * #2690 — sessions are implicit on a native connection, so no SESSION_IS_LOCKED can occur).
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void cancelStopsLongInsert() throws Exception {
        try (Connection conn = connect(); Connection observer = connect();
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_cancel_insert");
            st.execute("CREATE TABLE stmt_cancel_insert (num UInt64) "
                    + "ENGINE = MergeTree ORDER BY tuple()");

            AtomicReference<Throwable> workerError = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            Thread worker = new Thread(() -> {
                started.countDown();
                try {
                    st.executeUpdate("INSERT INTO stmt_cancel_insert "
                            + "SELECT number FROM numbers(" + HUGE + ")");
                } catch (Throwable t) {
                    workerError.set(t);
                }
            });
            worker.start();
            started.await();

            // Observe from a second connection until the INSERT shows in system.processes.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            boolean running = false;
            while (!running && System.nanoTime() < deadline && worker.isAlive()) {
                running = scalar(observer, "SELECT count() FROM system.processes "
                        + "WHERE query LIKE 'INSERT INTO stmt_cancel_insert%'") > 0;
                if (!running) {
                    Thread.sleep(50);
                }
            }
            assertTrue(running, "insert never started on the server (worker error: "
                    + workerError.get() + ")");

            st.cancel();
            worker.join(TimeUnit.SECONDS.toMillis(60));
            assertFalse(worker.isAlive(), "cancel() must unblock the inserting thread");
            // Whether the worker saw an exception depends on where the server stopped the
            // query; either way it must have terminated early.
            assertTrue(scalar(observer, "SELECT count() FROM stmt_cancel_insert") < HUGE,
                    "cancel stopped the insert early");

            // The connection remains usable after the cancel.
            assertEquals(1, scalar(conn, "SELECT 1"));
        }
    }

    // ------------------------------------------------------------------
    // queryTimeout / maxRows knobs vs server settings
    // (jdbc-v2 testExecuteQueryTimeout / testMaxRows, v1 testMaxResultsRows)
    // ------------------------------------------------------------------

    /**
     * KNOWN BUG (failing on purpose — ported from jdbc-v2
     * {@code StatementTest#testExecuteQueryTimeout}): a query running longer than
     * {@code setQueryTimeout(seconds)} must be aborted with an {@link SQLException}
     * (JDBC contract; jdbc-v2 enforces it, and the core's
     * {@code QueryCancelledException} javadoc explicitly promises "JDBC
     * {@code Statement.setQueryTimeout}" fires the client-side watchdog).
     *
     * <p>Actual: {@code ChStatement.setQueryTimeout} only stores the value —
     * {@code executeQuery}/{@code executeUpdate} never consult it (and the core's
     * {@code ClickHouseConfig.queryTimeout} deadline is likewise unwired), so
     * {@code SELECT sleep(2)} completes normally after ~2s.
     *
     * <p>HOW TO FIX: in {@code ChStatement.executeQuery}/{@code executeUpdate}
     * (src/main/java/.../jdbc/ChStatement.java), when {@code queryTimeoutSeconds > 0}
     * pass the per-query server setting {@code max_execution_time=<seconds>} via the
     * core's {@code query(String, Map)} / {@code execute(String, Map)} overloads
     * (simplest), or implement the promised client-side watchdog that schedules
     * {@code conn.core().cancel()} after the deadline and surfaces
     * {@code QueryCancelledException} as an {@code SQLTimeoutException}.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void knownBug_queryTimeoutAbortsLongRunningQuery() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.setQueryTimeout(1);
            assertEquals(1, st.getQueryTimeout());
            assertThrows(SQLException.class, () -> {
                try (ResultSet rs = st.executeQuery("SELECT sleep(2)")) {
                    rs.next();
                }
            }, "a query exceeding the 1s timeout must be aborted");
        }
    }

    /**
     * KNOWN DIVERGENCE (pinned): {@code setMaxRows} is advisory — the full result comes
     * back (jdbc-v2 truncates to the limit). Server-side limiting works via connection
     * settings in the URL: {@code settings.max_result_rows} with
     * {@code result_overflow_mode=break} truncates (to block granularity), and with the
     * default {@code throw} mode the server rejects the query with code 396.
     */
    @Test
    void maxRowsIsAdvisoryAndServerSettingsEnforceLimits() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.setMaxRows(5);
            assertEquals(5, st.getMaxRows());
            assertEquals(100, countRows(conn, "SELECT number FROM numbers(100)"),
                    "pinned: maxRows does not truncate the result");
        }

        // Server-side enforcement: break mode truncates (block-granular, like the
        // reference's max_result_rows data-provider cases).
        try (Connection conn = DriverManager.getConnection(url()
                + "?settings.max_result_rows=1000&settings.result_overflow_mode=break")) {
            int n = countRows(conn, "SELECT number FROM numbers(100000)");
            assertTrue(n >= 1000 && n < 100000,
                    "break mode truncates to >= limit (block granularity), got " + n);
        }

        // Default overflow mode (throw): the server rejects with TOO_MANY_ROWS (396).
        // The error arrives MID-STREAM, and this driver currently leaks it as a raw
        // core exception rather than an SQLException — the type defect is documented by
        // knownBug_midStreamServerErrorSurfacesAsSqlException below; here we only prove
        // the server-side setting is enforced and its code reaches the caller.
        try (Connection conn = DriverManager.getConnection(url()
                + "?settings.max_result_rows=1000");
                Statement st = conn.createStatement()) {
            Exception e = assertThrows(Exception.class, () -> {
                try (ResultSet rs = st.executeQuery("SELECT number FROM numbers(100000)")) {
                    while (rs.next()) {
                        // drain — the overflow surfaces mid-stream
                    }
                }
            });
            ServerException server = serverException(e);
            assertNotNull(server, "expected a server-side limit error: " + e);
            assertEquals(396, server.code(),
                    "expected TOO_MANY_ROWS/LIMIT_EXCEEDED (396): " + e.getMessage());
        }
    }

    /**
     * KNOWN BUG (failing on purpose — JDBC contract: {@link ResultSet#next()} declares
     * {@code throws SQLException}, and every reference driver surfaces mid-stream server
     * errors that way): when the server reports an error AFTER the result stream has
     * started (here {@code max_result_rows} overflow in the default {@code throw} mode),
     * draining the result set must raise an {@link SQLException}.
     *
     * <p>Actual: {@code ChResultSet.next()} iterates the core block iterator with no
     * exception translation, so the raw unchecked core
     * {@code io.github.danielbunting.clickhouse.ServerException} escapes through the
     * JDBC API (callers catching {@code SQLException} miss it entirely).
     *
     * <p>HOW TO FIX: in {@code ChResultSet.next()}
     * (src/main/java/.../jdbc/ChResultSet.java), wrap the {@code blocks.hasNext()} /
     * {@code blocks.next()} calls in {@code try/catch (ClickHouseException e)} and
     * rethrow {@code new SQLException(e.getMessage(), e)} (mirroring
     * {@code ChStatement.wrap}); while there, propagate {@code ServerException.code()}
     * as the vendor code (see
     * {@code JdbcErrorHandlingIT#knownBug_serverErrorCodePropagatedToSqlExceptionGetErrorCode}).
     */
    @Test
    void knownBug_midStreamServerErrorSurfacesAsSqlException() throws Exception {
        try (Connection conn = DriverManager.getConnection(url()
                + "?settings.max_result_rows=1000");
                Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT number FROM numbers(100000)");
            assertThrows(SQLException.class, () -> {
                while (rs.next()) {
                    // drain until the overflow error arrives mid-stream
                }
            }, "mid-stream server errors must surface as SQLException from next()");
        }
    }

    // ------------------------------------------------------------------
    // WITH clause (jdbc-v2 testWithClause)
    // ------------------------------------------------------------------

    @Test
    void withClauseCteReturnsRowsViaExecute() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            assertTrue(st.execute(
                    "with data as (SELECT number FROM numbers(100)) select * from data"));
            int count = 0;
            try (ResultSet rs = st.getResultSet()) {
                while (rs.next()) {
                    count++;
                }
            }
            assertEquals(100, count);
        }
    }

    // ------------------------------------------------------------------
    // DESCRIBE (jdbc-v2 testDescribeStatement, v1 testDescMetadata)
    // ------------------------------------------------------------------

    @Test
    void describeStatementYieldsNameTypeRowsAndMetadata() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            assertTrue(st.execute("DESCRIBE TABLE (SELECT 10, 'message', 30)"),
                    "DESCRIBE produces a result set");
            try (ResultSet rs = st.getResultSet()) {
                // DESC output carries 7 metadata columns (name, type, default_type, ...).
                assertEquals(7, rs.getMetaData().getColumnCount());
                Object[][] expected = {
                        {"10", "UInt8"},
                        {"'message'", "String"},
                        {"30", "UInt8"},
                };
                for (Object[] row : expected) {
                    assertTrue(rs.next());
                    assertEquals(row[0], rs.getString("name"));
                    assertEquals(row[1], rs.getString("type"));
                }
                assertFalse(rs.next());
            }

            // v1 testDescMetadata: DESC of a two-column subquery.
            try (ResultSet rs = st.executeQuery(
                    "DESC (SELECT timezone(), number FROM system.numbers)")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getMetaData().getColumnCount());
                assertEquals("timezone()", rs.getString("name"));
                assertTrue(rs.next());
                assertEquals("number", rs.getString("name"));
                assertEquals("UInt64", rs.getString("type"));
                assertFalse(rs.next());
            }
        }
    }

    // ------------------------------------------------------------------
    // DDL via execute() + SHOW USERS (jdbc-v2 testDDLStatements)
    // ------------------------------------------------------------------

    @Test
    void createUserDdlExecutesAndShowUsersListsIt() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            assertFalse(st.execute(
                    "CREATE USER IF NOT EXISTS 'stmt_user011' IDENTIFIED BY 'password'"),
                    "DDL via execute() must not report a result set");
            boolean found = false;
            try (ResultSet rs = st.executeQuery("SHOW USERS")) {
                while (rs.next()) {
                    if ("stmt_user011".equals(rs.getString("name"))) {
                        found = true;
                    }
                }
            }
            assertTrue(found, "SHOW USERS must list the created user");
            st.execute("DROP USER IF EXISTS 'stmt_user011'");
        }
    }

    // ------------------------------------------------------------------
    // SimpleAggregateFunction (v1 testSimpleAggregateFunction)
    // ------------------------------------------------------------------

    @Test
    void simpleAggregateFunctionColumnReadsThroughJdbc() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_simple_agg_func");
            st.execute("CREATE TABLE stmt_simple_agg_func (id UInt64, "
                    + "x SimpleAggregateFunction(max, UInt64)) "
                    + "ENGINE = AggregatingMergeTree ORDER BY tuple(id)");
            st.execute("INSERT INTO stmt_simple_agg_func VALUES (1, 1)");
            try (ResultSet rs = st.executeQuery("SELECT * FROM stmt_simple_agg_func")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong(1));
                assertEquals(1L, rs.getLong(2));
                assertFalse(rs.next());
            }
        }
    }

    // ------------------------------------------------------------------
    // FixedString families (jdbc-v2 testNullableFixedStringType)
    // ------------------------------------------------------------------

    @Test
    void fixedStringFamiliesReadAsStrings() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS stmt_fixed_strings");
            assertEquals(0, st.executeUpdate("CREATE TABLE stmt_fixed_strings ("
                    + "f1 FixedString(4), f2 LowCardinality(FixedString(4)), "
                    + "f3 Nullable(FixedString(4)), "
                    + "f4 LowCardinality(Nullable(FixedString(4)))) ENGINE = Memory"));
            assertEquals(0, st.executeUpdate(
                    "INSERT INTO stmt_fixed_strings VALUES ('val1', 'val2', 'val3', 'val4')"));
            try (ResultSet rs = st.executeQuery("SELECT * FROM stmt_fixed_strings")) {
                assertTrue(rs.next());
                assertEquals("val1", rs.getString(1));
                assertEquals("val2", rs.getString(2));
                assertEquals("val3", rs.getString(3));
                assertEquals("val4", rs.getString(4));
                assertFalse(rs.next());
            }
            try (ResultSet rs = st.executeQuery("SELECT f4 FROM stmt_fixed_strings")) {
                assertTrue(rs.next());
                assertEquals("val4", rs.getString(1));
            }
        }
    }

    // ------------------------------------------------------------------
    // Enum getters (v1 testCustomTypeMappings — the getter matrix; the
    // typeMappings connection option has no counterpart here)
    // ------------------------------------------------------------------

    /**
     * DIVERGENCE (pinned): the native codec materializes Enum8 as its String name, so
     * {@code getString}/{@code getObject} return {@code "a"}, and the numeric getters
     * (which the v1 reference maps to the ordinal 1) throw {@link SQLException} because
     * the name is not numeric.
     */
    @Test
    void enumColumnGettersReturnNameNotOrdinal() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT CAST('a' AS Enum('a'=1,'b'=2))")) {
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("a", rs.getObject(1));
            // Reference: getByte/getShort/getInt return the ordinal 1.
            assertThrows(SQLException.class, () -> rs.getByte(1));
            assertThrows(SQLException.class, () -> rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    // ------------------------------------------------------------------
    // Nested arrays in tuples (v1 testNestedArrayInTuple; also pins the
    // getObject shapes the v1 wrapperObject test relies on — this driver has
    // no wrapperObject option and never returns java.sql.Struct)
    // ------------------------------------------------------------------

    @Test
    void nestedArrayInTupleShapes() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            // Nested values on the same row: two Tuple(Array(Int32)) columns.
            st.execute("DROP TABLE IF EXISTS stmt_nested_tuple");
            st.execute("CREATE TABLE stmt_nested_tuple (id UInt64, v1 Tuple(Array(Int32)), "
                    + "v2 Tuple(Array(Int32))) ENGINE = Memory");
            st.execute("INSERT INTO stmt_nested_tuple VALUES (1, ([1, 2]), ([2, 3]))");
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM stmt_nested_tuple ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                List<?> t1 = assertInstanceOf(List.class, rs.getObject(2),
                        "tuples materialize as List");
                assertEquals(1, t1.size());
                assertIntSequence(t1.get(0), 1, 2);
                List<?> t2 = assertInstanceOf(List.class, rs.getObject(3));
                assertIntSequence(t2.get(0), 2, 3);
                assertFalse(rs.next());
            }

            // Deeper nesting: Array(Tuple(UInt16, Array(UInt32))) across two rows.
            st.execute("DROP TABLE IF EXISTS stmt_nested_tuple");
            st.execute("CREATE TABLE stmt_nested_tuple (id UInt64, "
                    + "val Array(Tuple(UInt16, Array(UInt32)))) ENGINE = Memory");
            st.execute("INSERT INTO stmt_nested_tuple VALUES "
                    + "(1, [(0, [1, 2]), (1, [2, 3])]), (2, [(2, [4, 5]), (3, [6, 7])])");
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM stmt_nested_tuple ORDER BY id")) {
                assertTrue(rs.next());
                List<Object> outer = elements(rs.getObject(2));
                assertEquals(2, outer.size());
                List<?> first = assertInstanceOf(List.class, outer.get(0));
                assertEquals(0L, ((Number) first.get(0)).longValue());
                assertIntSequence(first.get(1), 1, 2);
                List<?> second = assertInstanceOf(List.class, outer.get(1));
                assertEquals(1L, ((Number) second.get(0)).longValue());
                assertIntSequence(second.get(1), 2, 3);

                assertTrue(rs.next());
                outer = elements(rs.getObject(2));
                List<?> third = assertInstanceOf(List.class, outer.get(0));
                assertEquals(2L, ((Number) third.get(0)).longValue());
                assertIntSequence(third.get(1), 4, 5);
                assertFalse(rs.next());
            }
        }
    }

    // ------------------------------------------------------------------
    // Temporal semantics (jdbc-v2 testExecuteQueryDates, v1 testTimestamp/
    // testTimeZone/testUseOffsetDateTime)
    // ------------------------------------------------------------------

    /**
     * Temporal reads pin this driver's native mapping: {@code Date -> LocalDate} and
     * {@code DateTime -> Instant} (a fixed point in time, so the column's timezone is
     * only a rendering concern and never shifts the value read).
     *
     * <p>DIVERGENCES (pinned): the reference renders {@code getString(DateTime)} as the
     * wall-clock {@code "2020-01-01 10:11:12"} and returns
     * {@code LocalDateTime}/{@code OffsetDateTime} from {@code getObject}; this driver
     * renders the Instant's ISO form and offers no OffsetDateTime coercion. The
     * {@code use_time_zone}/{@code use_server_time_zone} connection options have no
     * counterpart here.
     */
    @Test
    void temporalGetObjectAndGetStringSemantics() throws Exception {
        Instant istanbul = ZonedDateTime.of(
                2020, 1, 1, 10, 11, 12, 0, ZoneId.of("Asia/Istanbul")).toInstant();
        Instant epoch1616633456 = Instant.ofEpochSecond(1616633456L);
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT toDate('2020-01-01') AS date, "
                        + "toDateTime('2020-01-01 10:11:12', 'Asia/Istanbul') AS datetime, "
                        + "toDateTime(1616633456, 'America/Los_Angeles') AS la, "
                        + "toDateTime(1616633456, 'Asia/Chongqing') AS cq")) {
            assertTrue(rs.next());
            assertEquals("2020-01-01", rs.getDate(1).toString());
            assertEquals("2020-01-01", rs.getDate("date").toString());
            assertEquals("2020-01-01", rs.getString(1), "Date renders as ISO date");

            assertEquals(istanbul, rs.getObject(2), "DateTime reads as the exact Instant");
            // Reference renders "2020-01-01 10:11:12"; we pin the Instant's ISO form.
            assertEquals(istanbul.toString(), rs.getString(2));

            // The column timezone does not shift the value: same epoch, same Instant.
            assertEquals(epoch1616633456, rs.getObject(3));
            assertEquals(epoch1616633456, rs.getObject(4));
            assertEquals(rs.getObject(3), rs.getObject(4));

            // Pinned: no OffsetDateTime coercion (v1 returns OffsetDateTime for
            // tz-qualified DateTime columns).
            assertThrows(SQLException.class,
                    () -> rs.getObject(3, OffsetDateTime.class));
            assertFalse(rs.next());
        }
    }

    // ------------------------------------------------------------------
    // Float extremes (v1 testMaxFloatValues)
    // ------------------------------------------------------------------

    @Test
    void maxFloatAndDoubleValuesRoundTrip() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS stmt_float_values");
                st.execute("CREATE TABLE stmt_float_values (f1 Nullable(Float64), "
                        + "f2 Nullable(Float64)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO stmt_float_values VALUES (?, ?)")) {
                ps.setObject(1, Float.MAX_VALUE);
                ps.setObject(2, Double.MAX_VALUE);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM stmt_float_values")) {
                assertTrue(rs.next());
                assertEquals(Float.MAX_VALUE, rs.getFloat(1));
                assertEquals(Double.MAX_VALUE, rs.getDouble(2));
                assertFalse(rs.next());
            }
        }
    }

    // ------------------------------------------------------------------
    // DateTime64(9) nanoseconds (v1 testTimestampWithNanoSeconds)
    // ------------------------------------------------------------------

    @Test
    void dateTime64NanosecondTimestampRoundTrips() throws Exception {
        // The driver renders a Timestamp's wall clock into the (UTC) DateTime64 column,
        // so compare as an Instant derived from the same wall clock (see
        // JdbcPreparedStatementIT#scalarRoundTrip for the same convention).
        LocalDateTime wall = LocalDateTime.of(2026, 5, 30, 13, 45, 7, 123_456_789);
        Instant expected = wall.toInstant(ZoneOffset.UTC);
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS stmt_ts_nanos");
                st.execute("CREATE TABLE stmt_ts_nanos (d DateTime64(9)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO stmt_ts_nanos VALUES (?)")) {
                ps.setTimestamp(1, Timestamp.valueOf(wall));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT d FROM stmt_ts_nanos")) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getObject(1));
                assertEquals(123_456_789, rs.getTimestamp(1).getNanos(),
                        "the nanosecond fraction survives the round trip");
                assertFalse(rs.next());
            }
        }
    }

    // ------------------------------------------------------------------
    // Odd-but-valid SQL + executeQuery(INSERT) (jdbc-v2 testUnknownStatement)
    // ------------------------------------------------------------------

    /**
     * A SELECT with a trailing comma still queries. DIVERGENCE (pinned): jdbc-v2 throws
     * from {@code executeQuery(INSERT)}; this driver routes it to the query path, the
     * server executes the insert, and an empty result set comes back.
     */
    @Test
    void oddSqlAndExecuteQueryOnInsert() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            assertTrue(st.execute("SELECT number, FROM system.numbers LIMIT 3"));
            try (ResultSet rs = st.getResultSet()) {
                for (long i = 0; i < 3; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getLong(1));
                }
                assertFalse(rs.next());
            }

            st.execute("DROP TABLE IF EXISTS stmt_unknown_statement");
            st.execute("CREATE TABLE stmt_unknown_statement (v Int32) ENGINE = Memory");
            // INSERT via execute() reports no result set.
            assertFalse(st.execute("INSERT INTO stmt_unknown_statement VALUES (1)"));
            assertNull(st.getResultSet());
            // Pinned divergence: executeQuery(INSERT) succeeds with an empty result set
            // (reference throws) — and the row genuinely lands.
            try (ResultSet rs = st.executeQuery(
                    "INSERT INTO stmt_unknown_statement VALUES (3)")) {
                assertFalse(rs.next(), "INSERT via executeQuery yields an empty result set");
            }
            assertEquals(2, scalar(conn, "SELECT count() FROM stmt_unknown_statement"));
        }
    }

    // ------------------------------------------------------------------
    // setCatalog (v1 testSwitchCatalog/testSwitchSchema)
    // ------------------------------------------------------------------

    /**
     * DIVERGENCE (pinned): v1 {@code setCatalog} switches the active database (and
     * {@code USE} updates {@code getCatalog}); in this driver both catalog and schema
     * setters are storage-only and {@code USE} is not tracked back into them. The
     * actual database switch via {@code USE} is covered by
     * {@code JdbcSessionIT#useStatementSwitchesDatabaseForSubsequentQueries}.
     */
    @Test
    void setCatalogIsStorageOnlyAndUseIsNotTracked() throws Exception {
        try (Connection conn = connect()) {
            // The URL carries the database in the path, not the "database" property, so
            // the initial catalog is unset.
            assertNull(conn.getCatalog());

            conn.setCatalog("system");
            assertEquals("system", conn.getCatalog(), "setCatalog stores the value");
            assertEquals("default", scalarString(conn, "SELECT currentDatabase()"),
                    "pinned: setCatalog must NOT switch the session database");

            // A real switch via USE is NOT reflected in getCatalog (no tracking).
            try (Statement st = conn.createStatement()) {
                st.execute("USE system");
            }
            assertEquals("system", scalarString(conn, "SELECT currentDatabase()"));
            assertEquals("system", conn.getCatalog(),
                    "still the stored value — coincidentally equal here");
            try (Statement st = conn.createStatement()) {
                st.execute("USE default");
            }
            assertEquals("system", conn.getCatalog(),
                    "pinned: USE does not update the stored catalog");
        }
    }

    private static String scalarString(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }
}
