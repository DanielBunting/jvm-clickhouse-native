package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChPreparedStatement}: placeholder substitution, literal
 * quoting/escaping, and multi-row batch INSERT building. No server is involved;
 * the core connection is a hand-written fake that records the SQL it is asked to
 * execute.
 */
class ChPreparedStatementTest {

    /** A core {@link ClickHouseConnection} that records executed SQL and does nothing else. */
    private static final class RecordingCore implements ClickHouseConnection {
        final List<String> executed = new ArrayList<>();

        @Override
        public long executeScalar(String sql) {
            return 0;
        }

        @Override
        public void execute(String sql) {
            executed.add(sql);
        }

        @Override
        public QueryResult query(String sql) {
            executed.add(sql);
            return null;
        }

        @Override
        public <T> Stream<T> query(String sql, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static ChConnection conn(RecordingCore core) {
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", new Properties());
    }

    // ---- static helpers -----------------------------------------------------

    @Test
    void quotesAndEscapesStrings() {
        assertEquals("'hello'", ChPreparedStatement.quote("hello"));
        assertEquals("'O\\'Brien'", ChPreparedStatement.quote("O'Brien"));
        assertEquals("'a\\\\b'", ChPreparedStatement.quote("a\\b"));
    }

    @Test
    void rendersLiteralsByType() {
        assertEquals("NULL", ChPreparedStatement.toLiteral(null));
        assertEquals("42", ChPreparedStatement.toLiteral(42));
        assertEquals("42", ChPreparedStatement.toLiteral(42L));
        assertEquals("3.14", ChPreparedStatement.toLiteral(3.14d));
        assertEquals("1", ChPreparedStatement.toLiteral(Boolean.TRUE));
        assertEquals("0", ChPreparedStatement.toLiteral(Boolean.FALSE));
        assertEquals("12.50", ChPreparedStatement.toLiteral(new BigDecimal("12.50")));
        assertEquals("'it\\'s'", ChPreparedStatement.toLiteral("it's"));
    }

    @Test
    void rendersTimestampLiteral() {
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 5, 30, 13, 45, 7));
        assertEquals("'2026-05-30 13:45:07'", ChPreparedStatement.toLiteral(ts));
    }

    @Test
    void countsPlaceholdersOutsideStringLiterals() {
        assertEquals(2, ChPreparedStatement.countPlaceholders("SELECT ? WHERE a = ?"));
        // The '?' inside the literal must not count.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT ? WHERE s = 'is it?'"));
        // Escaped quote inside a literal.
        assertEquals(1, ChPreparedStatement.countPlaceholders("SELECT 'o''?' , ?"));
    }

    @Test
    void substitutesPlaceholdersAndLeavesLiteralQuestionMarks() throws SQLException {
        Object[] values = {null, "x", 5};
        String result = ChPreparedStatement.substitute(
                "SELECT ? FROM t WHERE note = 'really?' AND n = ?", values);
        assertEquals("SELECT 'x' FROM t WHERE note = 'really?' AND n = 5", result);
    }

    // ---- end-to-end through the statement -----------------------------------

    @Test
    void executeUpdateSubstitutesAndForwardsToCore() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (id, name) VALUES (?, ?)");
        ps.setInt(1, 7);
        ps.setString(2, "a'b");
        ps.executeUpdate();

        assertEquals(1, core.executed.size());
        assertEquals("INSERT INTO t (id, name) VALUES (7, 'a\\'b')", core.executed.get(0));
    }

    @Test
    void nullParameterRendersAsSqlNull() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        ps.setNull(1, java.sql.Types.VARCHAR);
        ps.executeUpdate();

        assertEquals("INSERT INTO t (x) VALUES (NULL)", core.executed.get(0));
    }

    @Test
    void batchInsertCollapsesToSingleMultiRowStatement() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (id, name) VALUES (?, ?)");

        ps.setInt(1, 1);
        ps.setString(2, "alice");
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setString(2, "bob");
        ps.addBatch();

        int[] counts = ps.executeBatch();

        assertEquals(2, counts.length);
        assertEquals(1, core.executed.size());
        assertEquals(
                "INSERT INTO t (id, name) VALUES (1, 'alice'),(2, 'bob')",
                core.executed.get(0));
    }

    @Test
    void clearParametersResetsBindings() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        ps.setInt(1, 99);
        ps.clearParameters();
        // After clearing, the unbound parameter renders as NULL.
        ps.executeUpdate();
        assertEquals("INSERT INTO t (x) VALUES (NULL)", core.executed.get(0));
    }

    @Test
    void outOfRangeParameterIndexThrows() {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps =
                new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        assertThrows(SQLException.class, () -> ps.setInt(2, 1));
    }

    // ---- issue regressions (distributed slices; see ChJdbcIssuesTest) --------

    /**
     * A {@code Collection}/array binding renders as a ClickHouse array literal.
     * Regression for clickhouse-java#2329 (write an Array(String) via setObject).
     */
    @Test
    void rendersArrayLiteralFromCollection() {
        assertEquals("['a','b']", ChPreparedStatement.toLiteral(List.of("a", "b")));
        assertEquals("[1,2,3]", ChPreparedStatement.toLiteral(List.of(1, 2, 3)));
        assertEquals("[]", ChPreparedStatement.toLiteral(List.of()));
        // Nested arrays recurse.
        assertEquals("[[1,2],[3]]", ChPreparedStatement.toLiteral(List.of(List.of(1, 2), List.of(3))));
        // Java arrays are handled like collections.
        assertEquals("[1,2]", ChPreparedStatement.toLiteral(new int[] {1, 2}));
    }

    /** setObject(collection) flows an array literal end-to-end. Regression for #2329. */
    @Test
    void setObjectCollectionRendersArrayLiteralEndToEnd() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (id, arr) VALUES (?, ?)");
        ps.setString(1, "id01");
        ps.setObject(2, List.of("x", "y"));
        ps.executeUpdate();
        assertEquals("INSERT INTO t (id, arr) VALUES ('id01', ['x','y'])", core.executed.get(0));
    }

    /** UUID binds as a quoted string literal ClickHouse casts to UUID. Regression for #2327. */
    @Test
    void rendersUuidLiteral() {
        java.util.UUID uuid = java.util.UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        assertEquals("'61f0c404-5cb3-11e7-907b-a6006ad3dba0'", ChPreparedStatement.toLiteral(uuid));
    }

    /** IPv6/IPv4 bind as the canonical numeric address, quoted. Regression for #315. */
    @Test
    void rendersInetAddressLiteral() throws Exception {
        java.net.InetAddress v6 = java.net.InetAddress.getByName("2001:db8::1");
        assertEquals("'" + v6.getHostAddress() + "'", ChPreparedStatement.toLiteral(v6));
        java.net.InetAddress v4 = java.net.InetAddress.getByName("192.168.0.1");
        assertEquals("'192.168.0.1'", ChPreparedStatement.toLiteral(v4));
    }

    /** Sub-second timestamps keep their fractional part. Regression for #612 (DateTime64). */
    @Test
    void rendersFractionalTimestampLiteral() {
        Timestamp ts = Timestamp.valueOf("2026-05-30 13:45:07.123456");
        assertEquals("'2026-05-30 13:45:07.123456'", ChPreparedStatement.toLiteral(ts));
        // Whole-second values stay terse (no trailing .000000).
        Timestamp whole = Timestamp.valueOf("2026-05-30 13:45:07");
        assertEquals("'2026-05-30 13:45:07'", ChPreparedStatement.toLiteral(whole));
    }

    /**
     * A {@code null} bound on one row of a batch does not throw and renders as NULL.
     * Regression for clickhouse-java#1373.
     */
    @Test
    void nullInBatchDoesNotThrowAndRendersNull() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (a, b) VALUES (?, ?)");
        ps.setString(1, "r1a");
        ps.setString(2, "r1b");
        ps.addBatch();
        ps.setString(1, "r2a");
        ps.setString(2, null); // null mid-batch
        ps.addBatch();

        int[] counts = ps.executeBatch();
        assertEquals(2, counts.length);
        assertEquals("INSERT INTO t (a, b) VALUES ('r1a', 'r1b'),('r2a', NULL)", core.executed.get(0));
    }
}
