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
}
