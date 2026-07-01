package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link ChStatement} — statement-kind classification, the JDBC-spec
 * unsupported-feature contract, and the wrapper API. This class previously had no dedicated
 * test. No server: the unsupported-feature methods throw before touching the connection, so a
 * {@code null} connection is sufficient, and {@code producesResultSet} is a pure classifier.
 */
class ChStatementTest {

    private static ChStatement stmt() {
        // These methods throw (or classify) before dereferencing the connection.
        return new ChStatement((ChConnection) null);
    }

    @Test
    void producesResultSet_classifiesResultProducingStatements() {
        assertTrue(ChStatement.producesResultSet("SELECT 1"));
        assertTrue(ChStatement.producesResultSet("  select * from t"), "leading space + lowercase");
        assertTrue(ChStatement.producesResultSet("SHOW TABLES"));
        assertTrue(ChStatement.producesResultSet("DESC t"));
        assertTrue(ChStatement.producesResultSet("DESCRIBE TABLE t"), "DESCRIBE starts with DESC");
        assertTrue(ChStatement.producesResultSet("EXISTS TABLE t"));
        assertTrue(ChStatement.producesResultSet("WITH x AS (SELECT 1) SELECT * FROM x"));
    }

    @Test
    void producesResultSet_rejectsNonQueryStatements() {
        assertFalse(ChStatement.producesResultSet("INSERT INTO t VALUES (1)"));
        assertFalse(ChStatement.producesResultSet("CREATE TABLE t (a UInt8) ENGINE = Memory"));
        assertFalse(ChStatement.producesResultSet("DROP TABLE t"));
        assertFalse(ChStatement.producesResultSet(null), "null SQL is not result-producing");
    }

    @Test
    void generatedKeysOverloadsAreUnsupported() {
        ChStatement s = stmt();
        assertThrows(SQLFeatureNotSupportedException.class, s::getGeneratedKeys);
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.executeUpdate("INSERT INTO t VALUES (1)", Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.executeUpdate("x", new int[]{1}));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.executeUpdate("x", new String[]{"c"}));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.execute("x", Statement.RETURN_GENERATED_KEYS));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.execute("x", new int[]{1}));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.execute("x", new String[]{"c"}));
    }

    @Test
    void namedCursorsAreUnsupported() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> stmt().setCursorName("c"));
    }

    @Test
    void onlyForwardFetchDirectionIsAllowed() throws SQLException {
        ChStatement s = stmt();
        s.setFetchDirection(ResultSet.FETCH_FORWARD); // allowed, no throw
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> s.setFetchDirection(ResultSet.FETCH_UNKNOWN));
    }

    @Test
    void wrapperContract() throws SQLException {
        ChStatement s = stmt();
        assertTrue(s.isWrapperFor(Statement.class));
        assertSame(s, s.unwrap(Statement.class));
        assertFalse(s.isWrapperFor(String.class));
        assertThrows(SQLException.class, () -> s.unwrap(String.class));
    }
}
