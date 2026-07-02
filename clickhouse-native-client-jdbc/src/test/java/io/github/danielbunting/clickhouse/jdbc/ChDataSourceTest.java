package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * Server-free unit tests for {@link ChDataSource} configuration and the
 * {@code DataSource} contract. Actual connection acquisition is covered by the
 * integration suite.
 */
class ChDataSourceTest {

    private static final String URL = "jdbc:chnative://localhost:9000/default";

    @Test
    void rejectsNonClickHouseUrl() {
        assertThrows(IllegalArgumentException.class, () -> new ChDataSource("jdbc:postgresql://x/y"));
        assertThrows(IllegalArgumentException.class, () -> new ChDataSource(null));
    }

    @Test
    void retainsUrl() {
        assertEquals(URL, new ChDataSource(URL).getUrl());
        assertEquals(URL, new ChDataSource(URL, new Properties()).getUrl());
    }

    @Test
    void loginTimeoutRoundTrips() {
        ChDataSource ds = new ChDataSource(URL);
        assertEquals(0, ds.getLoginTimeout());
        ds.setLoginTimeout(15);
        assertEquals(15, ds.getLoginTimeout());
    }

    @Test
    void logWriterRoundTrips() {
        ChDataSource ds = new ChDataSource(URL);
        assertEquals(null, ds.getLogWriter());
        PrintWriter w = new PrintWriter(System.out);
        ds.setLogWriter(w);
        assertSame(w, ds.getLogWriter());
    }

    @Test
    void parentLoggerUnsupported() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> new ChDataSource(URL).getParentLogger());
    }

    @Test
    void getConnectionToUnreachableEndpointThrows() {
        // Exercises getConnection()/getConnection(user,pass) + the open() delegate path
        // without a server: a closed local port refuses fast.
        ChDataSource ds = new ChDataSource("jdbc:chnative://127.0.0.1:1/default");
        assertThrows(SQLException.class, ds::getConnection);
        assertThrows(SQLException.class, () -> ds.getConnection("user", "pass"));
    }

    @Test
    void wrapperContract() throws SQLException {
        ChDataSource ds = new ChDataSource(URL);
        assertTrue(ds.isWrapperFor(DataSource.class));
        assertSame(ds, ds.unwrap(DataSource.class));
        assertFalse(ds.isWrapperFor(String.class));
        assertThrows(SQLException.class, () -> ds.unwrap(String.class));
    }
}
