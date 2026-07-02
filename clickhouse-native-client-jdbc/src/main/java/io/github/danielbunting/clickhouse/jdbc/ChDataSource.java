package io.github.danielbunting.clickhouse.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A minimal {@link DataSource} over the native-TCP driver. It holds a
 * {@code jdbc:chnative://...} URL plus optional connection {@link Properties} and
 * hands out {@link ChConnection}s by delegating to {@link ClickHouseDriver#connect}.
 *
 * <p>This is a thin, non-pooling data source intended for frameworks and tooling that
 * discover a driver through {@code javax.sql.DataSource}. Connection pooling is
 * provided separately by the core client's pool; a pooling {@code DataSource} may be
 * added later.
 */
public final class ChDataSource implements DataSource {

    private final ClickHouseDriver driver = new ClickHouseDriver();
    private final String url;
    private final Properties properties;

    private PrintWriter logWriter;
    private int loginTimeoutSeconds;

    /**
     * Creates a data source for the given URL with no extra properties.
     *
     * @param url a {@code jdbc:chnative://host:port/db} URL
     */
    public ChDataSource(String url) {
        this(url, new Properties());
    }

    /**
     * Creates a data source for the given URL and default connection properties.
     *
     * @param url        a {@code jdbc:chnative://host:port/db} URL
     * @param properties default connection properties (copied defensively; may be {@code null})
     */
    public ChDataSource(String url, Properties properties) {
        if (url == null || !driver.acceptsURL(url)) {
            throw new IllegalArgumentException("Not a clickhouse-native JDBC URL: " + url);
        }
        this.url = url;
        this.properties = new Properties();
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }

    /** {@return the JDBC URL this data source connects to} */
    public String getUrl() {
        return url;
    }

    @Override
    public Connection getConnection() throws SQLException {
        // A real entry copy — not new Properties(properties), which stores the
        // configured entries only as defaults that Properties.isEmpty()/keys() ignore.
        Properties copy = new Properties();
        copy.putAll(properties);
        return open(copy);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties merged = new Properties();
        merged.putAll(properties);
        if (username != null) {
            merged.setProperty("user", username);
        }
        if (password != null) {
            merged.setProperty("password", password);
        }
        return open(merged);
    }

    private Connection open(Properties info) throws SQLException {
        Connection conn = driver.connect(url, info);
        if (conn == null) {
            throw new SQLException("URL not accepted by driver: " + url);
        }
        return conn;
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        this.loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeoutSeconds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger is not supported");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
