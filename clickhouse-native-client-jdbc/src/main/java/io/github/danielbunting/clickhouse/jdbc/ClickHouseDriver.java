package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC {@link Driver} for the native-TCP ClickHouse client.
 *
 * <p>This driver is a thin wrapper over the core
 * {@link io.github.danielbunting.clickhouse.ClickHouseConnection}. It accepts URLs of the form
 * {@code jdbc:chnative://host:9000/db?params}. The leading {@code "jdbc:"} prefix is stripped to
 * obtain a {@code chnative://...} URL understood by the core
 * {@link io.github.danielbunting.clickhouse.ClickHouseConfig#fromUrl(String, Properties)}.
 *
 * <p>The driver registers itself with the {@link DriverManager} via a static initializer, and is
 * also discoverable through the {@code java.sql.Driver} {@code ServiceLoader} entry under
 * {@code META-INF/services}. As such, {@code Class.forName} is not required by callers.
 *
 * <p>This driver is not fully JDBC-compliant ({@link #jdbcCompliant()} returns {@code false})
 * because ClickHouse does not support the complete JDBC/SQL feature set (notably transactions).
 */
public final class ClickHouseDriver implements Driver {

    /** URL prefix accepted by this driver. */
    private static final String JDBC_PREFIX = "jdbc:chnative://";

    /** Length of the {@code "jdbc:"} prefix stripped to obtain the core URL. */
    private static final String JDBC_SCHEME = "jdbc:";

    /** Major version of this driver. */
    private static final int MAJOR_VERSION = 1;

    /** Minor version of this driver. */
    private static final int MINOR_VERSION = 0;

    static {
        try {
            DriverManager.registerDriver(new ClickHouseDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Connects to ClickHouse using the given JDBC URL and connection properties.
     *
     * <p>Returns {@code null} if the URL is not accepted by this driver, as mandated by the
     * {@link Driver} contract (so the {@link DriverManager} may try the next driver).
     *
     * @param url  a {@code jdbc:chnative://...} URL
     * @param info connection properties; {@code user}/{@code username} and {@code password} keys,
     *             if present, override credentials embedded in the URL
     * @return a new {@link ChConnection}, or {@code null} if {@code url} is not accepted
     * @throws SQLException if a connection cannot be opened
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        // Strip the leading "jdbc:" to obtain a chnative:// URL the core understands.
        String coreUrl = url.substring(JDBC_SCHEME.length());
        try {
            ClickHouseConfig config = ClickHouseConfig.fromUrl(coreUrl, info);
            ClickHouseConnection core = ClickHouseConnection.open(config);
            return new ChConnection(core, url, info, config.database());
        } catch (RuntimeException e) {
            throw new SQLException("Failed to connect to ClickHouse via '" + url + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * Reports whether this driver can handle the given URL.
     *
     * @param url the URL to test (may be {@code null})
     * @return {@code true} if {@code url} starts with {@code "jdbc:chnative://"}
     */
    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(JDBC_PREFIX);
    }

    /**
     * Returns an empty property-info array; this driver derives all configuration from the URL
     * and the supplied {@link Properties}.
     *
     * @param url  the connection URL (ignored)
     * @param info the proposed connection properties (ignored)
     * @return an empty {@link DriverPropertyInfo} array
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    /**
     * {@return the driver's major version}
     */
    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /**
     * {@return the driver's minor version}
     */
    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    /**
     * {@return {@code false}; this driver is not fully JDBC-compliant because ClickHouse lacks
     * full transaction support}
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Parent logger access is not supported.
     *
     * @return never returns normally
     * @throws SQLFeatureNotSupportedException always
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger is not supported");
    }
}
