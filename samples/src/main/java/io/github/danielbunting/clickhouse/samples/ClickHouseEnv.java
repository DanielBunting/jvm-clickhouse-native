package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;

/**
 * Convenience factory that builds a {@link ClickHouseConfig} from environment
 * variables (with system-property fallback) so every sample needs only a
 * single {@code ClickHouseEnv.open()} call.
 *
 * <h2>Supported environment variables</h2>
 * <table border="1">
 *   <tr><th>Variable</th><th>System property</th><th>Default</th></tr>
 *   <tr><td>{@code CH_HOST}</td><td>{@code ch.host}</td><td>{@code localhost}</td></tr>
 *   <tr><td>{@code CH_PORT}</td><td>{@code ch.port}</td><td>{@code 9000}</td></tr>
 *   <tr><td>{@code CH_DB}</td><td>{@code ch.db}</td><td>{@code default}</td></tr>
 *   <tr><td>{@code CH_USER}</td><td>{@code ch.user}</td><td>{@code demo}</td></tr>
 *   <tr><td>{@code CH_PASSWORD}</td><td>{@code ch.password}</td><td>{@code demo}</td></tr>
 * </table>
 *
 * <p>Each sample should connect via {@code ClickHouseConnection.open(ClickHouseEnv.config())}
 * or the shorthand {@link #open()}.
 */
public final class ClickHouseEnv {

    private ClickHouseEnv() {
        // utility class
    }

    /**
     * Reads connection settings from the environment (or system properties) and
     * returns a fully built {@link ClickHouseConfig}.
     *
     * <p>Lookup order for each setting:
     * <ol>
     *   <li>Environment variable (e.g. {@code CH_HOST})</li>
     *   <li>System property (e.g. {@code -Dch.host=…})</li>
     *   <li>Hard-coded default</li>
     * </ol>
     *
     * @return a {@link ClickHouseConfig} ready for {@link ClickHouseConnection#open}
     */
    public static ClickHouseConfig config() {
        String host = resolve("CH_HOST", "ch.host", "localhost");
        int port = Integer.parseInt(resolve("CH_PORT", "ch.port", "9000"));
        String database = resolve("CH_DB", "ch.db", "default");
        String username = resolve("CH_USER", "ch.user", "demo");
        String password = resolve("CH_PASSWORD", "ch.password", "demo");

        return ClickHouseConfig.builder()
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                .build();
    }

    /**
     * Opens a {@link ClickHouseConnection} using the configuration returned by
     * {@link #config()}.
     *
     * <p>Callers are responsible for closing the connection, preferably in a
     * try-with-resources block.
     *
     * @return an open {@link ClickHouseConnection}
     */
    public static ClickHouseConnection open() {
        return ClickHouseConnection.open(config());
    }

    /**
     * Resolves a configuration value by checking the environment variable first,
     * then the system property, and finally returning the given default.
     *
     * @param envKey      the environment variable name (e.g. {@code "CH_HOST"})
     * @param sysPropKey  the system property name (e.g. {@code "ch.host"})
     * @param defaultValue the fallback value when neither source is set
     * @return the resolved value; never {@code null}
     */
    private static String resolve(String envKey, String sysPropKey, String defaultValue) {
        String value = System.getenv(envKey);
        if (value != null && !value.isBlank()) {
            return value;
        }
        value = System.getProperty(sysPropKey);
        if (value != null && !value.isBlank()) {
            return value;
        }
        return defaultValue;
    }
}
