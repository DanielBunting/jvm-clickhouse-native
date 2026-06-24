package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;

import java.util.Properties;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * Shared JMH benchmark state owning the ClickHouse server under test.
 *
 * <p>This is a {@link Scope#BENCHMARK benchmark-scoped} state object: a single
 * instance is shared by every thread of a benchmark trial. The lifecycle is:</p>
 * <ul>
 *   <li>{@link #setUp()} runs once at {@link Level#TRIAL} start. If the system
 *       property {@code ch.host} is set, that external server is used (ports come
 *       from {@code ch.nativePort}, default 9000, and {@code ch.httpPort}, default
 *       8123). Otherwise a Testcontainer on {@link ClickHouseImages#SERVER}
 *       is started exposing both 9000 and 8123, with the {@code default} user opened
 *       to all networks via an inlined {@code users.d} override.</li>
 *   <li>{@link #tearDown()} runs once at {@link Level#TRIAL} end and stops the
 *       container (a no-op for an external host).</li>
 * </ul>
 *
 * <p>Benchmark classes (B1/B2/B3) take this state as a parameter and call
 * {@link #openNative(CompressionMethod)} / {@link #competitorProps()} to obtain
 * clients, and {@link #recreateTable(ClickHouseConnection)} from their own
 * {@code @Setup} to start each iteration from a clean table.</p>
 */
@State(Scope.Benchmark)
public class ClickHouseResource {

    /** ClickHouse server image used when no external host is supplied (centralized in {@link ClickHouseImages}). */
    private static final String IMAGE = ClickHouseImages.SERVER;

    /** Default native protocol port. */
    private static final int DEFAULT_NATIVE_PORT = 9000;

    /** Default HTTP protocol port. */
    private static final int DEFAULT_HTTP_PORT = 8123;

    /**
     * {@code users.d} override that lets the {@code default} user (no password)
     * connect from any network, so both the native client and the competitor
     * drivers can authenticate against the container.
     */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks></default></users></clickhouse>";

    /** Container instance, or {@code null} when an external host is used. */
    private GenericContainer<?> container;

    private String host;
    private int nativePort;
    private int httpPort;

    /**
     * Starts (or attaches to) the ClickHouse server for this trial.
     */
    @Setup(Level.Trial)
    public void setUp() {
        String externalHost = System.getProperty("ch.host");
        if (externalHost != null && !externalHost.isBlank()) {
            this.host = externalHost;
            this.nativePort = Integer.getInteger("ch.nativePort", DEFAULT_NATIVE_PORT);
            this.httpPort = Integer.getInteger("ch.httpPort", DEFAULT_HTTP_PORT);
            return;
        }

        this.container = new GenericContainer<>(IMAGE)
                .withExposedPorts(DEFAULT_NATIVE_PORT, DEFAULT_HTTP_PORT)
                .withCopyToContainer(
                        Transferable.of(OPEN_DEFAULT_USER_XML),
                        "/etc/clickhouse-server/users.d/zz-open-default.xml")
                .waitingFor(Wait.forListeningPort());
        this.container.start();
        this.host = this.container.getHost();
        this.nativePort = this.container.getMappedPort(DEFAULT_NATIVE_PORT);
        this.httpPort = this.container.getMappedPort(DEFAULT_HTTP_PORT);
    }

    /**
     * Stops the container if this trial started one.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        if (this.container != null) {
            this.container.stop();
            this.container = null;
        }
    }

    /**
     * @return the reachable host name or IP of the ClickHouse server
     */
    public String host() {
        return host;
    }

    /**
     * @return the (possibly mapped) native protocol port
     */
    public int nativePort() {
        return nativePort;
    }

    /**
     * @return the (possibly mapped) HTTP protocol port
     */
    public int httpPort() {
        return httpPort;
    }

    /**
     * Opens a native client connection to the server under test.
     *
     * @param compression the compression method to negotiate
     * @return a freshly opened {@link ClickHouseConnection}; the caller owns it and
     *         must close it
     */
    public ClickHouseConnection openNative(CompressionMethod compression) {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host(host)
                .port(nativePort)
                .database("default")
                .username("default")
                .password("")
                .compression(compression)
                .build();
        return ClickHouseConnection.open(cfg);
    }

    /**
     * Opens an official {@code client-v2} client ({@code com.clickhouse.client.api.Client},
     * HTTP transport) against the server under test.
     *
     * @return a freshly built client; the caller owns it and must close it
     */
    public com.clickhouse.client.api.Client openV2Client() {
        return new com.clickhouse.client.api.Client.Builder()
                .addEndpoint("http://" + host + ":" + httpPort)
                .setUsername("default")
                .setPassword("")
                .setDefaultDatabase("default")
                .build();
    }

    /**
     * Builds the JDBC connection properties for the competitor drivers
     * ({@code default} user, empty password).
     *
     * @return a fresh {@link Properties} instance per call
     */
    public Properties competitorProps() {
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");
        return props;
    }

    /**
     * Drops and recreates the {@code bench} table, leaving it empty and ready for a
     * fresh insert benchmark.
     *
     * @param conn an open native connection to the server under test
     */
    public void recreateTable(ClickHouseConnection conn) {
        conn.execute("DROP TABLE IF EXISTS " + SyntheticData.TABLE);
        conn.execute(SyntheticData.DDL);
    }
}
