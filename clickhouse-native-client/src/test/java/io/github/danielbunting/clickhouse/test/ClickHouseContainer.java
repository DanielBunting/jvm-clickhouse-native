package io.github.danielbunting.clickhouse.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * A Testcontainers {@link GenericContainer} that starts a ClickHouse server
 * using the image resolved by {@link ClickHouseImages#SERVER} (the default
 * version, or a {@code -Dch.image} override) and exposes the native TCP port
 * (9000) plus the HTTP port (8123, used by the official ClickHouse Java
 * client in the cross-client round-trip tests).
 *
 * <p>The container is considered ready once TCP connections to both exposed
 * ports can be established — no HTTP request or JDBC dependency is required.
 *
 * <p>Intended for use by {@link IntegrationTestBase} and any integration test
 * that is annotated with {@code @Tag("integration")}.
 *
 * <p>Example (manual lifecycle):
 * <pre>{@code
 * try (ClickHouseContainer ch = new ClickHouseContainer()) {
 *     ch.start();
 *     int port = ch.getMappedNativePort();
 *     // ... connect via raw TCP
 * }
 * }</pre>
 */
public class ClickHouseContainer extends GenericContainer<ClickHouseContainer> {

    /** Docker image used for all integration tests (centralized in {@link ClickHouseImages}). */
    public static final String IMAGE = ClickHouseImages.SERVER;

    /** ClickHouse native TCP port (server-side). */
    public static final int NATIVE_PORT = 9000;

    /** ClickHouse HTTP port (server-side) — used by the official Java client. */
    public static final int HTTP_PORT = 8123;

    /**
     * The official image ships {@code users.d/default-user.xml} restricting the
     * {@code default} user to localhost ({@code ::1}, {@code 127.0.0.1}).
     * Testcontainers connects from the Docker gateway address, so without this
     * the {@code default} user is rejected with AUTHENTICATION_FAILED (code 516).
     * This override re-opens the {@code default} user to all networks for tests.
     */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse>\n"
            + "  <users>\n"
            + "    <default>\n"
            + "      <networks replace=\"replace\"><ip>::/0</ip></networks>\n"
            + "    </default>\n"
            + "  </users>\n"
            + "</clickhouse>\n";

    /**
     * Creates a new container backed by {@value #IMAGE} and waits for the
     * native TCP port to accept connections before declaring the container ready.
     */
    public ClickHouseContainer() {
        super(IMAGE);
        withExposedPorts(NATIVE_PORT, HTTP_PORT);
        // Re-open the `default` user to all networks (see OPEN_DEFAULT_USER_XML);
        // the file name sorts after the image's default-user.xml so it wins.
        withCopyToContainer(
                Transferable.of(OPEN_DEFAULT_USER_XML),
                "/etc/clickhouse-server/users.d/zz-open-default.xml");
        // Wait until both TCP ports are accepting connections (the server opens
        // 9000 and 8123 in the same startup phase) — no HTTP request needed.
        waitingFor(Wait.forListeningPort());
    }

    /**
     * Returns the host-side port that is mapped to the ClickHouse native TCP
     * port ({@value #NATIVE_PORT}) inside the container.
     *
     * @return the mapped port number (ephemeral, assigned by Docker)
     */
    public int getMappedNativePort() {
        return getMappedPort(NATIVE_PORT);
    }

    /**
     * Returns the host-side port that is mapped to the ClickHouse HTTP port
     * ({@value #HTTP_PORT}) inside the container.
     *
     * @return the mapped port number (ephemeral, assigned by Docker)
     */
    public int getMappedHttpPort() {
        return getMappedPort(HTTP_PORT);
    }
}
