package io.github.danielbunting.clickhouse.test;

import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for integration tests that require a live ClickHouse server.
 *
 * <p>Subclasses are automatically tagged with {@code "integration"} so that
 * the Gradle {@code test} task (which excludes that tag) keeps the offline
 * unit-test build fast, while the dedicated {@code integrationTest} task
 * includes them.
 *
 * <p>The {@link ClickHouseContainer} is declared as a shared static field so
 * Testcontainers will start one container per test class and reuse it across
 * all test methods in that class.
 *
 * <p>Subclasses can access the container host via {@link #clickHouseHost()} and
 * the mapped native port via {@link #clickHousePort()}.
 *
 * <p>Example:
 * <pre>{@code
 * class MyIT extends IntegrationTestBase {
 *     @Test
 *     void shouldConnect() {
 *         // connect to clickHouseHost():clickHousePort()
 *     }
 * }
 * }</pre>
 */
@Tag("integration")
@Testcontainers
public abstract class IntegrationTestBase {

    /** Shared ClickHouse container, started once per subclass test run. */
    @Container
    protected static final ClickHouseContainer CLICK_HOUSE =
            new ClickHouseContainer();

    /**
     * Returns the Docker host address at which the ClickHouse container is
     * reachable from the test JVM.
     *
     * @return the container host string (e.g. {@code "localhost"})
     */
    protected static String clickHouseHost() {
        return CLICK_HOUSE.getHost();
    }

    /**
     * Returns the host-side port mapped to the ClickHouse native TCP port
     * ({@value ClickHouseContainer#NATIVE_PORT}).
     *
     * @return the mapped port number assigned by Docker
     */
    protected static int clickHousePort() {
        return CLICK_HOUSE.getMappedNativePort();
    }
}
