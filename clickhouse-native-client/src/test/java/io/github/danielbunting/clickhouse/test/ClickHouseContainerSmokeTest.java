package io.github.danielbunting.clickhouse.test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Socket;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Trivial integration smoke-test that verifies:
 * <ol>
 *   <li>The ClickHouse container starts successfully.</li>
 *   <li>The native TCP port (9000) is reachable from the test JVM.</li>
 * </ol>
 *
 * <p>This test makes no driver calls — the {@code ClickHouseConnection}
 * implementation is delivered in Wave 2 (task W2). It is deliberately minimal
 * so that the integration harness itself can be validated independently.
 *
 * <p>Tagged {@code "integration"} so that the default {@code test} task
 * (offline) skips it; run via:
 * <pre>
 *   ./gradlew :clickhouse-native-client:integrationTest
 * </pre>
 */
@Tag("integration")
class ClickHouseContainerSmokeTest extends IntegrationTestBase {

    /**
     * Asserts that the container is running and that a raw TCP socket can be
     * opened to the mapped native port.
     *
     * @throws IOException if the TCP connection attempt itself fails unexpectedly
     */
    @Test
    void containerStartsAndPortIsReachable() throws IOException {
        assertNotNull(clickHouseHost(),
                "Container host must be non-null after startup");
        assertTrue(CLICK_HOUSE.isRunning(),
                "ClickHouse container must report itself as running");

        int port = clickHousePort();
        assertTrue(port > 0, "Mapped native port must be a positive number");

        // Open a raw TCP socket to confirm the port is truly accepting connections.
        try (Socket socket = new Socket(clickHouseHost(), port)) {
            assertTrue(socket.isConnected(),
                    "TCP socket must connect successfully to the native port");
        }
    }
}
