package io.github.danielbunting.clickhouse.adbc;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.junit.jupiter.api.Tag;

/**
 * Shared Testcontainers harness for the ADBC integration suite. Uses the <em>singleton
 * container</em> pattern: one ClickHouse server (version from {@code ClickHouseImages.SERVER},
 * overridable via {@code -Dch.image}) is started once per JVM and reused across every IT class,
 * with Ryuk reaping it at exit. This avoids booting one server per class — several native
 * servers coming up at once race the handshake and intermittently fail to connect.
 */
@Tag("integration")
abstract class AdbcIntegrationTest {

    /** Native-TCP protocol port. */
    static final int NATIVE_PORT = 9000;

    /**
     * Users override that opens the default user to all networks (IPv4/IPv6) and grants it
     * access management, so session tests can CREATE USER / ROLE / ROW POLICY via plain DDL.
     */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks>"
            + "<access_management>1</access_management>"
            + "</default></users></clickhouse>";

    @SuppressWarnings("resource")
    static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(ClickHouseImages.SERVER)
                    .withExposedPorts(NATIVE_PORT)
                    .withCopyToContainer(
                            Transferable.of(OPEN_DEFAULT_USER_XML),
                            "/etc/clickhouse-server/users.d/zz-open-default.xml")
                    .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

    static {
        CLICKHOUSE.start();
    }

    /** Discrete host/port ADBC parameters pointing at the test container. */
    protected static Map<String, Object> connectParams() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, CLICKHOUSE.getHost());
        params.put(AdbcParams.PARAM_PORT, CLICKHOUSE.getMappedPort(NATIVE_PORT));
        return params;
    }

    /** The same target expressed as a {@code chnative://} URI parameter. */
    protected static Map<String, Object> uriParams() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(
                params,
                "chnative://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(NATIVE_PORT));
        return params;
    }

    protected static String uniqueTable(String prefix) {
        return prefix + "_" + System.nanoTime();
    }
}
