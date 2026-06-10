package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.kotlin.clickHouseConfig
import io.github.danielbunting.clickhouse.test.ClickHouseImages
import org.junit.jupiter.api.Tag
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Base for Kotlin integration tests: starts one real ClickHouse server per subclass via
 * Testcontainers and exposes a [config] pointing at it.
 *
 * Like the JDBC module's smoke test, this reuses only [ClickHouseImages] (the cross-module
 * `testFixtures` constant that resolves the image and honours `-Dch.image=<version>`); the core
 * module's own `ClickHouseContainer`/`IntegrationTestBase` live in its private test source set and
 * are not visible here, so the container is stood up locally.
 *
 * The stock image restricts the `default` user to localhost; Testcontainers reaches the server via
 * a mapped (non-loopback) port, so a small users override opens `default` to all networks.
 *
 * Run with: `./gradlew :clickhouse-native-client-kotlin:integrationTest`
 */
@Tag("integration")
@Testcontainers
abstract class ClickHouseIntegrationTest {

    protected fun config(): ClickHouseConfig = clickHouseConfig {
        host = clickhouse.host
        port = clickhouse.getMappedPort(NATIVE_PORT)
    }

    /** Unique table name so methods/classes running in parallel never collide. */
    protected fun uniqueTable(prefix: String): String = prefix + "_" + System.nanoTime()

    companion object {
        private const val NATIVE_PORT = 9000

        private const val OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">" +
                "<ip>::/0</ip></networks></default></users></clickhouse>"

        @JvmStatic
        @Container
        val clickhouse: GenericContainer<*> = GenericContainer(ClickHouseImages.SERVER).apply {
            withExposedPorts(NATIVE_PORT)
            withCopyToContainer(
                Transferable.of(OPEN_DEFAULT_USER_XML),
                "/etc/clickhouse-server/users.d/zz-open-default.xml",
            )
            waitingFor(Wait.forListeningPort())
        }
    }
}
