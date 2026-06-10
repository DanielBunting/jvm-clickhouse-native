package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.compress.CompressionMethod
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

class ConfigDslTest {

    @Test
    fun buildsConfigFromDsl() {
        val config = clickHouseConfig {
            host = "ch.example.com"
            port = 9100
            database = "analytics"
            username = "analyst"
            password = "s3cr3t"
            compression = CompressionMethod.ZSTD
            insertBatchSize = 65_536
            connectTimeout = Duration.ofSeconds(10)
            socketTimeout = Duration.ofSeconds(30)
            settings["max_threads"] = "4"
            settings["async_insert"] = "1"
        }

        assertEquals("ch.example.com", config.host())
        assertEquals(9100, config.port())
        assertEquals("analytics", config.database())
        assertEquals("analyst", config.username())
        assertEquals("s3cr3t", config.password())
        assertEquals(CompressionMethod.ZSTD, config.compression())
        assertEquals(65_536, config.insertBatchSize())
        assertEquals(Duration.ofSeconds(10), config.connectTimeout())
        assertEquals(Duration.ofSeconds(30), config.socketTimeout())
        assertEquals("4", config.settings()["max_threads"])
        assertEquals("1", config.settings()["async_insert"])
    }

    @Test
    fun unsetPropertiesKeepBuilderDefaults() {
        val explicit = clickHouseConfig { host = "h"; port = 9000 }
        val defaults = ClickHouseConfigDefaults.reference()

        // Properties we never touched should match a plain builder build().
        assertEquals(defaults.compression(), explicit.compression())
        assertEquals(defaults.insertBatchSize(), explicit.insertBatchSize())
        assertEquals(defaults.database(), explicit.database())
    }
}

private object ClickHouseConfigDefaults {
    fun reference() = io.github.danielbunting.clickhouse.ClickHouseConfig.builder()
        .host("h").port(9000).build()
}
