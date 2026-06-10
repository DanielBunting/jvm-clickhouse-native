package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.compress.CompressionMethod
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.time.Duration

/**
 * Marks the [ClickHouseConfigDsl] receiver so nested DSL blocks can't accidentally
 * pull in members of an outer receiver.
 */
@DslMarker
annotation class ClickHouseDsl

/**
 * Kotlin builder for [ClickHouseConfig]. Only the properties you set are forwarded to the
 * underlying [ClickHouseConfig.Builder]; everything left unset keeps the Java builder's
 * defaults. Obtain one via [clickHouseConfig].
 */
@ClickHouseDsl
class ClickHouseConfigDsl internal constructor() {
    var host: String? = null
    var port: Int? = null
    var database: String? = null
    var username: String? = null
    var password: String? = null
    var accessToken: String? = null
    var compression: CompressionMethod? = null
    var insertBatchSize: Int? = null
    var connectTimeout: Duration? = null
    var socketTimeout: Duration? = null
    var queryTimeout: Duration? = null
    var tls: Boolean? = null

    /** Per-query / connection server settings (insertion order preserved). */
    val settings: MutableMap<String, String> = LinkedHashMap()

    private val endpoints: MutableList<Pair<String, Int>> = ArrayList()

    /** Adds an endpoint for connect-time failover / load balancing. */
    fun endpoint(host: String, port: Int) {
        endpoints += host to port
    }

    internal fun build(): ClickHouseConfig {
        val b = ClickHouseConfig.builder()
        host?.let(b::host)
        port?.let(b::port)
        database?.let(b::database)
        username?.let(b::username)
        password?.let(b::password)
        accessToken?.let(b::accessToken)
        compression?.let(b::compression)
        insertBatchSize?.let(b::insertBatchSize)
        connectTimeout?.let(b::connectTimeout)
        socketTimeout?.let(b::socketTimeout)
        queryTimeout?.let(b::queryTimeout)
        tls?.let(b::tls)
        endpoints.forEach { (h, p) -> b.endpoint(h, p) }
        settings.forEach { (k, v) -> b.setting(k, v) }
        return b.build()
    }
}

/**
 * Builds a [ClickHouseConfig] with a Kotlin DSL:
 *
 * ```
 * val config = clickHouseConfig {
 *     host = "localhost"
 *     port = 9000
 *     database = "analytics"
 *     compression = CompressionMethod.LZ4
 *     settings["max_threads"] = "4"
 * }
 * ```
 */
fun clickHouseConfig(block: ClickHouseConfigDsl.() -> Unit): ClickHouseConfig =
    ClickHouseConfigDsl().apply(block).build()

/**
 * Opens a [ClickHouseConnection] for this config, running the (blocking) socket connect and
 * handshake on [dispatcher] so the calling coroutine's thread is never blocked.
 *
 * The returned connection is a single, stateful, **not thread-safe** socket — use it from one
 * coroutine at a time (concurrent use is rejected by the core's connection guard). Close it with
 * Kotlin's `use { }` when done.
 */
suspend fun ClickHouseConfig.connect(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): ClickHouseConnection =
    withContext(dispatcher) { ClickHouseConnection.open(this@connect) }
