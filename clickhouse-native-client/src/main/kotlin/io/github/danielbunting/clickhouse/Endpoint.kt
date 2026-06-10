package io.github.danielbunting.clickhouse

/**
 * A single `host:port` ClickHouse endpoint.
 *
 * Immutable value object used by [ClickHouseConfig] to describe one or more
 * candidate servers for connecting. When a config lists several endpoints, the driver
 * tries them in a [LoadBalancingPolicy]-determined order, failing over to the next
 * on a connect-time [ConnectionException].
 *
 * @property host the hostname or IP (must be non-null, non-blank)
 * @property port the TCP port in `[1, 65535]`
 */
@JvmRecord
public data class Endpoint(val host: String, val port: Int) {

    init {
        if (host.isBlank()) {
            throw ConfigurationException("Endpoint host must not be null or blank")
        }
        if (port < 1 || port > 65535) {
            throw ConfigurationException(
                "Endpoint port out of range [1, 65535]: $port"
            )
        }
    }

    override fun toString(): String = "$host:$port"
}
