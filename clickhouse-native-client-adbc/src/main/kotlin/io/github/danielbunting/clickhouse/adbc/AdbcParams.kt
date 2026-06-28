package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConfig
import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.adbc.core.AdbcException
import java.util.Properties

/**
 * Translates an ADBC parameter map into a [ClickHouseConfig].
 *
 * Two ways to point at a server, checked in order:
 * 1. A connection URI under the standard [AdbcDriver.PARAM_URI] key — routed through
 *    [ClickHouseConfig.fromUrl] (`chnative://host:port/db?...`).
 *    [AdbcDriver.PARAM_USERNAME]/[AdbcDriver.PARAM_PASSWORD] override URL credentials.
 * 2. Discrete [PARAM_HOST]/[PARAM_PORT]/[PARAM_DATABASE] plus username/password — built via
 *    [ClickHouseConfig.builder].
 */
public object AdbcParams {

    /** Discrete host parameter key (used when no URI is supplied). */
    public const val PARAM_HOST: String = "adbc.clickhouse.host"

    /** Discrete port parameter key; value may be an [Int] or numeric [String]. */
    public const val PARAM_PORT: String = "adbc.clickhouse.port"

    /** Target database parameter key. */
    public const val PARAM_DATABASE: String = "adbc.clickhouse.database"

    /** Builds a [ClickHouseConfig] from an ADBC parameter map. */
    @JvmStatic
    public fun toConfig(parameters: Map<String, Any?>): ClickHouseConfig {
        val username = AdbcDriver.PARAM_USERNAME.get(parameters)
        val password = AdbcDriver.PARAM_PASSWORD.get(parameters)
        val uri = AdbcDriver.PARAM_URI.get(parameters)

        if (uri != null) {
            val info = Properties()
            if (username != null) info.setProperty("user", username)
            if (password != null) info.setProperty("password", password)
            return try {
                ClickHouseConfig.fromUrl(uri, info)
            } catch (e: RuntimeException) {
                throw AdbcException.invalidArgument(
                    "Invalid ClickHouse connection URI '$uri': ${e.message}"
                ).withCause(e)
            }
        }

        val host = parameters[PARAM_HOST] as String?
            ?: throw AdbcException.invalidArgument(
                "Missing connection target: set '${AdbcDriver.PARAM_URI.key}' or '$PARAM_HOST'"
            )
        val builder = ClickHouseConfig.builder().host(host)
        intParam(parameters[PARAM_PORT])?.let { builder.port(it) }
        (parameters[PARAM_DATABASE] as String?)?.let { builder.database(it) }
        username?.let { builder.username(it) }
        password?.let { builder.password(it) }
        return try {
            builder.build()
        } catch (e: RuntimeException) {
            throw AdbcException.invalidArgument("Invalid ClickHouse connection parameters: ${e.message}")
                .withCause(e)
        }
    }

    private fun intParam(value: Any?): Int? = when (value) {
        null -> null
        is Number -> value.toInt()
        is String -> value.toIntOrNull()
            ?: throw AdbcException.invalidArgument("'$PARAM_PORT' must be an integer, got '$value'")
        else -> throw AdbcException.invalidArgument("'$PARAM_PORT' must be an integer, got '$value'")
    }
}
