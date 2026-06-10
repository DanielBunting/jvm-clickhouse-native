package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.ConnectionException
import io.github.danielbunting.clickhouse.Endpoint
import java.util.function.Function

/**
 * Opens a [ClickHouseConnection] with connect-time failover across the configured
 * [endpoints][Endpoint].
 *
 * It asks an [EndpointSelector] for the policy-ordered attempt sequence, then tries
 * each endpoint in turn: the first that connects wins; a connect-time
 * [ConnectionException] moves on to the next; if every endpoint fails, it throws a
 * single aggregated [ConnectionException] listing each attempt and its cause (the
 * individual failures are attached as suppressed exceptions).
 *
 * **Testability without sockets.** The actual per-endpoint open is supplied as an
 * injected [Function]`<Endpoint, ClickHouseConnection>`. Production code
 * passes a function that builds a real [NativeClientImpl]; unit tests pass a fake that
 * throws for the dead endpoints and returns a stub connection for the live one, so failover
 * order and aggregation are verified with no real network I/O.
 *
 * Only connect-time failover is in scope. A failure that occurs *after* a
 * connection is established (mid-query/mid-insert) is not retried here — the caller sees the
 * original error.
 */
public class FailoverConnector
/**
 * @param selector the endpoint selector (provides the policy-ordered attempt sequence)
 * @param connect  opens a connection to one endpoint; called once per attempted endpoint
 */
public constructor(
    private val selector: EndpointSelector,
    private val connect: Function<Endpoint, ClickHouseConnection>,
) {

    /**
     * Attempts the endpoints in policy order and returns the first connection that opens.
     *
     * @return a live connection
     * @throws ConnectionException if every configured endpoint fails to connect; the message
     *                             lists each attempt and the individual failures are attached
     *                             as suppressed exceptions
     */
    public fun open(): ClickHouseConnection {
        val order = selector.attemptOrder()
        var aggregate: ConnectionException? = null
        for (endpoint in order) {
            try {
                return connect.apply(endpoint)
            } catch (e: ConnectionException) {
                aggregate = recordFailure(aggregate, order, endpoint, e)
            }
        }
        // order is never empty (EndpointSelector rejects empty endpoint lists), so at least
        // one attempt ran and aggregate is non-null here.
        throw aggregate!!
    }

    public companion object {

        /**
         * Builds a connector for [config] that opens real [NativeClientImpl]-backed
         * connections, using a freshly constructed [EndpointSelector].
         *
         * @param config the connection configuration
         * @return a connector ready to [open]
         */
        @JvmStatic
        public fun forConfig(config: ClickHouseConfig): FailoverConnector {
            return forConfig(config, EndpointSelector(config.endpoints(), config.loadBalancingPolicy()))
        }

        /**
         * Builds a connector for [config] that opens real connections but reuses a shared
         * [selector] — used by the pool so round-robin/random rotation spreads across nodes
         * over the pool's lifetime rather than resetting on every open.
         *
         * @param config   the connection configuration
         * @param selector the shared endpoint selector
         * @return a connector ready to [open]
         */
        @JvmStatic
        public fun forConfig(config: ClickHouseConfig, selector: EndpointSelector): FailoverConnector {
            return FailoverConnector(
                selector,
                Function { endpoint -> ClickHouseConnectionImpl(NativeClientImpl(config, endpoint)) }
            )
        }

        private fun recordFailure(
            aggregate: ConnectionException?,
            order: List<Endpoint>,
            failed: Endpoint,
            cause: ConnectionException,
        ): ConnectionException {
            var result = aggregate
            if (result == null) {
                result = ConnectionException(
                    "Failed to connect to any ClickHouse endpoint; tried " + order
                        + " (first failure on " + failed + "): " + cause.message, cause
                )
            }
            result.addSuppressed(cause)
            return result
        }
    }
}
