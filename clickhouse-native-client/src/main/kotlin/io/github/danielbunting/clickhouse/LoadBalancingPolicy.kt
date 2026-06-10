package io.github.danielbunting.clickhouse

/**
 * Strategy for ordering the configured [endpoints][Endpoint] when opening a
 * connection. The driver attempts endpoints in the order produced by the policy,
 * failing over to the next on a connect-time [ConnectionException].
 */
public enum class LoadBalancingPolicy {

    /**
     * Always start from the first configured endpoint, falling through to subsequent
     * endpoints only when earlier ones fail to connect. This keeps all traffic on the
     * primary while it is alive. This is the default.
     */
    FIRST_ALIVE,

    /**
     * Rotate the starting endpoint on each connection open using a shared
     * [java.util.concurrent.atomic.AtomicInteger] counter, spreading new
     * connections across nodes. On a connect failure, the remaining endpoints are still
     * tried in rotation order.
     */
    ROUND_ROBIN,

    /**
     * Pick a random starting endpoint per connection open (using an injectable
     * [java.util.Random] so tests are deterministic), then try the rest in order.
     */
    RANDOM,
}
