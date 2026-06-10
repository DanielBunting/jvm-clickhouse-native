package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.Endpoint
import io.github.danielbunting.clickhouse.LoadBalancingPolicy
import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

/**
 * Produces the ordered sequence of [endpoints][Endpoint] to attempt for a single
 * connection open, according to a [LoadBalancingPolicy].
 *
 * The first endpoint in the returned list is the policy-chosen starting node; the
 * remaining endpoints follow in rotation order so a failed start can fail over to every
 * other configured node. Each call to [attemptOrder] returns a complete
 * permutation containing all endpoints exactly once.
 *
 * Per the project rules, randomness/rotation never use unseeded
 * `Math.random()`/`new Random()`: [LoadBalancingPolicy.ROUND_ROBIN]
 * uses a shared [AtomicInteger] counter, and [LoadBalancingPolicy.RANDOM]
 * uses a [Random] that may be injected (deterministic in tests). A single selector
 * instance is meant to be shared across opens (e.g. by the connection pool) so rotation
 * spreads across nodes; it is thread-safe.
 */
public class EndpointSelector
/**
 * Creates a selector with an explicit [Random], enabling deterministic tests of
 * the [LoadBalancingPolicy.RANDOM] policy.
 *
 * @param endpoints the configured endpoints (non-empty)
 * @param policy    the load-balancing policy
 * @param random    the random source for `RANDOM`; ignored by other policies
 */
public constructor(endpoints: List<Endpoint>?, policy: LoadBalancingPolicy?, random: Random) {

    private val endpoints: List<Endpoint>
    private val policy: LoadBalancingPolicy
    private val roundRobinCounter = AtomicInteger()
    private val random: Random

    init {
        if (endpoints == null || endpoints.isEmpty()) {
            throw IllegalArgumentException("endpoints must not be null or empty")
        }
        this.endpoints = java.util.List.copyOf(endpoints)
        this.policy = policy ?: LoadBalancingPolicy.FIRST_ALIVE
        this.random = random
    }

    /**
     * Creates a selector with a default [Random] (seeded from the system source —
     * acceptable here because it is constructed once, not per call).
     *
     * @param endpoints the configured endpoints (non-empty)
     * @param policy    the load-balancing policy
     */
    public constructor(endpoints: List<Endpoint>?, policy: LoadBalancingPolicy?) :
        this(endpoints, policy, Random())

    /** The endpoints this selector orders, in their configured order. */
    public fun endpoints(): List<Endpoint> {
        return endpoints
    }

    /** The active policy. */
    public fun policy(): LoadBalancingPolicy {
        return policy
    }

    /**
     * Returns a full permutation of the endpoints to try for one connection open, starting
     * at the policy-chosen endpoint and continuing in rotation order. The list always
     * contains every endpoint exactly once.
     *
     * @return the ordered attempt sequence (a fresh list per call)
     */
    public fun attemptOrder(): List<Endpoint> {
        val n = endpoints.size
        val start = when (policy) {
            LoadBalancingPolicy.FIRST_ALIVE -> 0
            // getAndIncrement keeps a monotonically advancing counter; floorMod keeps the
            // index non-negative even after the counter wraps past Integer.MAX_VALUE.
            LoadBalancingPolicy.ROUND_ROBIN -> Math.floorMod(roundRobinCounter.getAndIncrement(), n)
            LoadBalancingPolicy.RANDOM -> random.nextInt(n)
        }
        val order = ArrayList<Endpoint>(n)
        for (i in 0 until n) {
            order.add(endpoints[(start + i) % n])
        }
        return order
    }
}
