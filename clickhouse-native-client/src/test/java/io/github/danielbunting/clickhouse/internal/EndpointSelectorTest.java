package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.Endpoint;
import io.github.danielbunting.clickhouse.LoadBalancingPolicy;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for multi-endpoint parsing and the {@link EndpointSelector} ordering
 * policies. Pure logic — no sockets, no server.
 */
class EndpointSelectorTest {

    private static final List<Endpoint> THREE = List.of(
            new Endpoint("a", 9000), new Endpoint("b", 9000), new Endpoint("c", 9000));

    @Test
    void parsesCommaSeparatedHostsFromUrl() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl(
                "chnative://user:pw@h1:9000,h2:9001,h3/db?compression=lz4");

        assertEquals(
                List.of(new Endpoint("h1", 9000), new Endpoint("h2", 9001), new Endpoint("h3", 9000)),
                cfg.endpoints(),
                "all comma-separated endpoints parsed, default port 9000 applied");
        assertEquals("user", cfg.username());
        assertEquals("db", cfg.database());
    }

    @Test
    void trimsWhitespaceAndSkipsEmptyEndpoints() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl(
                "chnative://h1:9000, h2:9001 ,,h3/db");

        assertEquals(
                List.of(new Endpoint("h1", 9000), new Endpoint("h2", 9001), new Endpoint("h3", 9000)),
                cfg.endpoints(),
                "surrounding whitespace trimmed and empty segments skipped");
    }

    @Test
    void parsesBracketedIpv6HostWithPort() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://[2001:db8::1]:9000/db");

        assertEquals(
                List.of(new Endpoint("2001:db8::1", 9000)),
                cfg.endpoints(),
                "bracketed IPv6 literal keeps its internal colons; only the trailing :port is the port");
    }

    @Test
    void parsesBracketedIpv6HostWithoutPortDefaultsTo9000() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://[::1]/db");

        assertEquals(
                List.of(new Endpoint("::1", 9000)),
                cfg.endpoints(),
                "bracketed IPv6 literal without an explicit port defaults to 9000, not mis-parsed");
    }

    /**
     * Underscore hostnames (reference: client-v2 HttpEndpointTest — underscore hosts make
     * {@code java.net.URI.getHost()} return null) survive multi-endpoint parsing, because
     * the endpoint list is extracted with the client's own authority parser.
     */
    @Test
    void parsesUnderscoreHostsInEndpointList() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl(
                "chnative://ch_node_1:9000,ch_node_2:9001,plain-host/db");

        assertEquals(
                List.of(new Endpoint("ch_node_1", 9000),
                        new Endpoint("ch_node_2", 9001),
                        new Endpoint("plain-host", 9000)),
                cfg.endpoints(),
                "underscore hosts must not be dropped or nulled by java.net.URI host parsing");
        assertEquals("db", cfg.database());
    }

    @Test
    void parsesMixedIpv6AndIpv4EndpointList() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://[::1]:9000,10.0.0.5:9001/db");

        assertEquals(
                List.of(new Endpoint("::1", 9000), new Endpoint("10.0.0.5", 9001)),
                cfg.endpoints());
    }

    @Test
    void loadBalancingPolicyParsedFromUrlCaseInsensitively() {
        assertEquals(
                LoadBalancingPolicy.ROUND_ROBIN,
                ClickHouseConfig.fromUrl("chnative://h1,h2/db?loadBalancingPolicy=RoundRobin")
                        .loadBalancingPolicy());
        assertEquals(
                LoadBalancingPolicy.RANDOM,
                ClickHouseConfig.fromUrl("chnative://h1,h2/db?loadBalancingPolicy=random")
                        .loadBalancingPolicy());
        assertEquals(
                LoadBalancingPolicy.FIRST_ALIVE,
                ClickHouseConfig.fromUrl("chnative://h1,h2/db?loadBalancingPolicy=first_alive")
                        .loadBalancingPolicy());
    }

    @Test
    void unknownLoadBalancingPolicyThrows() {
        assertThrows(
                ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://h1,h2/db?loadBalancingPolicy=bogus"));
    }

    @Test
    void firstAliveKeepsConfiguredOrder() {
        EndpointSelector sel = new EndpointSelector(THREE, LoadBalancingPolicy.FIRST_ALIVE);
        assertEquals(THREE, sel.attemptOrder());
        assertEquals(THREE, sel.attemptOrder(), "FIRST_ALIVE is stable across calls");
    }

    @Test
    void roundRobinRotatesStartEachCall() {
        EndpointSelector sel = new EndpointSelector(THREE, LoadBalancingPolicy.ROUND_ROBIN);
        List<Endpoint> first = sel.attemptOrder();
        List<Endpoint> second = sel.attemptOrder();
        List<Endpoint> third = sel.attemptOrder();
        // Each call starts one position later, wrapping around; all three are full permutations.
        assertEquals(3, first.size());
        assertEquals(first.get(1), second.get(0), "second call starts at the next endpoint");
        assertEquals(first.get(2), third.get(0), "third call starts two positions later");
    }

    @Test
    void randomIsDeterministicWithSeededRng() {
        List<Endpoint> a = new EndpointSelector(THREE, LoadBalancingPolicy.RANDOM, new Random(42))
                .attemptOrder();
        List<Endpoint> b = new EndpointSelector(THREE, LoadBalancingPolicy.RANDOM, new Random(42))
                .attemptOrder();
        assertEquals(a, b, "same seed yields the same attempt order");
        assertEquals(3, a.size(), "all endpoints present, just reordered");
    }

    /**
     * Round-robin distributes evenly under concurrency (reference:
     * ClickHouseClusterTest#testGetNode / ClickHouseLoadBalancingPolicyTest#testRoundRobin —
     * multi-threaded even distribution). The rotating start index is shared state; N
     * threads each drawing an attempt order must see every endpoint as the first choice
     * an equal number of times.
     */
    @Test
    void roundRobinDistributesEvenlyAcrossThreads() throws Exception {
        EndpointSelector sel = new EndpointSelector(THREE, LoadBalancingPolicy.ROUND_ROBIN);
        int perEndpoint = 100;
        int draws = THREE.size() * perEndpoint;

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(8);
        java.util.concurrent.ConcurrentHashMap<Endpoint, java.util.concurrent.atomic.AtomicInteger>
                firstChoice = new java.util.concurrent.ConcurrentHashMap<>();
        try {
            java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < draws; i++) {
                futures.add(pool.submit(() -> firstChoice
                        .computeIfAbsent(sel.attemptOrder().get(0),
                                e -> new java.util.concurrent.atomic.AtomicInteger())
                        .incrementAndGet()));
            }
            for (java.util.concurrent.Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(3, firstChoice.size(), "every endpoint led at least one attempt order");
        for (Endpoint e : THREE) {
            assertEquals(perEndpoint, firstChoice.get(e).get(),
                    "round-robin start index gives " + e + " exactly its share");
        }
    }

    /**
     * The RANDOM policy reaches every endpoint (reference:
     * ClickHouseLoadBalancingPolicyTest#testRandom — random policy hits all nodes). With
     * a seeded RNG the check is deterministic: across a modest number of draws each of
     * the three endpoints leads the attempt order at least once, so no endpoint is
     * unreachable under the random policy.
     */
    @Test
    void randomPolicyEventuallyLeadsWithEveryEndpoint() {
        EndpointSelector sel = new EndpointSelector(THREE, LoadBalancingPolicy.RANDOM, new Random(7));
        java.util.Set<Endpoint> leaders = new java.util.HashSet<>();
        for (int i = 0; i < 50 && leaders.size() < THREE.size(); i++) {
            List<Endpoint> order = sel.attemptOrder();
            assertEquals(3, order.size(), "every draw is a full permutation");
            leaders.add(order.get(0));
        }
        assertEquals(java.util.Set.copyOf(THREE), leaders,
                "every endpoint leads some attempt order under the random policy");
    }
}
