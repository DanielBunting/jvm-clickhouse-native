package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
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
}
