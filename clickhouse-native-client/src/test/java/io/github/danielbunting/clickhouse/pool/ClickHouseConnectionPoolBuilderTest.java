package io.github.danielbunting.clickhouse.pool;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link ClickHouseConnectionPool}'s builder-validation guards that run
 * <em>before</em> any connection is opened — so no server or Docker is needed. Live pool
 * behaviour (borrow/return, poison eviction, self-heal, borrow timeout) is covered by the
 * {@code integration} ITs.
 */
class ClickHouseConnectionPoolBuilderTest {

    private static ClickHouseConfig config() {
        // A well-formed config; the pool is never built successfully here (validation throws
        // first), so no socket is ever opened to this address.
        return ClickHouseConfig.fromUrl("chnative://localhost:9000/default");
    }

    @Test
    void builderRejectsNullConfig() {
        assertThrows(IllegalArgumentException.class, () -> ClickHouseConnectionPool.builder(null),
                "a null config must be rejected up front, not at first borrow");
    }

    @Test
    void createRejectsNullConfig() {
        assertThrows(IllegalArgumentException.class, () -> ClickHouseConnectionPool.create(null, 4),
                "create() delegates to builder(config), so null config fails the same way");
    }

    @Test
    void buildRejectsZeroSize() {
        // size <= 0 is checked as the pool's very first init step, before any connection open.
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseConnectionPool.builder(config()).size(0).build(),
                "a non-positive pool size is rejected before touching the network");
    }

    @Test
    void buildRejectsNegativeSize() {
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseConnectionPool.builder(config()).size(-1).build());
    }
}
