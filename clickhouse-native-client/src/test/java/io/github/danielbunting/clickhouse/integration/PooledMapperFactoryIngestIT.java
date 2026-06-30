package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.mapping.RowMapper;
import io.github.danielbunting.clickhouse.mapping.RowMapperFactory;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Regression for code-review finding #3: a pooled connection must delegate the
 * {@link RowMapperFactory}-driven {@code createBulkInserter} overload to its underlying native
 * connection, not fall through to the interface default (which throws). Fails pre-fix because
 * {@code PooledConnection} did not override that overload.
 */
@Tag("integration")
class PooledMapperFactoryIngestIT extends IntegrationTestBase {

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    @Test
    void pooledConnectionSupportsMapperFactoryIngest() {
        String table = "pool_mapperfactory_" + System.nanoTime();
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.create(config(), 2)) {
            pool.useConnection(c -> c.execute(
                    "CREATE TABLE IF NOT EXISTS " + table + " (id Int64) ENGINE = MergeTree ORDER BY id"));

            pool.useConnection(c -> {
                RowMapperFactory<Integer> factory = names -> new RowMapper<Integer>() {
                    @Override
                    public String[] columnNames() {
                        return names;
                    }

                    @Override
                    public Integer map(Object[] columnValues) {
                        throw new UnsupportedOperationException("write-only");
                    }

                    @Override
                    public void bind(Integer value, Object[] dest) {
                        dest[0] = (long) value;
                    }
                };
                try (BulkInserter<Integer> inserter =
                        c.createBulkInserter(table, Integer.class, factory)) {
                    inserter.init();
                    for (int i = 0; i < 3; i++) {
                        inserter.add(i);
                    }
                    inserter.complete();
                }
            });

            AtomicLong count = new AtomicLong();
            pool.useConnection(c -> count.set(c.executeScalar("SELECT count() FROM " + table)));
            assertEquals(3L, count.get());
        }
    }
}
