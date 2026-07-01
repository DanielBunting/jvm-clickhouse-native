package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Concurrent-query throughput of our native client driven through a
 * {@link ClickHouseConnectionPool}. Complements the single-threaded select benchmarks by
 * measuring how the pool + native transport scale when {@link #CONCURRENCY} threads issue
 * queries at once — the concurrent-workload dimension the suite previously lacked.
 *
 * <p>Throughput mode (ops/s): each op borrows a pooled connection and streams a bounded
 * aggregate read, so the number reflects borrow + round-trip + decode under contention.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Threads(ConcurrentQueryBenchmark.CONCURRENCY)
public class ConcurrentQueryBenchmark {

    /** Number of concurrent client threads (also the pool size). */
    static final int CONCURRENCY = 8;

    /** Rows pre-loaded into the bench table. */
    @Param({"200000"})
    public int rows;

    private ClickHouseResource resource;
    private ClickHouseConnectionPool pool;

    private static final String SELECT_SQL = "SELECT count() FROM " + SyntheticData.TABLE;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp();

        // Load data once using a throwaway native connection.
        try (ClickHouseConnection loader = resource.openNative(CompressionMethod.NONE)) {
            resource.recreateTable(loader);
            List<BenchRow> data = SyntheticData.generate(rows);
            try (var ins = loader.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
                ins.init();
                ins.addRange(data);
                ins.complete();
            }
        }

        // A pool sized to the thread count so every benchmark thread gets its own connection.
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host(resource.host())
                .port(resource.nativePort())
                .database("default")
                .username("default")
                .password("")
                .build();
        pool = ClickHouseConnectionPool.create(cfg, CONCURRENCY);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (pool != null) {
            pool.close();
        }
        if (resource != null) {
            resource.tearDown();
        }
    }

    /** Each thread borrows a pooled connection and runs a bounded aggregate query. */
    @Benchmark
    public void ours_native_pooled(Blackhole bh) {
        bh.consume(pool.withConnection(c -> c.executeScalar(SELECT_SQL)));
    }
}
