package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.QueryResult;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark that quantifies the compression trade-off for the native ClickHouse client.
 *
 * <p>The benchmark covers a full insert/select round-trip for a fixed synthetic dataset:
 * <ol>
 *   <li>Drop and re-create the {@code bench} table.</li>
 *   <li>Bulk-insert {@link SyntheticData#ROWS} rows using the selected {@link CompressionMethod}.</li>
 *   <li>Stream all rows back with {@code SELECT *} and hand each column value to the {@link Blackhole}
 *       so the JIT cannot eliminate the read.</li>
 * </ol>
 *
 * <p>The dataset is deterministic (see {@link SyntheticData#generate}), so results across
 * compression methods are directly comparable.
 *
 * <p>Compression is selected via the {@link #compression} {@code @Param}; JMH will fork once
 * per value and report separate statistics for each.
 *
 * <p>The ClickHouse server is provided by {@link ClickHouseResource}: a Testcontainers container
 * by default, or an external server when {@code -Dch.host} is set.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class CompressionBenchmark {

    /**
     * The compression algorithm to apply on the native TCP channel.
     * JMH parameterizes this as a {@code String} so the value can be passed on the command line
     * with {@code -p compression=LZ4} (for example). The actual {@link CompressionMethod} is
     * resolved in {@link #setup()}.
     */
    @Param({"NONE", "LZ4", "ZSTD"})
    public String compression;

    /**
     * Shared ClickHouse server state (container lifecycle / external host details).
     * Declared as a field so JMH injects the singleton {@link ClickHouseResource} that was
     * started at Trial level.
     */
    @State(Scope.Benchmark)
    public static class ServerState {
        /** Managed by JMH — starts the container or reads system properties. */
        public ClickHouseResource resource = new ClickHouseResource();

        /**
         * Starts the ClickHouse server (or validates the external one) before any benchmark
         * iteration runs.
         */
        @Setup(Level.Trial)
        public void startServer() {
            resource.setUp();
        }

        /**
         * Stops the ClickHouse server (or no-ops for external ones) after all iterations finish.
         */
        @TearDown(Level.Trial)
        public void stopServer() {
            resource.tearDown();
        }
    }

    /** Resolved compression method, derived from the {@link #compression} param string. */
    private CompressionMethod method;

    /** Pre-generated, deterministic dataset shared across all benchmark iterations. */
    private List<BenchRow> rows;

    /**
     * Resolves the {@link CompressionMethod} from the {@link #compression} string param and
     * generates the synthetic dataset once per trial. Executed before the warmup starts.
     */
    @Setup(Level.Trial)
    public void setup() {
        method = CompressionMethod.valueOf(compression);
        rows = SyntheticData.generate(SyntheticData.ROWS);
    }

    /**
     * Full insert/select round-trip measured under the configured compression method.
     *
     * <p>The table is dropped and re-created before each invocation so that each measurement
     * starts from an identical, empty state. This makes the reported time include only the work
     * of inserting and reading {@link SyntheticData#ROWS} rows, not accumulated table growth.
     *
     * <p>Column values are fed to the {@link Blackhole} to prevent dead-code elimination.
     *
     * @param bh        JMH blackhole that consumes column values to prevent DCE
     * @param server    injected server state holding the ClickHouse connection details
     */
    @Benchmark
    public void insertRoundTrip(Blackhole bh, ServerState server) {
        try (ClickHouseConnection conn = server.resource.openNative(method)) {
            // Ensure a clean table for every measurement.
            server.resource.recreateTable(conn);

            // Bulk-insert the full synthetic dataset with the selected compression.
            try (var inserter = conn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
                inserter.init();
                inserter.addRange(rows);
                inserter.complete();
            }

            // Stream every row back so the read path is included in the measurement.
            try (QueryResult result = conn.query("SELECT id, ts, user, value, status FROM " + SyntheticData.TABLE)) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    int cols = block.columnCount();
                    int rows = block.rowCount();
                    for (int c = 0; c < cols; c++) {
                        var col = block.column(c);
                        for (int r = 0; r < rows; r++) {
                            bh.consume(col.value(r));
                        }
                    }
                }
            }
        }
    }
}
