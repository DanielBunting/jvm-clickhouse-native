package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.adbc.AdbcParams;
import io.github.danielbunting.clickhouse.adbc.ChAdbcDriver;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
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

/**
 * JMH benchmark that fully consumes a large SELECT through the ADBC (Arrow) driver, measured
 * against our native columnar path as the baseline. Both read the identical 1M-row dataset and
 * sum the same two columns, so the gap is the cost of the native-block → Arrow {@code
 * VectorSchemaRoot} bridge (and Arrow's off-heap buffer management) over the raw native read.
 *
 * <ul>
 *   <li>{@code ours_native} – block-streaming, low-allocation native path (the baseline).</li>
 *   <li>{@code ours_adbc} – the same query through ADBC, consumed as Arrow batches.</li>
 * </ul>
 *
 * <p>With the {@code gc} profiler enabled in {@code jmh { profilers.add("gc") }}, this also reports
 * bytes allocated per operation — the more interesting number for the Arrow path, whose batch
 * buffers are off-heap.
 *
 * <p>Setup mirrors {@link StreamingSelectBenchmark}: one million rows are inserted once per trial
 * via our bulk inserter so both paths read identical data.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class AdbcSelectBenchmark {

    /** Number of rows to pre-load and SELECT. Parameterised so it can be overridden via JMH. */
    @Param({"1000000"})
    public int rows;

    /** Shared Testcontainers / external-host resource. */
    private ClickHouseResource resource;

    /** Native connection: used to seed data and as the baseline read path. */
    private ClickHouseConnection nativeConn;

    /** Root allocator owning the ADBC driver's Arrow buffer tree. */
    private BufferAllocator adbcAllocator;

    private AdbcDatabase adbcDatabase;
    private AdbcConnection adbcConn;

    /** SQL executed by every benchmark method. */
    private static final String SELECT_SQL = "SELECT id, value FROM bench";

    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp();

        // Seed the bench table once per trial via our native bulk inserter (same data for both paths).
        nativeConn = resource.openNative(CompressionMethod.NONE);
        resource.recreateTable(nativeConn);
        List<BenchRow> data = SyntheticData.generate(rows);
        try (var ins = nativeConn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
        }

        // Open the ADBC driver directly against the same native endpoint (no compression).
        adbcAllocator = new RootAllocator();
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, resource.host());
        params.put(AdbcParams.PARAM_PORT, resource.nativePort());
        params.put(AdbcParams.PARAM_DATABASE, "default");
        adbcDatabase = new ChAdbcDriver(adbcAllocator).open(params);
        adbcConn = adbcDatabase.connect();
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (adbcConn != null) {
            adbcConn.close();
        }
        if (adbcDatabase != null) {
            adbcDatabase.close();
        }
        if (adbcAllocator != null) {
            adbcAllocator.close();
        }
        if (nativeConn != null) {
            nativeConn.close();
        }
        if (resource != null) {
            resource.tearDown();
        }
    }

    /**
     * Baseline: streams all rows via our native client, reading the primitive backing arrays
     * directly so the comparison measures wire/decoding throughput, not consumer boxing.
     *
     * @param bh JMH {@link Blackhole} used to prevent dead-code elimination
     * @throws Exception if the query fails
     */
    @Benchmark
    public void ours_native(Blackhole bh) throws Exception {
        try (var result = nativeConn.query(SELECT_SQL)) {
            var iter = result.blocks();
            while (iter.hasNext()) {
                Block block = iter.next();
                if (block.isEmpty()) {
                    continue;
                }
                Column idCol    = block.column(0); // id   UInt64  -> long[]
                Column valueCol = block.column(1); // value Float64 -> double[]
                long[] ids      = (long[]) idCol.values();
                double[] values = (double[]) valueCol.values();
                int rowCount = block.rowCount();
                long idSum = 0L;
                double valueSum = 0.0;
                for (int r = 0; r < rowCount; r++) {
                    idSum += ids[r];
                    valueSum += values[r];
                }
                bh.consume(idSum);
                bh.consume(valueSum);
            }
        }
    }

    /**
     * Streams all rows through the ADBC driver, consuming each Arrow batch's vectors directly:
     * {@code id} (UInt64 → an unsigned Arrow int, read via {@link BaseIntVector#getValueAsLong})
     * and {@code value} (Float64 → {@link Float8Vector}). Mirrors the native baseline's summing
     * loop so the two are semantically equivalent.
     *
     * @param bh JMH {@link Blackhole} used to prevent dead-code elimination
     * @throws Exception if the query or batch iteration fails
     */
    @Benchmark
    public void ours_adbc(Blackhole bh) throws Exception {
        try (AdbcStatement statement = adbcConn.createStatement()) {
            statement.setSqlQuery(SELECT_SQL);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    BaseIntVector id = (BaseIntVector) root.getVector(0);
                    Float8Vector value = (Float8Vector) root.getVector(1);
                    int rowCount = root.getRowCount();
                    long idSum = 0L;
                    double valueSum = 0.0;
                    for (int r = 0; r < rowCount; r++) {
                        idSum += id.getValueAsLong(r);
                        valueSum += value.get(r);
                    }
                    bh.consume(idSum);
                    bh.consume(valueSum);
                }
            }
        }
    }
}
