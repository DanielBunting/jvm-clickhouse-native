package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Read-only primitive SELECT measured under each {@link CompressionMethod}, our client only.
 *
 * <p>This isolates the <b>compressed read path</b> that {@code CompressionBenchmark} cannot:
 * that benchmark times a full insert+select round-trip whose allocation is dominated by the
 * insert side, so improvements confined to block <em>decoding</em> (decompression framing,
 * per-block reader reuse) are diluted below the noise floor. Here the data is inserted once at
 * trial setup (outside the measured loop), and only the {@code SELECT id, value} read is
 * measured — so {@code gc.alloc.rate.norm} reflects the decode path alone.
 *
 * <p>The {@code NONE} param is the control: it exercises no compressed-frame code, so its
 * allocation should be stable across changes to the compression path. The {@code LZ4}/{@code ZSTD}
 * params route through {@code CompressedFrameInputStream} + the decompressors, so changes such as
 * single-buffer frame checksum/decompress (C1) and compressed-reader reuse (C2) show up as a gap
 * between those lanes and {@code NONE}, and as a before/after delta on those lanes.
 *
 * <p>Consumption mirrors {@code StreamingSelectBenchmark.ours_native}: the primitive backing
 * arrays ({@code long[]}/{@code double[]}) are read directly and summed without boxing, so the
 * measured allocation is wire/decoding, not consumer boxing.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class CompressedSelectBenchmark {

    /** Native-channel compression method under test; {@code NONE} is the control lane. */
    @Param({"NONE", "LZ4", "ZSTD"})
    public String compression;

    /** Number of rows to pre-load and SELECT. Overridable via {@code -p rows=...}. */
    @Param({"1000000"})
    public int rows;

    /** SQL executed by the measured method — the same narrow primitive shape as StreamingSelectBenchmark. */
    private static final String SELECT_SQL = "SELECT id, value FROM " + SyntheticData.TABLE;

    private ClickHouseResource resource;

    /** Reusable native connection opened with the {@link #compression} under test. */
    private ClickHouseConnection nativeConn;

    /**
     * Trial-level setup: starts (or connects to) ClickHouse, recreates the {@code bench} table,
     * and bulk-inserts {@link #rows} rows once. The connection used for the measured read is
     * opened with the parameterised compression so the server compresses its response blocks.
     *
     * @throws Exception if any setup step fails
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        CompressionMethod method = CompressionMethod.valueOf(compression);
        resource = new ClickHouseResource();
        resource.setUp();

        nativeConn = resource.openNative(method);
        resource.recreateTable(nativeConn);
        List<BenchRow> data = SyntheticData.generate(rows);
        try (var ins = nativeConn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
        }
    }

    /**
     * Trial-level teardown: closes the connection and stops the container (if one was started).
     *
     * @throws Exception if a close operation fails
     */
    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (nativeConn != null) {
            nativeConn.close();
        }
        if (resource != null) {
            resource.tearDown();
        }
    }

    /**
     * Streams {@code SELECT id, value} back under the configured compression, summing the
     * primitive backing arrays without boxing so only the decode path allocates.
     *
     * @param bh JMH blackhole to prevent dead-code elimination
     * @throws Exception if the query fails
     */
    @Benchmark
    public void ours_compressed_select(Blackhole bh) throws Exception {
        try (var result = nativeConn.query(SELECT_SQL)) {
            var iter = result.blocks();
            while (iter.hasNext()) {
                Block block = iter.next();
                if (block.isEmpty()) {
                    continue;
                }
                Column idCol    = block.column(0); // id    UInt64  -> long[]
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
}
