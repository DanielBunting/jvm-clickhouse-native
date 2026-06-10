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

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark that fully consumes a large SELECT via three drivers:
 * <ol>
 *   <li>Our native client ({@code ours_native}) – block-streaming, low-allocation path.</li>
 *   <li>Official ClickHouse JDBC over HTTP ({@code clickhouseJavaHttp}).</li>
 *   <li>HousePower native JDBC ({@code housepowerNative}).</li>
 * </ol>
 *
 * <p>The benchmark validates the lazy-streaming / low-memory story. Combined with the
 * {@code gc} profiler enabled in {@code jmh { profilers.add("gc") }}, it reports
 * bytes allocated per operation for each driver.
 *
 * <p>Setup: one million rows are inserted once per trial via our bulk inserter so that
 * all three drivers read identical data.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class StreamingSelectBenchmark {

    /** Number of rows to pre-load and SELECT. Parameterised so it can be overridden via JMH. */
    @Param({"1000000"})
    public int rows;

    // -----------------------------------------------------------------------
    // Infrastructure shared across all benchmark methods.
    // -----------------------------------------------------------------------

    /**
     * Shared Testcontainers / external-host resource, managed by task B0
     * ({@code ClickHouseResource}).
     */
    private ClickHouseResource resource;

    /** Reusable native connection (no compression to keep comparisons fair). */
    private ClickHouseConnection nativeConn;

    /** Official ClickHouse JDBC connection (HTTP transport, port 8123). */
    private java.sql.Connection httpConn;

    /** HousePower native JDBC connection (native TCP, port 9000). */
    private java.sql.Connection hpConn;

    /** SQL executed by every benchmark method. */
    private static final String SELECT_SQL = "SELECT id, value FROM bench";

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Trial-level setup: starts (or connects to) ClickHouse, recreates the bench
     * table, bulk-inserts {@link #rows} rows via our native client, and opens a
     * connection for each driver.
     *
     * @throws Exception if any setup step fails
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp(); // delegate to ClickHouseResource @Setup logic

        // Open our native connection.
        nativeConn = resource.openNative(CompressionMethod.NONE);

        // Create table and load data.
        resource.recreateTable(nativeConn);
        List<BenchRow> data = SyntheticData.generate(rows);
        try (var ins = nativeConn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
        }

        // Open competitor connections (instantiated directly to avoid URL-scheme clash).
        java.util.Properties props = resource.competitorProps();

        httpConn = new com.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default", props);

        hpConn = new com.github.housepower.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.nativePort() + "/default", props);
    }

    /**
     * Trial-level teardown: closes all open connections and stops the container
     * (if one was started).
     *
     * @throws Exception if a close operation fails
     */
    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (nativeConn != null) {
            nativeConn.close();
        }
        if (httpConn != null) {
            httpConn.close();
        }
        if (hpConn != null) {
            hpConn.close();
        }
        if (resource != null) {
            resource.tearDown(); // delegate to ClickHouseResource @TearDown logic
        }
    }

    // -----------------------------------------------------------------------
    // Benchmark methods
    // -----------------------------------------------------------------------

    /**
     * Streams all rows from ClickHouse using our native client.
     *
     * <p>Data flows as a sequence of {@link Block} objects. To keep the comparison
     * apples-to-apples with the JDBC competitors (which are consumed via the
     * primitive {@code rs.getLong}/{@code rs.getDouble} getters), we read the
     * primitive backing arrays directly — {@code (long[]) idCol.values()} and
     * {@code (double[]) valueCol.values()} — and sum without boxing, rather than
     * the boxing {@link Column#value(int)} path. This measures wire/decoding
     * throughput, not our consumer's boxing. Both {@code id} (column 0, UInt64 →
     * {@code long[]}) and {@code value} (column 1, Float64 → {@code double[]}) are
     * consumed to keep the comparison semantically equivalent.
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
     * Streams all rows using the official {@code clickhouse-jdbc} driver over HTTP.
     *
     * <p>The driver is instantiated directly (not via {@link java.sql.DriverManager})
     * to avoid URL-scheme conflicts with HousePower.
     *
     * @param bh JMH {@link Blackhole} to prevent dead-code elimination
     * @throws Exception if the query or result-set iteration fails
     */
    @Benchmark
    public void clickhouseJavaHttp(Blackhole bh) throws Exception {
        try (Statement stmt = httpConn.createStatement();
             ResultSet rs   = stmt.executeQuery(SELECT_SQL)) {
            while (rs.next()) {
                bh.consume(rs.getLong(1));
                bh.consume(rs.getDouble(2));
            }
        }
    }

    /**
     * Streams all rows using the HousePower native JDBC driver over the native
     * TCP protocol.
     *
     * <p>The driver is instantiated directly (not via {@link java.sql.DriverManager})
     * to avoid URL-scheme conflicts with the official driver.
     *
     * @param bh JMH {@link Blackhole} to prevent dead-code elimination
     * @throws Exception if the query or result-set iteration fails
     */
    @Benchmark
    public void housepowerNative(Blackhole bh) throws Exception {
        try (Statement stmt = hpConn.createStatement();
             ResultSet rs   = stmt.executeQuery(SELECT_SQL)) {
            while (rs.next()) {
                bh.consume(rs.getLong(1));
                bh.consume(rs.getDouble(2));
            }
        }
    }
}
