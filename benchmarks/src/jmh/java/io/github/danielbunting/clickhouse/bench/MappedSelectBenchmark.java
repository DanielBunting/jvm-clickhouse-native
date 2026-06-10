package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * JMH benchmark for the mapped-read API {@code query(sql, Class<T>)} on our native
 * client. It measures the cost of streaming a large SELECT through the lazy,
 * {@code Spliterator}-backed object mapper introduced by improvement 05 (no eager
 * {@code List}, reused per-block row buffer).
 *
 * <p>Combined with the {@code gc} profiler (enabled via {@code jmh { profilers.add("gc") }})
 * this reports bytes allocated per operation, validating the allocation win versus the
 * previous "materialize the whole result into a {@code List<T>}" implementation.
 *
 * <p>For a complete head-to-head, the official ClickHouse JDBC (HTTP) and HousePower
 * native JDBC drivers run the same SELECT ({@code clickhouseJavaHttp}, {@code housepowerNative}),
 * building one {@link MappedRow} per row from the {@code ResultSet} so each driver produces
 * the same materialised objects our mapper does.
 *
 * <p>Setup mirrors {@link StreamingSelectBenchmark}: one million rows are inserted once
 * per trial via our bulk inserter, and the shared {@link ClickHouseResource} (honoring
 * {@code -Dch.host}) owns the server.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class MappedSelectBenchmark {

    /**
     * Mapped row type. Component names ({@code id}, {@code value}) match the selected
     * {@code bench} columns so the reflection mapper binds them positionally.
     *
     * @param id    the row identifier, from {@code bench.id} ({@code UInt64})
     * @param value the numeric payload, from {@code bench.value} ({@code Float64})
     */
    public record MappedRow(long id, double value) {}

    /** Number of rows to pre-load and SELECT. Parameterised so it can be overridden via JMH. */
    @Param({"1000000"})
    public int rows;

    /** Shared Testcontainers / external-host resource. */
    private ClickHouseResource resource;

    /** Reusable native connection (no compression to keep comparisons fair). */
    private ClickHouseConnection nativeConn;

    /** Official ClickHouse JDBC connection (HTTP transport, port 8123). */
    private java.sql.Connection httpConn;

    /** HousePower native JDBC connection (native TCP, port 9000). */
    private java.sql.Connection hpConn;

    /** SQL executed by the benchmark; selects exactly the mapped columns. */
    private static final String SELECT_SQL = "SELECT id, value FROM bench";

    /**
     * Trial-level setup: starts (or connects to) ClickHouse, recreates the bench table,
     * and bulk-inserts {@link #rows} rows via our native client.
     *
     * @throws Exception if any setup step fails
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp();

        nativeConn = resource.openNative(CompressionMethod.NONE);
        resource.recreateTable(nativeConn);

        List<BenchRow> data = SyntheticData.generate(rows);
        try (var ins = nativeConn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
        }

        // Open competitor connections (instantiated directly to avoid URL-scheme clash).
        Properties props = resource.competitorProps();
        httpConn = new com.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default", props);
        hpConn = new com.github.housepower.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.nativePort() + "/default", props);
    }

    /**
     * Trial-level teardown: closes the connection and stops the container (if started).
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
            resource.tearDown();
        }
    }

    /**
     * Streams all rows mapped to {@link MappedRow} via {@code query(sql, Class)} and
     * consumes each mapped object through the {@link Blackhole}. The stream is closed
     * (try-with-resources) so the connection guard is released for the next iteration.
     *
     * @param bh JMH {@link Blackhole} used to prevent dead-code elimination
     */
    @Benchmark
    public void ours_query_class(Blackhole bh) {
        try (Stream<MappedRow> stream = nativeConn.query(SELECT_SQL, MappedRow.class)) {
            stream.forEach(bh::consume);
        }
    }

    /**
     * Official {@code clickhouse-jdbc} (HTTP) head-to-head: build one {@link MappedRow}
     * per result row via the {@code ResultSet} getters, mirroring our mapper's output.
     *
     * @param bh JMH {@link Blackhole} used to prevent dead-code elimination
     * @throws Exception if the query or result-set iteration fails
     */
    @Benchmark
    public void clickhouseJavaHttp(Blackhole bh) throws Exception {
        mapRows(httpConn, bh);
    }

    /**
     * HousePower native JDBC head-to-head: build one {@link MappedRow} per result row
     * via the {@code ResultSet} getters, mirroring our mapper's output.
     *
     * @param bh JMH {@link Blackhole} used to prevent dead-code elimination
     * @throws Exception if the query or result-set iteration fails
     */
    @Benchmark
    public void housepowerNative(Blackhole bh) throws Exception {
        mapRows(hpConn, bh);
    }

    /**
     * Streams the SELECT through a JDBC connection, constructing a {@link MappedRow}
     * per row so the competitor produces the same materialised objects our mapper does.
     */
    private void mapRows(java.sql.Connection conn, Blackhole bh) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(SELECT_SQL)) {
            while (rs.next()) {
                bh.consume(new MappedRow(rs.getLong(1), rs.getDouble(2)));
            }
        }
    }
}
