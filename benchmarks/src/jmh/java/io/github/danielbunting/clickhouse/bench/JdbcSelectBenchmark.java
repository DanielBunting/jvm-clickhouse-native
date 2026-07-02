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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Head-to-head JDBC read benchmark: <b>our</b> JDBC driver built from local source
 * ({@code io.github.danielbunting.clickhouse.jdbc.ClickHouseDriver}, native TCP) versus the
 * <b>official</b> {@code com.clickhouse.jdbc.ClickHouseDriver} pulled from Maven (HTTP).
 *
 * <p>The workload exercises exactly the {@code ChResultSet} read path recently changed:
 * <ul>
 *   <li>{@code *_getString} — reads every {@link #STRING_COLUMNS} {@code String} column via
 *       {@link ResultSet#getString(int)}, which funnels through {@code ChResultSet.raw()}
 *       (now carrying the short-circuiting UInt64 check).</li>
 *   <li>{@code *_uint64_getObject} — reads the {@code UInt64 id} column via
 *       {@link ResultSet#getObject(int)}, exercising the new unsigned-{@code BigInteger}
 *       conversion.</li>
 *   <li>{@code *_uint64_getLong} — reads the same column via {@link ResultSet#getLong(int)},
 *       the raw-bits primitive path (unchanged), as a control.</li>
 * </ul>
 *
 * <p><b>Caveat:</b> this is not a like-for-like transport comparison — our driver speaks the
 * native TCP protocol (port 9000) while the official driver here speaks HTTP (port 8123).
 * The point is to measure our JDBC read stack against a realistic published baseline, and to
 * catch any regression in the recent value-path change, not to isolate transport overhead.
 *
 * <p>Run just this class with the {@code gc} profiler:
 * {@code ./gradlew :benchmarks:jmh -PjmhInclude=JdbcSelectBenchmark}. Honors {@code -Dch.host}
 * via the shared {@link ClickHouseResource}; otherwise spins a Testcontainer.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class JdbcSelectBenchmark {

    /** Number of wide {@code String} columns in the table. */
    private static final int STRING_COLUMNS = 4;

    /** Target table for this benchmark. */
    private static final String TABLE = "bench_jdbc";

    /** Number of rows to pre-load and SELECT. */
    @Param({"200000"})
    public int rows;

    private ClickHouseResource resource;
    private ClickHouseConnection nativeConn;

    /** Our JDBC driver (local source, native TCP, port 9000). */
    private Connection oursConn;

    /** Official clickhouse-jdbc driver (Maven, HTTP transport, port 8123). */
    private Connection officialConn;

    private String selectStringsSql;
    private String selectIdSql;

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp();

        nativeConn = resource.openNative(CompressionMethod.NONE);

        StringBuilder cols = new StringBuilder("id UInt64");
        for (int c = 0; c < STRING_COLUMNS; c++) {
            cols.append(", s").append(c).append(" String");
        }
        nativeConn.execute("DROP TABLE IF EXISTS " + TABLE);
        nativeConn.execute("CREATE TABLE " + TABLE + " (" + cols + ") ENGINE = MergeTree ORDER BY id");

        // Populate server-side so loader cost is excluded from the measured methods. The id
        // stays within signed-long range so the official driver's getLong control doesn't
        // throw; our per-cell unsigned-BigInteger conversion in getObject runs regardless of
        // magnitude, so it is still fully exercised.
        StringBuilder insertExprs = new StringBuilder("number AS id");
        for (int c = 0; c < STRING_COLUMNS; c++) {
            insertExprs.append(", concat('col").append(c).append("-row-', toString(number), '-")
                    .append("padpadpadpadpadpadpad')")
                    .append(" AS s").append(c);
        }
        nativeConn.execute("INSERT INTO " + TABLE + " SELECT " + insertExprs
                + " FROM numbers(" + rows + ")");

        StringBuilder allCols = new StringBuilder("s0");
        for (int c = 1; c < STRING_COLUMNS; c++) {
            allCols.append(", s").append(c);
        }
        selectStringsSql = "SELECT " + allCols + " FROM " + TABLE;
        selectIdSql = "SELECT id FROM " + TABLE;

        Properties props = resource.competitorProps();
        // Instantiate both drivers directly to avoid ServiceLoader URL-scheme clashes.
        oursConn = new io.github.danielbunting.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:chnative://" + resource.host() + ":" + resource.nativePort() + "/default", props);
        officialConn = new com.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default", props);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (nativeConn != null) {
            nativeConn.close();
        }
        if (oursConn != null) {
            oursConn.close();
        }
        if (officialConn != null) {
            officialConn.close();
        }
        if (resource != null) {
            resource.tearDown();
        }
    }

    // -----------------------------------------------------------------------
    // Benchmark methods — ours (local source)
    // -----------------------------------------------------------------------

    @Benchmark
    public void ours_getString(Blackhole bh) throws Exception {
        sumStrings(oursConn, bh);
    }

    @Benchmark
    public void ours_uint64_getObject(Blackhole bh) throws Exception {
        sumIdObjects(oursConn, bh);
    }

    @Benchmark
    public void ours_uint64_getLong(Blackhole bh) throws Exception {
        sumIdLongs(oursConn, bh);
    }

    // -----------------------------------------------------------------------
    // Benchmark methods — official (Maven)
    // -----------------------------------------------------------------------

    @Benchmark
    public void official_getString(Blackhole bh) throws Exception {
        sumStrings(officialConn, bh);
    }

    @Benchmark
    public void official_uint64_getObject(Blackhole bh) throws Exception {
        sumIdObjects(officialConn, bh);
    }

    @Benchmark
    public void official_uint64_getLong(Blackhole bh) throws Exception {
        sumIdLongs(officialConn, bh);
    }

    // -----------------------------------------------------------------------
    // Shared read helpers (driver-agnostic; both run the identical JDBC calls)
    // -----------------------------------------------------------------------

    private void sumStrings(Connection conn, Blackhole bh) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectStringsSql)) {
            long lenSum = 0;
            while (rs.next()) {
                for (int c = 1; c <= STRING_COLUMNS; c++) {
                    lenSum += rs.getString(c).length();
                }
            }
            bh.consume(lenSum);
        }
    }

    private void sumIdObjects(Connection conn, Blackhole bh) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectIdSql)) {
            while (rs.next()) {
                bh.consume(rs.getObject(1));
            }
        }
    }

    private void sumIdLongs(Connection conn, Blackhole bh) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectIdSql)) {
            long sum = 0;
            while (rs.next()) {
                sum += rs.getLong(1);
            }
            bh.consume(sum);
        }
    }
}
