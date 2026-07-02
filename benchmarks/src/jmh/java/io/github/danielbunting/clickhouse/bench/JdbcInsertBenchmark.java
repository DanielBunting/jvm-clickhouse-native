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
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Head-to-head JDBC <b>write</b> benchmark, the insert analog of
 * {@link JdbcSelectBenchmark}: <b>our</b> JDBC driver built from local source
 * ({@code io.github.danielbunting.clickhouse.jdbc.ClickHouseDriver}, native TCP) versus the
 * <b>official</b> {@code com.clickhouse.jdbc.ClickHouseDriver} pulled from Maven (HTTP).
 *
 * <p>The workload exercises the {@code ChPreparedStatement} batch-insert path: every row of the
 * shared {@link SyntheticData} dataset is bound and added to a batch, then flushed with a single
 * {@link PreparedStatement#executeBatch()}. Our driver collapses the batch into one multi-row
 * {@code INSERT ... VALUES (...),(...)} statement; the official v2 driver streams it as a
 * RowBinary HTTP INSERT. Both run the identical JDBC calls via {@link #insertBatch(Connection)}.
 *
 * <p>This is complementary to {@link BulkInsertBenchmark}, which measures our native
 * {@code BulkInserter} against the competitor matrix. Here the subject is specifically our
 * <em>JDBC</em> stack, so the only two participants are our JDBC driver and the official one.
 *
 * <p><b>Caveat:</b> this is not a like-for-like transport comparison — our driver speaks the
 * native TCP protocol (port 9000) while the official driver here speaks HTTP (port 8123). The
 * point is to track our JDBC insert stack against a realistic published baseline, not to isolate
 * transport overhead.
 *
 * <p>The {@code bench} table is recreated before every measured invocation so each iteration
 * starts from an empty table. Run just this class:
 * {@code ./gradlew :benchmarks:jmh -PjmhInclude=JdbcInsertBenchmark}. Honors {@code -Dch.host}
 * via the shared {@link ClickHouseResource}; otherwise spins a Testcontainer.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class JdbcInsertBenchmark {

    /** Number of rows bound and flushed per measured invocation. */
    @Param({"200000"})
    public int rows;

    private ClickHouseResource resource;

    /** Pre-generated synthetic dataset shared by both drivers. */
    private List<BenchRow> data;

    /** Our JDBC driver (local source, native TCP, port 9000). */
    private Connection oursConn;

    /** Official clickhouse-jdbc driver (Maven, HTTP transport, port 8123). */
    private Connection officialConn;

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp();
        data = SyntheticData.generate(rows);

        Properties props = resource.competitorProps();
        // Instantiate both drivers directly to avoid ServiceLoader URL-scheme clashes.
        oursConn = new io.github.danielbunting.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:chnative://" + resource.host() + ":" + resource.nativePort() + "/default", props);
        officialConn = new com.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default", props);
    }

    /**
     * Recreates an empty {@code bench} table before every measured invocation so each insert
     * starts from a clean slate and timings are not perturbed by accumulated parts.
     */
    @Setup(Level.Invocation)
    public void recreateTable() {
        try (ClickHouseConnection conn = resource.openNative(CompressionMethod.NONE)) {
            resource.recreateTable(conn);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
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
    // Benchmark methods
    // -----------------------------------------------------------------------

    @Benchmark
    public void ours_batchInsert(Blackhole bh) throws Exception {
        bh.consume(insertBatch(oursConn));
    }

    @Benchmark
    public void official_batchInsert(Blackhole bh) throws Exception {
        bh.consume(insertBatch(officialConn));
    }

    // -----------------------------------------------------------------------
    // Shared write helper (driver-agnostic; both run the identical JDBC calls)
    // -----------------------------------------------------------------------

    /**
     * Prepares the canonical insert statement and submits every row as one batch.
     *
     * <p>The {@code user} column is backtick-quoted because the official v2 JDBC driver's
     * client-side parser rejects it bare and falls back to a far slower path; the server accepts
     * the unquoted form, so quoting is a no-op for our driver (see {@link BulkInsertBenchmark}).
     *
     * @param conn an open JDBC connection to the {@code default} database
     * @return the number of rows inserted
     * @throws Exception if statement preparation or execution fails
     */
    private long insertBatch(Connection conn) throws Exception {
        String sql = "INSERT INTO " + SyntheticData.TABLE + " (id,ts,`user`,value,status) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (BenchRow row : data) {
                ps.setLong(1, row.id());
                ps.setTimestamp(2, Timestamp.from(row.ts()));
                ps.setString(3, row.user());
                ps.setDouble(4, row.value());
                ps.setInt(5, row.status());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return data.size();
    }
}
