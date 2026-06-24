package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.BulkInserter;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark that measures the throughput of inserting a synthetic dataset
 * into a ClickHouse {@code bench} table via each of the available JVM drivers:
 *
 * <ul>
 *   <li>{@code clickhouse-native-client} (this project) with LZ4, ZSTD and no
 *       compression, using its native {@link BulkInserter};</li>
 *   <li>the official {@code com.clickhouse:clickhouse-jdbc} driver over HTTP
 *       (port 8123), in both generations shipped by 0.9.x: the legacy v1
 *       implementation ({@code com.clickhouse.jdbc.DriverV1}) and the v2 rewrite
 *       ({@code com.clickhouse.jdbc.ClickHouseDriver}, built on client-v2);</li>
 *   <li>the official {@code com.clickhouse:client-v2} API directly
 *       ({@code com.clickhouse.client.api.Client}), i.e. the idiomatic non-JDBC
 *       bulk path with schema-compiled POJO serializers;</li>
 *   <li>the {@code com.github.housepower:clickhouse-native-jdbc} driver over
 *       the native protocol (port 9000).</li>
 * </ul>
 *
 * <p>The dataset is generated once per trial via
 * {@link SyntheticData#generate(int)} and is deterministic. The {@code bench}
 * table is recreated before every measured invocation so that each benchmark
 * iteration starts from an empty table.
 *
 * <p>Both competitor drivers register the same {@code jdbc:clickhouse://} URL
 * scheme, so they are instantiated directly rather than through
 * {@link java.sql.DriverManager} (see the corresponding driver class names in
 * each method).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class BulkInsertBenchmark {

    /** Shared ClickHouse endpoint / container managed by the JMH state class. */
    private final ClickHouseResource resource = new ClickHouseResource();

    /** Number of rows inserted per benchmark invocation. */
    @Param({"1000000"})
    public int rows;

    /** Pre-generated synthetic dataset shared across all benchmark methods. */
    private List<BenchRow> data;

    /**
     * The same dataset as JavaBeans for the client-v2 POJO path, whose
     * serializer registration requires bean-style getters (see {@link BenchRowBean}).
     * Converted once per trial so the conversion is not measured.
     */
    private List<BenchRowBean> beanData;

    /** Connection properties for the competitor JDBC drivers. */
    private Properties competitorProps;

    /**
     * Starts (or connects to) the ClickHouse server and pre-generates the
     * synthetic dataset. Runs once per trial.
     *
     * @throws Exception if the resource fails to start
     */
    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
        resource.setUp();
        this.data = SyntheticData.generate(rows);
        this.beanData = data.stream().map(BenchRowBean::new).toList();
        this.competitorProps = resource.competitorProps();
    }

    /**
     * Stops the ClickHouse resource. Runs once per trial.
     *
     * @throws Exception if the resource fails to stop
     */
    @org.openjdk.jmh.annotations.TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        resource.tearDown();
    }

    /**
     * Recreates an empty {@code bench} table before every measured invocation
     * so each insert benchmark starts from a clean slate.
     */
    @Setup(Level.Invocation)
    public void recreateTable() {
        try (ClickHouseConnection conn = resource.openNative(CompressionMethod.NONE)) {
            resource.recreateTable(conn);
        }
    }

    // ------------------------------------------------------------------
    // clickhouse-native-client (this project)
    // ------------------------------------------------------------------

    /**
     * Inserts the dataset using this project's native bulk inserter with LZ4
     * compression.
     *
     * @param bh JMH sink that consumes the inserted row count to prevent
     *           dead-code elimination
     */
    @Benchmark
    public void ours_lz4(Blackhole bh) {
        bh.consume(insertOurs(CompressionMethod.LZ4));
    }

    /**
     * Inserts the dataset using this project's native bulk inserter with ZSTD
     * compression.
     *
     * @param bh JMH sink that consumes the inserted row count
     */
    @Benchmark
    public void ours_zstd(Blackhole bh) {
        bh.consume(insertOurs(CompressionMethod.ZSTD));
    }

    /**
     * Inserts the dataset using this project's native bulk inserter with no
     * compression.
     *
     * @param bh JMH sink that consumes the inserted row count
     */
    @Benchmark
    public void ours_none(Blackhole bh) {
        bh.consume(insertOurs(CompressionMethod.NONE));
    }

    /**
     * Opens a native connection with the given compression method and bulk
     * inserts the entire dataset.
     *
     * @param method the compression method to negotiate for this connection
     * @return the number of rows inserted
     */
    private long insertOurs(CompressionMethod method) {
        try (ClickHouseConnection conn = resource.openNative(method);
             BulkInserter<BenchRow> ins = conn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
            return data.size();
        }
    }

    // ------------------------------------------------------------------
    // Official ClickHouse JDBC driver (HTTP, port 8123)
    // ------------------------------------------------------------------

    /**
     * Inserts the dataset using the official driver's <b>legacy v1</b> JDBC
     * implementation ({@code com.clickhouse.jdbc.DriverV1}) over HTTP via a
     * batched {@link PreparedStatement}.
     *
     * <p>On {@code executeBatch()} the v1 {@code InputBasedPreparedStatement}
     * streams the whole batch as a single RowBinary HTTP INSERT, going through
     * the generic {@code ClickHouseValue} layer per cell. This is the same code
     * path previously benchmarked as {@code clickhouseJavaHttp} against
     * {@code clickhouse-jdbc:0.6.5}.</p>
     *
     * @param bh JMH sink that consumes the inserted row count
     * @throws Exception if the JDBC insert fails
     */
    @Benchmark
    public void clickhouseJavaV1Http(Blackhole bh) throws Exception {
        String url = "jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default";
        try (Connection conn = new com.clickhouse.jdbc.DriverV1().connect(url, competitorProps)) {
            bh.consume(insertJdbc(conn));
        }
    }

    /**
     * Inserts the dataset using the official driver's <b>v2</b> JDBC
     * implementation ({@code com.clickhouse.jdbc.ClickHouseDriver}, the default
     * since 0.8.0, built on client-v2) over HTTP via a batched
     * {@link PreparedStatement}.
     *
     * @param bh JMH sink that consumes the inserted row count
     * @throws Exception if the JDBC insert fails
     */
    @Benchmark
    public void clickhouseJavaV2Jdbc(Blackhole bh) throws Exception {
        String url = "jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default";
        try (Connection conn = new com.clickhouse.jdbc.ClickHouseDriver().connect(url, competitorProps)) {
            bh.consume(insertJdbc(conn));
        }
    }

    /**
     * Inserts the dataset using the official {@code com.clickhouse:client-v2}
     * API directly: {@code register(Class, TableSchema)} compiles per-column
     * POJO serializers once, then {@code insert(table, List)} streams the rows
     * as one RowBinary HTTP INSERT. This is the official driver's idiomatic
     * (non-JDBC) bulk path and the closest analog to this project's
     * {@link BulkInserter}.
     *
     * <p>Client construction, schema fetch and serializer compilation are
     * inside the measured region, mirroring the other variants which also open
     * their connection (and, for the native client, run the schema exchange)
     * per invocation.</p>
     *
     * @param bh JMH sink that consumes the inserted row count
     * @throws Exception if the insert fails
     */
    @Benchmark
    public void clickhouseJavaV2Client(Blackhole bh) throws Exception {
        try (com.clickhouse.client.api.Client client = resource.openV2Client()) {
            client.register(BenchRowBean.class, client.getTableSchema(SyntheticData.TABLE));
            try (com.clickhouse.client.api.insert.InsertResponse response =
                    client.insert(SyntheticData.TABLE, beanData).get()) {
                bh.consume(response.getWrittenRows());
            }
        }
    }

    // ------------------------------------------------------------------
    // Housepower native JDBC driver (native, port 9000)
    // ------------------------------------------------------------------

    /**
     * Inserts the dataset using the
     * {@code com.github.housepower:clickhouse-native-jdbc} driver over the
     * native protocol via a batched {@link PreparedStatement}.
     *
     * @param bh JMH sink that consumes the inserted row count
     * @throws Exception if the JDBC insert fails
     */
    @Benchmark
    public void housepowerNative(Blackhole bh) throws Exception {
        String url = "jdbc:clickhouse://" + resource.host() + ":" + resource.nativePort() + "/default";
        try (Connection conn = new com.github.housepower.jdbc.ClickHouseDriver().connect(url, competitorProps)) {
            bh.consume(insertJdbc(conn));
        }
    }

    /**
     * Shared JDBC insert path used by all competitor JDBC drivers: prepares the
     * canonical insert statement and submits every row as one batch.
     *
     * <p>The {@code user} column is backtick-quoted because the v2 JDBC driver's
     * client-side ANTLR parser rejects it bare ({@code mismatched input 'user'})
     * and silently falls back to a far slower insert path (~1.5 s and ~2.3 GB
     * allocated per 1M rows vs ~1.0 s quoted, measured against 0.9.0). The server
     * itself accepts the unquoted form; quoting is a no-op for the other drivers.</p>
     *
     * @param conn an open JDBC connection to the {@code default} database
     * @return the number of rows inserted
     * @throws Exception if statement preparation or execution fails
     */
    private long insertJdbc(Connection conn) throws Exception {
        String sql = "INSERT INTO bench (id,ts,`user`,value,status) VALUES (?,?,?,?,?)";
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
