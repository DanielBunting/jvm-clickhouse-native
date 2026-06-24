package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.BulkInserter;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Wide-table companion to {@link BulkInsertBenchmark}: inserts a 30-column
 * fixed-width dataset ({@code bench_wide}, 15 × {@code UInt64} +
 * 15 × {@code Float64}, 240 raw bytes/row) through the same driver paths.
 *
 * <p>Motivation: per-cell costs — the server's row→column transpose for
 * row-major formats and the per-cell dispatch in row-shaped driver APIs —
 * scale with column count, while per-row and per-INSERT fixed costs amortize.
 * The 5-column {@code bench} table dilutes those effects; this schema isolates
 * them. String columns are deliberately absent so UTF-8 work (which costs every
 * driver the same) doesn't mask the format difference.</p>
 *
 * <p>Variants mirror {@link BulkInsertBenchmark}: this project's native
 * {@link BulkInserter} (LZ4 / none), the official driver's idiomatic
 * {@code client-v2} POJO path, both generations of the official JDBC driver,
 * and HousePower. See {@link InsertBreakdown} for the matching client/server
 * time split.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class WideBulkInsertBenchmark {

    /** Shared ClickHouse endpoint / container managed by the JMH state class. */
    private final ClickHouseResource resource = new ClickHouseResource();

    /** Number of rows inserted per benchmark invocation. */
    @Param({"1000000"})
    public int rows;

    /** Pre-generated wide dataset shared across all benchmark methods. */
    private List<WideRow> data;

    /** The same dataset as JavaBeans for the client-v2 POJO path (see {@link WideRowBean}). */
    private List<WideRowBean> beanData;

    /** Connection properties for the competitor JDBC drivers. */
    private Properties competitorProps;

    /** The parameterised wide INSERT statement shared by the JDBC variants. */
    private static final String JDBC_SQL = buildJdbcSql();

    private static String buildJdbcSql() {
        StringBuilder cols = new StringBuilder();
        StringBuilder marks = new StringBuilder();
        for (int c = 0; c < 15; c++) {
            cols.append("l").append(c).append(',');
            marks.append("?,");
        }
        for (int c = 0; c < 15; c++) {
            cols.append("d").append(c);
            marks.append('?');
            if (c < 14) {
                cols.append(',');
                marks.append(',');
            }
        }
        return "INSERT INTO " + WideSyntheticData.TABLE + " (" + cols + ") VALUES (" + marks + ")";
    }

    /**
     * Starts (or connects to) the ClickHouse server and pre-generates the wide
     * dataset. Runs once per trial.
     *
     * @throws Exception if the resource fails to start
     */
    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
        resource.setUp();
        this.data = WideSyntheticData.generate(rows);
        this.beanData = data.stream().map(WideRowBean::new).toList();
        this.competitorProps = resource.competitorProps();
    }

    /**
     * Stops the ClickHouse resource. Runs once per trial.
     *
     * @throws Exception if the resource fails to stop
     */
    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        resource.tearDown();
    }

    /**
     * Recreates an empty {@code bench_wide} table before every measured
     * invocation so each insert starts from a clean slate.
     */
    @Setup(Level.Invocation)
    public void recreateTable() {
        try (ClickHouseConnection conn = resource.openNative(CompressionMethod.NONE)) {
            conn.execute("DROP TABLE IF EXISTS " + WideSyntheticData.TABLE);
            conn.execute(WideSyntheticData.DDL);
        }
    }

    // ------------------------------------------------------------------
    // clickhouse-native-client (this project)
    // ------------------------------------------------------------------

    /**
     * Inserts the wide dataset using this project's native bulk inserter with
     * LZ4 compression.
     *
     * @param bh JMH sink that consumes the inserted row count
     */
    @Benchmark
    public void ours_lz4(Blackhole bh) {
        bh.consume(insertOurs(CompressionMethod.LZ4));
    }

    /**
     * Inserts the wide dataset using this project's native bulk inserter with
     * no compression.
     *
     * @param bh JMH sink that consumes the inserted row count
     */
    @Benchmark
    public void ours_none(Blackhole bh) {
        bh.consume(insertOurs(CompressionMethod.NONE));
    }

    private long insertOurs(CompressionMethod method) {
        try (ClickHouseConnection conn = resource.openNative(method);
             BulkInserter<WideRow> ins = conn.createBulkInserter(WideSyntheticData.TABLE, WideRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
            return data.size();
        }
    }

    // ------------------------------------------------------------------
    // Official drivers (HTTP, port 8123)
    // ------------------------------------------------------------------

    /**
     * Inserts the wide dataset using the official {@code client-v2} POJO path
     * (schema-compiled getter serializers, one RowBinary HTTP INSERT).
     *
     * @param bh JMH sink that consumes the inserted row count
     * @throws Exception if the insert fails
     */
    @Benchmark
    public void clickhouseJavaV2Client(Blackhole bh) throws Exception {
        try (com.clickhouse.client.api.Client client = resource.openV2Client()) {
            client.register(WideRowBean.class, client.getTableSchema(WideSyntheticData.TABLE));
            try (com.clickhouse.client.api.insert.InsertResponse response =
                    client.insert(WideSyntheticData.TABLE, beanData).get()) {
                bh.consume(response.getWrittenRows());
            }
        }
    }

    /**
     * Inserts the wide dataset using the official driver's legacy v1 JDBC
     * implementation over HTTP via a batched {@link PreparedStatement}.
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
     * Inserts the wide dataset using the official driver's v2 JDBC
     * implementation over HTTP via a batched {@link PreparedStatement}.
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

    // ------------------------------------------------------------------
    // Housepower native JDBC driver (native, port 9000)
    // ------------------------------------------------------------------

    /**
     * Inserts the wide dataset using the HousePower native JDBC driver over the
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
     * Shared JDBC insert path: binds all 30 columns positionally per row and
     * submits everything as one batch.
     *
     * @param conn an open JDBC connection to the {@code default} database
     * @return the number of rows inserted
     * @throws Exception if statement preparation or execution fails
     */
    private long insertJdbc(Connection conn) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(JDBC_SQL)) {
            for (WideRow row : data) {
                for (int c = 0; c < 15; c++) {
                    ps.setLong(c + 1, row.longAt(c));
                }
                for (int c = 0; c < 15; c++) {
                    ps.setDouble(c + 16, row.doubleAt(c));
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return data.size();
    }
}
