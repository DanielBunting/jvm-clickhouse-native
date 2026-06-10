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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark that exercises the lazy-String materialisation path (improvement 03)
 * over a wide, string-heavy table.
 *
 * <p>The table {@code bench_strings} has {@link #STRING_COLUMNS} wide {@code String}
 * columns plus an {@code id}. Our-native methods contrast the lazy decode story:
 * <ul>
 *   <li>{@code ours_native_all} reads <b>every</b> string column of every row, so every
 *       cell is materialised — measures full-scan decode throughput/allocation.</li>
 *   <li>{@code ours_native_projected} reads <b>only one</b> of the {@link #STRING_COLUMNS}
 *       columns. With lazy materialisation the unread columns' cells are never decoded
 *       into {@code String} objects (only the bulk byte copy happens), so MB/op should
 *       drop sharply versus {@code ours_native_all}.</li>
 * </ul>
 *
 * <p>For a complete head-to-head, the official ClickHouse JDBC (HTTP) and HousePower
 * native JDBC drivers run the same two SELECTs ({@code clickhouseJavaHttp_all/_projected},
 * {@code housepowerNative_all/_projected}), consumed via {@code rs.getString(...).length()}
 * to match the native methods' {@code stringAt(r).length()} semantics.
 *
 * <p>Run with the {@code gc} profiler (configured in the module's {@code jmh} block) to
 * observe bytes-allocated/op. Honors {@code -Dch.host} like the other benchmarks via the
 * shared {@link ClickHouseResource}; otherwise spins a {@code clickhouse/clickhouse-server}
 * Testcontainer through {@link io.github.danielbunting.clickhouse.test.ClickHouseImages#SERVER}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
public class StringSelectBenchmark {

    /** Number of wide {@code String} columns in the table (the "K" of "1 of K"). */
    private static final int STRING_COLUMNS = 8;

    /** Target table for this benchmark. */
    private static final String TABLE = "bench_strings";

    /** Number of rows to pre-load and SELECT. */
    @Param({"1000000"})
    public int rows;

    private ClickHouseResource resource;
    private ClickHouseConnection nativeConn;

    /** Official ClickHouse JDBC connection (HTTP transport, port 8123). */
    private java.sql.Connection httpConn;

    /** HousePower native JDBC connection (native TCP, port 9000). */
    private java.sql.Connection hpConn;

    /** SELECT over all string columns (full materialisation when read). */
    private String selectAllSql;

    /** SELECT over a single string column (projection — showcases lazy non-materialisation). */
    private String selectOneSql;

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Setup(Level.Trial)
    public void setup() throws Exception {
        resource = new ClickHouseResource();
        resource.setUp();

        nativeConn = resource.openNative(CompressionMethod.NONE);

        // Build a table with STRING_COLUMNS wide String columns.
        StringBuilder cols = new StringBuilder("id UInt64");
        for (int c = 0; c < STRING_COLUMNS; c++) {
            cols.append(", s").append(c).append(" String");
        }
        nativeConn.execute("DROP TABLE IF EXISTS " + TABLE);
        nativeConn.execute("CREATE TABLE " + TABLE + " (" + cols + ") ENGINE = MergeTree ORDER BY id");

        // Populate server-side so the loader cost is excluded from the measured methods.
        // Each cell is a wide-ish unique string derived from the row number + column index.
        StringBuilder insertExprs = new StringBuilder("number AS id");
        for (int c = 0; c < STRING_COLUMNS; c++) {
            insertExprs.append(", concat('col").append(c).append("-row-', toString(number), '-")
                    .append("padpadpadpadpadpadpad")
                    .append("') AS s").append(c);
        }
        nativeConn.execute("INSERT INTO " + TABLE + " SELECT " + insertExprs
                + " FROM numbers(" + rows + ")");

        StringBuilder allCols = new StringBuilder("s0");
        for (int c = 1; c < STRING_COLUMNS; c++) {
            allCols.append(", s").append(c);
        }
        selectAllSql = "SELECT " + allCols + " FROM " + TABLE;
        selectOneSql = "SELECT s0 FROM " + TABLE;

        // Open competitor connections (instantiated directly to avoid URL-scheme clash).
        Properties props = resource.competitorProps();
        httpConn = new com.clickhouse.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default", props);
        hpConn = new com.github.housepower.jdbc.ClickHouseDriver()
                .connect("jdbc:clickhouse://" + resource.host() + ":" + resource.nativePort() + "/default", props);
    }

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

    // -----------------------------------------------------------------------
    // Benchmark methods
    // -----------------------------------------------------------------------

    /**
     * Reads and materialises every cell of all {@link #STRING_COLUMNS} string columns.
     * This forces full lazy-decode of the entire result and represents the worst case
     * for allocation (every cell becomes a {@link String}).
     *
     * @param bh JMH blackhole
     * @throws Exception on query failure
     */
    @Benchmark
    public void ours_native_all(Blackhole bh) throws Exception {
        try (var result = nativeConn.query(selectAllSql)) {
            var iter = result.blocks();
            while (iter.hasNext()) {
                Block block = iter.next();
                if (block.isEmpty()) {
                    continue;
                }
                int rowCount = block.rowCount();
                int colCount = block.columnCount();
                long lenSum = 0;
                for (int c = 0; c < colCount; c++) {
                    Column col = block.column(c);
                    for (int r = 0; r < rowCount; r++) {
                        lenSum += col.stringAt(r).length();
                    }
                }
                bh.consume(lenSum);
            }
        }
    }

    /**
     * Reads and materialises only the single projected string column. The other
     * {@link #STRING_COLUMNS}{@code  - 1} columns are not selected at all here; this
     * method exists to contrast wire/decoding cost against {@link #ours_native_all}.
     * With lazy materialisation, the per-cell {@link String} allocations scale with the
     * one read column rather than all K.
     *
     * @param bh JMH blackhole
     * @throws Exception on query failure
     */
    @Benchmark
    public void ours_native_projected(Blackhole bh) throws Exception {
        try (var result = nativeConn.query(selectOneSql)) {
            var iter = result.blocks();
            while (iter.hasNext()) {
                Block block = iter.next();
                if (block.isEmpty()) {
                    continue;
                }
                int rowCount = block.rowCount();
                Column col = block.column(0);
                long lenSum = 0;
                for (int r = 0; r < rowCount; r++) {
                    lenSum += col.stringAt(r).length();
                }
                bh.consume(lenSum);
            }
        }
    }

    /**
     * Official {@code clickhouse-jdbc} (HTTP) reading every string column — the
     * head-to-head counterpart of {@link #ours_native_all}.
     *
     * @param bh JMH blackhole
     * @throws Exception on query failure
     */
    @Benchmark
    public void clickhouseJavaHttp_all(Blackhole bh) throws Exception {
        sumAllStrings(httpConn, bh);
    }

    /**
     * HousePower native JDBC reading every string column — the head-to-head
     * counterpart of {@link #ours_native_all}.
     *
     * @param bh JMH blackhole
     * @throws Exception on query failure
     */
    @Benchmark
    public void housepowerNative_all(Blackhole bh) throws Exception {
        sumAllStrings(hpConn, bh);
    }

    /**
     * Official {@code clickhouse-jdbc} (HTTP) reading only the projected column — the
     * head-to-head counterpart of {@link #ours_native_projected}.
     *
     * @param bh JMH blackhole
     * @throws Exception on query failure
     */
    @Benchmark
    public void clickhouseJavaHttp_projected(Blackhole bh) throws Exception {
        sumProjectedString(httpConn, bh);
    }

    /**
     * HousePower native JDBC reading only the projected column — the head-to-head
     * counterpart of {@link #ours_native_projected}.
     *
     * @param bh JMH blackhole
     * @throws Exception on query failure
     */
    @Benchmark
    public void housepowerNative_projected(Blackhole bh) throws Exception {
        sumProjectedString(hpConn, bh);
    }

    /**
     * Reads all {@link #STRING_COLUMNS} string columns from a JDBC connection,
     * summing their lengths to mirror {@link #ours_native_all}.
     */
    private void sumAllStrings(java.sql.Connection conn, Blackhole bh) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(selectAllSql)) {
            long lenSum = 0;
            while (rs.next()) {
                for (int c = 1; c <= STRING_COLUMNS; c++) {
                    lenSum += rs.getString(c).length();
                }
            }
            bh.consume(lenSum);
        }
    }

    /**
     * Reads the single projected string column from a JDBC connection, summing its
     * lengths to mirror {@link #ours_native_projected}.
     */
    private void sumProjectedString(java.sql.Connection conn, Blackhole bh) throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs   = stmt.executeQuery(selectOneSql)) {
            long lenSum = 0;
            while (rs.next()) {
                lenSum += rs.getString(1).length();
            }
            bh.consume(lenSum);
        }
    }
}
