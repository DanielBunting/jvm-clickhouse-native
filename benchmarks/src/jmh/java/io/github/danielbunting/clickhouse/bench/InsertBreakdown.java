package io.github.danielbunting.clickhouse.bench;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

/**
 * Diagnostic runner (not a JMH benchmark) that splits a 1M-row bulk insert into
 * client-side and server-side time for each driver path, answering "how much of
 * the wall clock is the driver, and how much is the server?". Runs the suite
 * twice: against the narrow 5-column {@code bench} table and against the
 * 30-column fixed-width {@code bench_wide} table, where per-cell costs (the
 * server's row→column transpose, per-cell driver dispatch) dominate per-row
 * fixed costs.
 *
 * <p>For every variant it performs the same insert as {@link BulkInsertBenchmark}
 * / {@link WideBulkInsertBenchmark}, then reads the server's own accounting for
 * that exact INSERT from {@code system.query_log} (after
 * {@code SYSTEM FLUSH LOGS}):</p>
 *
 * <ul>
 *   <li><b>client wall</b> — what the application observes (includes connection
 *       setup, mapping, serialization, transport, and waiting for the server);</li>
 *   <li><b>server duration</b> — {@code query_duration_ms}: first byte of the query
 *       to the INSERT being fully committed, as seen by the server;</li>
 *   <li><b>server CPU</b> — {@code ProfileEvents['OSCPUVirtualTimeMicroseconds']}:
 *       actual processor time the INSERT cost the server (parse/transpose, sort,
 *       compress, write part);</li>
 *   <li><b>net recv wait</b> — {@code ProfileEvents['NetworkReceiveElapsedMicroseconds']}:
 *       time the server's query thread sat idle waiting for the client to send the
 *       next bytes. High values mean the client (or its serialization) is the
 *       bottleneck and the transfer is pipelined; ~0 means the body arrived at full
 *       speed (serialize-then-ship).</li>
 * </ul>
 *
 * <p>Reading the split: {@code client wall ≈ server duration} means production and
 * ingestion overlap (streaming); {@code client wall >> server duration} means the
 * client spends time before/after the server sees bytes (e.g. serializing the whole
 * body first). {@code server duration − net recv wait} approximates pure server-side
 * processing for the insert.</p>
 *
 * <p>Run against the same Testcontainers/external server as the JMH suite:</p>
 *
 * <pre>{@code
 * java -cp benchmarks-<version>-jmh.jar io.github.danielbunting.clickhouse.bench.InsertBreakdown
 * }</pre>
 *
 * <p>Caveats: single-shot timings in Docker are noisy (the JMH suite remains the
 * source of truth for wall-clock comparisons); this tool's value is the *ratio*
 * between the columns per variant, which is stable across reps.</p>
 */
public final class InsertBreakdown {

    /** Rows per insert; matches the JMH bulk benchmark's default. */
    private static final int ROWS = 1_000_000;

    /** Measured repetitions per variant (after one discarded warmup). */
    private static final int REPS = 3;

    private InsertBreakdown() {
    }

    /** One variant's insert action, given the shared dataset. */
    private interface InsertAction {
        void run() throws Exception;
    }

    /** A named insert path to measure. */
    private record Variant(String name, InsertAction action) {
    }

    /** Server-side accounting for one INSERT, from {@code system.query_log}. */
    private record ServerStats(long durationMs, long cpuMicros, long netRecvMicros) {
    }

    public static void main(String[] args) throws Exception {
        ClickHouseResource resource = new ClickHouseResource();
        resource.setUp();
        try {
            Properties props = resource.competitorProps();
            String httpUrl = "jdbc:clickhouse://" + resource.host() + ":" + resource.httpPort() + "/default";
            String nativeUrl = "jdbc:clickhouse://" + resource.host() + ":" + resource.nativePort() + "/default";

            // Control connection for DDL and query_log reads (never inserts into the targets).
            try (ClickHouseConnection control = resource.openNative(CompressionMethod.NONE)) {

                List<BenchRow> data = SyntheticData.generate(ROWS);
                List<BenchRowBean> beanData = data.stream().map(BenchRowBean::new).toList();
                runSuite(control,
                        "narrow (bench: 5 columns, ~37 raw B/row)",
                        () -> resource.recreateTable(control),
                        // 'INSERT INTO%bench%' alone would also match bench_wide.
                        "query ILIKE 'INSERT INTO%bench%' AND query NOT ILIKE '%" + WideSyntheticData.TABLE + "%'",
                        List.of(
                                new Variant("ours (native, LZ4)", () -> insertOurs(resource, CompressionMethod.LZ4, data)),
                                new Variant("ours (native, none)", () -> insertOurs(resource, CompressionMethod.NONE, data)),
                                new Variant("client-v2 (POJO)", () -> insertV2Client(resource, beanData)),
                                new Variant("jdbc v1 (legacy, HTTP)", () -> insertJdbc(
                                        new com.clickhouse.jdbc.DriverV1().connect(httpUrl, props), data)),
                                new Variant("jdbc v2 (HTTP)", () -> insertJdbc(
                                        new com.clickhouse.jdbc.ClickHouseDriver().connect(httpUrl, props), data)),
                                new Variant("housepower (native)", () -> insertJdbc(
                                        new com.github.housepower.jdbc.ClickHouseDriver().connect(nativeUrl, props), data))));

                List<WideRow> wideData = WideSyntheticData.generate(ROWS);
                List<WideRowBean> wideBeanData = wideData.stream().map(WideRowBean::new).toList();
                runSuite(control,
                        "wide (bench_wide: 30 fixed-width columns, 240 raw B/row)",
                        () -> {
                            control.execute("DROP TABLE IF EXISTS " + WideSyntheticData.TABLE);
                            control.execute(WideSyntheticData.DDL);
                        },
                        "query ILIKE 'INSERT INTO%" + WideSyntheticData.TABLE + "%'",
                        List.of(
                                new Variant("ours (native, LZ4)", () -> insertOursWide(resource, CompressionMethod.LZ4, wideData)),
                                new Variant("ours (native, none)", () -> insertOursWide(resource, CompressionMethod.NONE, wideData)),
                                new Variant("client-v2 (POJO)", () -> insertV2ClientWide(resource, wideBeanData)),
                                new Variant("jdbc v1 (legacy, HTTP)", () -> insertJdbcWide(
                                        new com.clickhouse.jdbc.DriverV1().connect(httpUrl, props), wideData)),
                                new Variant("jdbc v2 (HTTP)", () -> insertJdbcWide(
                                        new com.clickhouse.jdbc.ClickHouseDriver().connect(httpUrl, props), wideData)),
                                new Variant("housepower (native)", () -> insertJdbcWide(
                                        new com.github.housepower.jdbc.ClickHouseDriver().connect(nativeUrl, props), wideData))));
            }
        } finally {
            resource.tearDown();
        }
    }

    /** Runs one schema's variants, printing a markdown table of the medians. */
    private static void runSuite(ClickHouseConnection control, String title, Runnable recreate,
                                 String queryLogFilter, List<Variant> variants) throws Exception {
        System.out.printf("%nInsert breakdown — %s — %,d rows, %d measured reps (median shown)%n%n", title, ROWS, REPS);
        System.out.println("| Variant | Client wall | Server duration | Server CPU | Net recv wait | Wall − server dur |");
        System.out.println("|---|---|---|---|---|---|");

        for (Variant variant : variants) {
            long[] wall = new long[REPS];
            long[] dur = new long[REPS];
            long[] cpu = new long[REPS];
            long[] net = new long[REPS];

            for (int rep = -1; rep < REPS; rep++) { // rep -1 = warmup, discarded
                recreate.run();
                long start = System.nanoTime();
                variant.action().run();
                long wallMs = (System.nanoTime() - start) / 1_000_000;
                ServerStats stats = lastInsertStats(control, queryLogFilter);
                if (rep >= 0) {
                    wall[rep] = wallMs;
                    dur[rep] = stats.durationMs();
                    cpu[rep] = stats.cpuMicros() / 1_000;
                    net[rep] = stats.netRecvMicros() / 1_000;
                }
            }

            long w = median(wall);
            long d = median(dur);
            System.out.printf("| %s | %,d ms | %,d ms | %,d ms | %,d ms | %,d ms |%n",
                    variant.name(), w, d, median(cpu), median(net), w - d);
        }
    }

    /** Fetches the server-side stats of the most recent INSERT matching {@code queryLogFilter}. */
    private static ServerStats lastInsertStats(ClickHouseConnection control, String queryLogFilter) {
        control.execute("SYSTEM FLUSH LOGS");
        String filter = "FROM system.query_log"
                + " WHERE type = 'QueryFinish' AND " + queryLogFilter
                + " AND event_date >= yesterday()"
                + " ORDER BY event_time_microseconds DESC LIMIT 1";
        long duration = control.executeScalar("SELECT query_duration_ms " + filter);
        long cpuMicros = control.executeScalar(
                "SELECT ProfileEvents['OSCPUVirtualTimeMicroseconds'] " + filter);
        long netMicros = control.executeScalar(
                "SELECT ProfileEvents['NetworkReceiveElapsedMicroseconds'] " + filter);
        return new ServerStats(duration, cpuMicros, netMicros);
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        java.util.Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    // ------------------------------------------------------------------
    // Insert paths, mirroring BulkInsertBenchmark
    // ------------------------------------------------------------------

    private static void insertOurs(ClickHouseResource resource, CompressionMethod method, List<BenchRow> data)
            throws Exception {
        try (ClickHouseConnection conn = resource.openNative(method);
             BulkInserter<BenchRow> ins = conn.createBulkInserter(SyntheticData.TABLE, BenchRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
        }
    }

    private static void insertV2Client(ClickHouseResource resource, List<BenchRowBean> beanData) throws Exception {
        try (com.clickhouse.client.api.Client client = resource.openV2Client()) {
            client.register(BenchRowBean.class, client.getTableSchema(SyntheticData.TABLE));
            try (com.clickhouse.client.api.insert.InsertResponse response =
                    client.insert(SyntheticData.TABLE, beanData).get()) {
                response.getWrittenRows();
            }
        }
    }

    private static void insertJdbc(Connection conn, List<BenchRow> data) throws Exception {
        try (conn) {
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
        }
    }

    // ------------------------------------------------------------------
    // Wide-table insert paths, mirroring WideBulkInsertBenchmark
    // ------------------------------------------------------------------

    private static void insertOursWide(ClickHouseResource resource, CompressionMethod method, List<WideRow> data)
            throws Exception {
        try (ClickHouseConnection conn = resource.openNative(method);
             BulkInserter<WideRow> ins = conn.createBulkInserter(WideSyntheticData.TABLE, WideRow.class)) {
            ins.init();
            ins.addRange(data);
            ins.complete();
        }
    }

    private static void insertV2ClientWide(ClickHouseResource resource, List<WideRowBean> beanData) throws Exception {
        try (com.clickhouse.client.api.Client client = resource.openV2Client()) {
            client.register(WideRowBean.class, client.getTableSchema(WideSyntheticData.TABLE));
            try (com.clickhouse.client.api.insert.InsertResponse response =
                    client.insert(WideSyntheticData.TABLE, beanData).get()) {
                response.getWrittenRows();
            }
        }
    }

    private static void insertJdbcWide(Connection conn, List<WideRow> data) throws Exception {
        try (conn) {
            StringBuilder cols = new StringBuilder();
            StringBuilder marks = new StringBuilder();
            for (int c = 0; c < 15; c++) {
                cols.append('l').append(c).append(',');
                marks.append("?,");
            }
            for (int c = 0; c < 15; c++) {
                cols.append('d').append(c);
                marks.append('?');
                if (c < 14) {
                    cols.append(',');
                    marks.append(',');
                }
            }
            String sql = "INSERT INTO " + WideSyntheticData.TABLE + " (" + cols + ") VALUES (" + marks + ")";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
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
        }
    }
}
