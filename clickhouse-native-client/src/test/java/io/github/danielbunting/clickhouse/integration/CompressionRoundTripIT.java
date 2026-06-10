package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests that run identical INSERT + SELECT scenarios with multiple
 * compression settings ({@link CompressionMethod#LZ4}, {@link CompressionMethod#NONE},
 * and {@link CompressionMethod#ZSTD}) and assert that the results are identical.
 *
 * <p>These tests exercise the full compressed-block path end-to-end:
 * <ul>
 *   <li>Client sends the Query packet with the compression flag set.</li>
 *   <li>The server compresses its Data-block responses accordingly.</li>
 *   <li>The client decompresses via {@code CompressedBlockCodec} before
 *       handing the raw bytes to {@code BlockCodec}.</li>
 *   <li>INSERT Data blocks sent by the client are also compressed when
 *       compression is enabled.</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class CompressionRoundTripIT extends IntegrationTestBase {

    /** Number of rows inserted in each round-trip scenario. */
    private static final int ROW_COUNT = 500;

    /**
     * Builds a config for the given compression method.
     *
     * @param method the compression method to use
     * @return a fresh {@link ClickHouseConfig}
     */
    private ClickHouseConfig config(CompressionMethod method) {
        return ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .compression(method)
                .build();
    }

    /**
     * Materialises all rows from a {@link QueryResult} as a list of object
     * arrays.  Closed after iteration.
     *
     * @param result the query result to drain
     * @return all rows in encounter order
     */
    @SuppressWarnings("unchecked")
    private List<Object[]> materialize(QueryResult result) {
        List<Object[]> rows = new ArrayList<>();
        Iterator<Block> it = result.blocks();
        while (it.hasNext()) {
            Block block = it.next();
            if (block.isEmpty()) {
                continue;
            }
            int cols  = block.columnCount();
            int nRows = block.rowCount();
            for (int r = 0; r < nRows; r++) {
                Object[] row = new Object[cols];
                for (int c = 0; c < cols; c++) {
                    Column col = block.column(c);
                    boolean isNull = col.nulls() != null && col.nulls()[r];
                    row[c] = isNull ? null : col.value(r);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Inserts {@value #ROW_COUNT} rows into a freshly-created table, then
     * selects them back with the given compression method.
     *
     * @param conn  an open connection configured with the desired compression
     * @param table the target table (must already exist)
     * @return the materialised rows for assertion
     */
    private List<Object[]> insertAndSelect(ClickHouseConnection conn, String table) {
        // Insert rows via literal VALUES (no BulkInserter, to keep this test
        // focused on the SELECT / compressed-response path).
        StringBuilder sb = new StringBuilder(
            "INSERT INTO " + table + " (id, label, val) VALUES");
        for (int i = 0; i < ROW_COUNT; i++) {
            if (i > 0) sb.append(',');
            sb.append(" (").append(i)
              .append(", 'item-").append(i).append("'")
              .append(", ").append(i * 1.1).append(')');
        }
        conn.execute(sb.toString());

        try (QueryResult result = conn.query(
                "SELECT id, label, val FROM " + table + " ORDER BY id")) {
            return materialize(result);
        }
    }

    /**
     * Creates the shared target table for a compression test.
     *
     * @param conn  any open connection (compression-agnostic for DDL)
     * @param table the table name to create
     */
    private void createTable(ClickHouseConnection conn, String table) {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS " + table + " ("
            + "  id    UInt32,"
            + "  label String,"
            + "  val   Float64"
            + ") ENGINE = MergeTree() ORDER BY id");
    }

    /**
     * Asserts that rows selected with LZ4 compression exactly match rows
     * selected with no compression.
     *
     * <p>A compressed-block wire-format bug will cause value corruption or a
     * {@link io.github.danielbunting.clickhouse.ProtocolException}, making the
     * failure loud and obvious.
     */
    @Test
    void lz4AndNoneProduceIdenticalResults() {
        String tableNone = "comp_rt_none_" + System.nanoTime();
        String tableLz4  = "comp_rt_lz4_"  + System.nanoTime();

        try (ClickHouseConnection noComp  = ClickHouseConnection.open(config(CompressionMethod.NONE));
             ClickHouseConnection lz4Conn = ClickHouseConnection.open(config(CompressionMethod.LZ4))) {

            createTable(noComp, tableNone);
            createTable(lz4Conn, tableLz4);

            List<Object[]> noneRows = insertAndSelect(noComp,  tableNone);
            List<Object[]> lz4Rows  = insertAndSelect(lz4Conn, tableLz4);

            assertEquals(ROW_COUNT, noneRows.size(),
                    "NONE: expected " + ROW_COUNT + " rows — baseline SELECT broken");
            assertEquals(ROW_COUNT, lz4Rows.size(),
                    "LZ4: expected " + ROW_COUNT + " rows — LZ4 decompression returning wrong row count");

            for (int i = 0; i < ROW_COUNT; i++) {
                Object[] rNone = noneRows.get(i);
                Object[] rLz4  = lz4Rows.get(i);

                assertEquals(((Number) rNone[0]).longValue(),
                             ((Number) rLz4[0]).longValue(),
                        "Row " + i + " id: LZ4 vs NONE mismatch — compressed block framing bug");
                assertEquals(rNone[1], rLz4[1],
                        "Row " + i + " label: LZ4 vs NONE mismatch — string decompression bug");
                assertEquals(((Number) rNone[2]).doubleValue(),
                             ((Number) rLz4[2]).doubleValue(), 1e-12,
                        "Row " + i + " val: LZ4 vs NONE mismatch — Float64 compression bug");
            }

            noComp.execute("DROP TABLE IF EXISTS " + tableNone);
            lz4Conn.execute("DROP TABLE IF EXISTS " + tableLz4);
        }
    }

    /**
     * Asserts that rows selected with ZSTD compression exactly match rows
     * selected with no compression.
     *
     * <p>Uses the same assertion pattern as {@link #lz4AndNoneProduceIdenticalResults()}
     * to exercise the ZSTD decompressor path ({@code CompressedBlockCodec} marker
     * {@code 0x90}).
     */
    @Test
    void zstdAndNoneProduceIdenticalResults() {
        String tableNone  = "comp_rt_znone_"  + System.nanoTime();
        String tableZstd  = "comp_rt_zstd_"   + System.nanoTime();

        try (ClickHouseConnection noComp   = ClickHouseConnection.open(config(CompressionMethod.NONE));
             ClickHouseConnection zstdConn = ClickHouseConnection.open(config(CompressionMethod.ZSTD))) {

            createTable(noComp,   tableNone);
            createTable(zstdConn, tableZstd);

            List<Object[]> noneRows = insertAndSelect(noComp,   tableNone);
            List<Object[]> zstdRows = insertAndSelect(zstdConn, tableZstd);

            assertEquals(ROW_COUNT, noneRows.size(),
                    "NONE: expected " + ROW_COUNT + " rows — baseline SELECT broken");
            assertEquals(ROW_COUNT, zstdRows.size(),
                    "ZSTD: expected " + ROW_COUNT + " rows — ZSTD decompression returning wrong row count");

            for (int i = 0; i < ROW_COUNT; i++) {
                Object[] rNone = noneRows.get(i);
                Object[] rZstd = zstdRows.get(i);

                assertEquals(((Number) rNone[0]).longValue(),
                             ((Number) rZstd[0]).longValue(),
                        "Row " + i + " id: ZSTD vs NONE mismatch — compressed block framing bug");
                assertEquals(rNone[1], rZstd[1],
                        "Row " + i + " label: ZSTD vs NONE mismatch — string decompression bug");
                assertEquals(((Number) rNone[2]).doubleValue(),
                             ((Number) rZstd[2]).doubleValue(), 1e-12,
                        "Row " + i + " val: ZSTD vs NONE mismatch — Float64 compression bug");
            }

            noComp.execute("DROP TABLE IF EXISTS " + tableNone);
            zstdConn.execute("DROP TABLE IF EXISTS " + tableZstd);
        }
    }

    /**
     * Exercises LZ4 compression end-to-end with a small bulk INSERT
     * (so compressed client Data blocks are also sent), then verifies count
     * and a sample of values.
     *
     * <p>This specifically validates that the INSERT data blocks written by the
     * client are correctly compressed and that the server accepts them.
     * A mismatch in the client-side compression flag in the Query packet would
     * cause the server to reject or misparse the compressed Data blocks.
     */
    @Test
    void lz4CompressedBulkInsertRoundTrips() {
        String table = "comp_bulk_lz4_" + System.nanoTime();

        try (ClickHouseConnection conn = ClickHouseConnection.open(config(CompressionMethod.LZ4))) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  val String"
                + ") ENGINE = MergeTree() ORDER BY id");

            // Insert via VALUES literals — the client compresses the Data blocks.
            StringBuilder sb = new StringBuilder(
                "INSERT INTO " + table + " (id, val) VALUES");
            for (int i = 0; i < ROW_COUNT; i++) {
                if (i > 0) sb.append(',');
                sb.append(" (").append(i).append(", 'v").append(i).append("')");
            }
            conn.execute(sb.toString());

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(ROW_COUNT, count,
                    "LZ4 bulk: expected " + ROW_COUNT
                    + " rows — INSERT may not have been acknowledged under compression");

            // Verify a known row
            try (QueryResult result = conn.query(
                    "SELECT id, val FROM " + table + " WHERE id = 42")) {
                List<Object[]> rows = materialize(result);
                assertEquals(1, rows.size(),
                        "LZ4 bulk: expected 1 row for id=42");
                assertNotNull(rows.get(0), "LZ4 bulk: row for id=42 must not be null");
                assertEquals(42L, ((Number) rows.get(0)[0]).longValue(),
                        "LZ4 bulk: id column for row id=42");
                assertEquals("v42", rows.get(0)[1],
                        "LZ4 bulk: val column for row id=42 — string round-trip under LZ4 broken");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Exercises the reused per-connection compressed reader across MANY blocks,
     * each spanning MULTIPLE compressed frames, under both LZ4 and ZSTD.
     *
     * <p>This is the targeted regression test for the C2 optimisation (reusing a
     * single {@code CompressedFrameInputStream} + unbuffered {@code DefaultBinaryReader}
     * across all blocks, with a {@code reset()} between blocks). It is designed to
     * trip any frame-boundary desync the reuse could introduce:
     * <ul>
     *   <li><b>Many native blocks</b> — selects {@code ROWS} (&gt; the ~65k native
     *       block size) so the response is split across several Data packets; the
     *       reused reader's {@code reset()} path runs once per block.</li>
     *   <li><b>Multi-frame blocks</b> — each row carries a ~2 KiB string, so a single
     *       65k-row block serialises to well over the per-frame ~1 MiB limit and the
     *       server emits multiple compressed frames per block; the stream must
     *       transparently advance across frames within one block.</li>
     * </ul>
     * Result is compared row-for-row against the identical SELECT over an
     * uncompressed connection. Any stale byte surviving a {@code reset()}, any
     * read-ahead past a block boundary, or any cross-frame advance bug corrupts a
     * value or throws, failing this test.
     */
    @Test
    void reusedCompressedReaderAcrossManyMultiFrameBlocks() {
        // 70k rows > one ~65k native block => multiple blocks per response.
        // Each value ~2 KiB => one block's uncompressed section >> 1 MiB => multi-frame.
        final int rows = 70_000;
        String table = "comp_multiframe_" + System.nanoTime();

        try (ClickHouseConnection setup = ClickHouseConnection.open(config(CompressionMethod.NONE))) {
            setup.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  payload String"
                + ") ENGINE = MergeTree() ORDER BY id");
            // Generate server-side to avoid a giant client INSERT statement. Each
            // payload is a deterministic ~2 KiB string keyed by id.
            setup.execute(
                "INSERT INTO " + table + " (id, payload) "
                + "SELECT number AS id, repeat(concat(toString(number), '-'), 256) AS payload "
                + "FROM numbers(" + rows + ")");
        }

        final String select = "SELECT id, payload FROM " + table + " ORDER BY id";

        List<Object[]> noneRows;
        try (ClickHouseConnection noComp = ClickHouseConnection.open(config(CompressionMethod.NONE));
             QueryResult r = noComp.query(select)) {
            noneRows = materialize(r);
        }
        assertEquals(rows, noneRows.size(),
                "NONE baseline must return all " + rows + " rows");

        for (CompressionMethod method : new CompressionMethod[]{
                CompressionMethod.LZ4, CompressionMethod.ZSTD}) {
            List<Object[]> compRows;
            try (ClickHouseConnection conn = ClickHouseConnection.open(config(method));
                 QueryResult r = conn.query(select)) {
                compRows = materialize(r);
            }
            assertEquals(rows, compRows.size(),
                    method + ": expected " + rows + " rows across many multi-frame blocks "
                    + "— frame-boundary desync in the reused compressed reader");
            for (int i = 0; i < rows; i++) {
                assertEquals(((Number) noneRows.get(i)[0]).longValue(),
                             ((Number) compRows.get(i)[0]).longValue(),
                        method + " row " + i + " id mismatch — block/frame boundary corruption");
                assertEquals(noneRows.get(i)[1], compRows.get(i)[1],
                        method + " row " + i + " payload mismatch — multi-frame string corruption");
            }
        }

        try (ClickHouseConnection cleanup = ClickHouseConnection.open(config(CompressionMethod.NONE))) {
            cleanup.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Verifies that the {@code SELECT count()} scalar path works correctly
     * under all three compression methods.  This is the simplest possible
     * compressed-block round-trip and should be the first test to pass when
     * implementing compression support.
     */
    @Test
    void scalarCountWorksUnderAllCompressionMethods() {
        String table = "comp_scalar_" + System.nanoTime();

        // Create and populate the table once using NONE compression.
        try (ClickHouseConnection setup = ClickHouseConnection.open(config(CompressionMethod.NONE))) {
            setup.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  n UInt32"
                + ") ENGINE = MergeTree() ORDER BY n");
            setup.execute("INSERT INTO " + table + " (n) VALUES (1),(2),(3),(4),(5)");
        }

        // Now count using each compression method independently.
        for (CompressionMethod method : CompressionMethod.values()) {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config(method))) {
                long count = conn.executeScalar("SELECT count() FROM " + table);
                assertEquals(5L, count,
                        "count() via " + method + " compression must return 5 — "
                        + "compressed-block decompression or framing bug for method " + method);
            }
        }

        // Drop using NONE to avoid compression state from the loop.
        try (ClickHouseConnection cleanup = ClickHouseConnection.open(config(CompressionMethod.NONE))) {
            cleanup.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
