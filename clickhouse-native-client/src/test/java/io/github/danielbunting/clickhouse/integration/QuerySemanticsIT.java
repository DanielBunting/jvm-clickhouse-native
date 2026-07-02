package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Query-semantics integration tests ported from the official client-v2 suite
 * ({@code query/QueryTests}): duplicate column labels through the native
 * {@link QueryResult} surface, and {@code max_execution_time} surfacing a
 * {@link ServerException} with code 159 (TIMEOUT_EXCEEDED).
 *
 * <p>API mapping to this client: the native {@link QueryResult} is purely
 * positional ({@code columnNames()} + block columns) — there is no name-based
 * row getter — so duplicate labels are legal and lossless at this level. The
 * only name-based access is the mapped path
 * ({@code query(sql, Class)}), whose duplicate-label resolution is pinned here.
 * Per-query server settings travel via {@code query(sql, Map)}.
 *
 * <p>Run: {@code ./gradlew :clickhouse-native-client:integrationTest --tests '*QuerySemanticsIT'}
 */
@Tag("integration")
class QuerySemanticsIT extends TypeRoundTripBase {

    /** Record with a single component matching the duplicated label. */
    record DupRow(long a) {}

    // =======================================================================================
    // 6. Duplicate column labels (client-v2 QueryTests#testDuplicateColumnNames)
    // =======================================================================================

    /**
     * Duplicate labels in a simple SELECT. Two DIFFERENT expressions under the same
     * alias ({@code SELECT 1 AS a, 2 AS a}) are rejected by the server itself
     * (MULTIPLE_EXPRESSIONS_FOR_ALIAS, code 179) — pinned first. Duplicate labels
     * are still reachable by selecting the SAME column twice
     * ({@code SELECT a, a FROM (SELECT 1 AS a)}): the native result surface must
     * expose BOTH columns — {@code columnNames()} lists the duplicate label twice
     * and the block yields both values positionally (nothing is dropped or renamed).
     *
     * <p>The mapped record path is the client's only name-based access; pinned: a
     * duplicated label resolves without error (the component-to-column index is
     * built by overwriting a name-keyed map in column order, so it lands on the
     * last matching column — indistinguishable here since both carry the same
     * value, but the lookup itself must not reject the duplicate).
     */
    @Test
    void duplicateColumnLabelsInSimpleSelect() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            // Distinct expressions under one alias: the server refuses the query outright.
            ServerException alias = assertThrows(ServerException.class,
                    () -> {
                        try (QueryResult r = conn.query("SELECT 1 AS a, 2 AS a")) {
                            materialize(r);
                        }
                    });
            assertEquals(179, alias.code(),
                    "expected MULTIPLE_EXPRESSIONS_FOR_ALIAS (179), got: " + alias.getMessage());

            try (QueryResult result = conn.query("SELECT a, a FROM (SELECT 1 AS a)")) {
                assertEquals(List.of("a", "a"), result.columnNames(),
                        "both duplicate labels must be reported, in order");
                List<Object[]> rows = materialize(result);
                assertEquals(1, rows.size(), "one row expected");
                assertEquals(1L, ((Number) rows.get(0)[0]).longValue(),
                        "positional access: first 'a'");
                assertEquals(1L, ((Number) rows.get(0)[1]).longValue(),
                        "positional access: second 'a'");
            }

            // Pinned: the mapped path tolerates the duplicated label.
            try (Stream<DupRow> mapped =
                         conn.query("SELECT a, a FROM (SELECT 1 AS a)", DupRow.class)) {
                DupRow row = mapped.findFirst().orElseThrow();
                assertEquals(1L, row.a(), "duplicate label resolves through the mapper");
            }
        }
    }

    /**
     * Duplicate labels produced by a cross join of two tables that both have a
     * {@code name} column (the reference's second scenario): the server
     * disambiguates the second column as {@code <table2>.name}; the native
     * result must carry both columns with their server-given names and the
     * correct positional values.
     */
    @Test
    void duplicateColumnNamesFromCrossJoin() {
        String t1 = "qsem_dup1_" + System.nanoTime();
        String t2 = "qsem_dup2_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            try {
                conn.execute("CREATE TABLE " + t1
                        + " (name String) ENGINE = MergeTree ORDER BY name");
                conn.execute("INSERT INTO " + t1 + " VALUES ('some name')");
                conn.execute("CREATE TABLE " + t2
                        + " (name String) ENGINE = MergeTree ORDER BY name");
                conn.execute("INSERT INTO " + t2 + " VALUES ('another name')");

                try (QueryResult result =
                             conn.query("SELECT * FROM " + t1 + ", " + t2)) {
                    List<String> names = result.columnNames();
                    assertEquals(2, names.size(), "cross join must yield both name columns");
                    assertEquals("name", names.get(0), "first column keeps the bare label");
                    assertTrue(names.get(1).endsWith(".name") && names.get(1).contains(t2),
                            "second column must be qualified with the second table, got: "
                                    + names.get(1));

                    List<Object[]> rows = materialize(result);
                    assertEquals(1, rows.size(), "1x1 cross join yields one row");
                    assertEquals("some name", String.valueOf(rows.get(0)[0]),
                            "first table's value in position 0");
                    assertEquals("another name", String.valueOf(rows.get(0)[1]),
                            "second table's value in position 1");
                }
            } finally {
                conn.execute("DROP TABLE IF EXISTS " + t1);
                conn.execute("DROP TABLE IF EXISTS " + t2);
            }
        }
    }

    // =======================================================================================
    // 7. max_execution_time -> ServerException code 159 (client-v2 QueryTests#testMaxExecutionTime)
    // =======================================================================================

    /**
     * A per-query {@code max_execution_time=1} on a query running ~2s must be
     * aborted by the server with TIMEOUT_EXCEEDED, surfaced through the native
     * client as a {@link ServerException} with {@code code() == 159}. A server
     * exception is a clean wire event: the connection must NOT be poisoned and
     * must remain usable afterwards.
     */
    @Test
    void maxExecutionTimeSurfacesTimeoutExceeded159() {
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                ServerException e = assertThrows(ServerException.class, () -> {
                    try (QueryResult result = conn.query("SELECT sleep(2)",
                            Map.of("max_execution_time", "1"))) {
                        materialize(result); // the exception may arrive mid-stream
                    }
                });
                assertEquals(159, e.code(),
                        "expected TIMEOUT_EXCEEDED (159), got: " + e.getMessage());

                // Clean server exception: connection stays healthy and reusable.
                assertFalse(conn.isPoisoned(),
                        "a max_execution_time server exception must not poison the connection");
                assertEquals(1L, conn.executeScalar("SELECT 1"),
                        "connection must be reusable after the timeout exception");
            }
        });
    }

    /**
     * A per-query {@code log_comment} lands in {@code system.query_log} (reference:
     * client-v2 ClientTests#testLogComment).
     */
    @Test
    void logCommentIsRecordedInQueryLog() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            String comment = "qsem_log_comment_" + System.nanoTime();
            try (QueryResult r = conn.query("SELECT 1", Map.of("log_comment", comment))) {
                materialize(r);
            }
            conn.execute("SYSTEM FLUSH LOGS");
            long hits = conn.executeScalar(
                    "SELECT count() FROM system.query_log WHERE log_comment = '" + comment + "'");
            assertTrue(hits >= 1, "the log_comment setting must be recorded in system.query_log");
        }
    }

    /**
     * Server error codes and full (multi-line) messages surface for a spread of error
     * families (reference: client-v2 ClientTests#testServerErrorsUncompressed): unknown
     * table (60), unknown identifier (47), syntax error (62).
     */
    @Test
    void serverErrorCodesAndMessagesSurfaceAcrossErrorFamilies() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            record Case(String sql, int code) {}
            List<Case> cases = List.of(
                    new Case("SELECT * FROM qsem_no_such_table_" + System.nanoTime(), 60),
                    new Case("SELECT no_such_column FROM system.one", 47),
                    new Case("SELECT * FRM system.one", 62));
            for (Case c : cases) {
                ServerException e = assertThrows(ServerException.class, () -> {
                    try (QueryResult r = conn.query(c.sql())) {
                        materialize(r);
                    }
                }, c.sql());
                assertEquals(c.code(), e.code(), "error code for: " + c.sql());
                assertTrue(e.getMessage() != null && e.getMessage().contains("DB::Exception"),
                        "full server message text is preserved: " + e.getMessage());
                // The connection survives each clean server error.
                assertEquals(1L, conn.executeScalar("SELECT 1"));
            }
        }
    }

    /**
     * A server error arriving over a COMPRESSED response stream still parses cleanly
     * (reference: client-v2 ClientTests#testServerErrorHandling with compression): the
     * exception packet is itself framed like any other packet, per compression method.
     */
    @Test
    void serverErrorSurvivesResponseCompression() {
        for (io.github.danielbunting.clickhouse.compress.CompressionMethod method :
                new io.github.danielbunting.clickhouse.compress.CompressionMethod[] {
                        io.github.danielbunting.clickhouse.compress.CompressionMethod.LZ4,
                        io.github.danielbunting.clickhouse.compress.CompressionMethod.ZSTD}) {
            io.github.danielbunting.clickhouse.ClickHouseConfig cfg =
                    io.github.danielbunting.clickhouse.ClickHouseConfig.builder()
                            .host(clickHouseHost())
                            .port(clickHousePort())
                            .compression(method)
                            .build();
            try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
                ServerException e = assertThrows(ServerException.class, () -> {
                    try (QueryResult r = conn.query("SELECT no_such_column FROM system.one")) {
                        materialize(r);
                    }
                }, method.toString());
                assertEquals(47, e.code(), "code parsed under " + method);
                assertEquals(1L, conn.executeScalar("SELECT 1"),
                        "connection reusable after compressed error (" + method + ")");
            }
        }
    }

    /**
     * A zero-row result still exposes full column metadata (reference: client-v2
     * QueryTests#testQueryRecordsOnEmptyDataset / #testQueryRecordsWithEmptyResult): the
     * server's header block carries names and types even when no data blocks follow.
     */
    @Test
    void emptyResultStillExposesColumnNamesAndTypes() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config());
             QueryResult result = conn.query(
                     "SELECT number AS n, toString(number) AS s FROM system.numbers WHERE 1 = 0")) {
            assertEquals(List.of("n", "s"), result.columnNames(),
                    "column names present on an empty result");
            assertEquals(List.of("UInt64", "String"), result.columnTypes(),
                    "column types present on an empty result");
            assertEquals(0, materialize(result).size(), "no rows");
        }
    }

    /**
     * The caller's settings map is not mutated by query execution (reference: client-v2
     * QueryTests#testSettingsNotChanged).
     */
    @Test
    void querySettingsMapIsNotMutatedByExecution() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            java.util.Map<String, String> settings = new java.util.HashMap<>();
            settings.put("max_result_rows", "100");
            settings.put("log_comment", "qsem_immutable");
            java.util.Map<String, String> snapshot = new java.util.HashMap<>(settings);

            try (QueryResult r = conn.query("SELECT 1", settings)) {
                materialize(r);
            }
            assertEquals(snapshot, settings, "query() must not mutate the caller's settings map");
        }
    }

    /**
     * {@code QueryResult.summary()} aggregates the server's Progress/ProfileInfo
     * feedback (reference: client-v2 OperationMetrics / QueryTests#testGettingRowsBeforeLimit):
     * read-rows on a full scan, and {@code rows_before_limit} + {@code appliedLimit}
     * under a LIMIT.
     */
    @Test
    void querySummaryReportsReadRowsAndRowsBeforeLimit() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            try (QueryResult r = conn.query("SELECT number FROM numbers(1000000)")) {
                materialize(r);
                io.github.danielbunting.clickhouse.QuerySummary s = r.summary();
                assertEquals(1_000_000L, s.readRows(),
                        "the server reported reading exactly the scanned rows");
                assertTrue(s.readBytes() > 0, "read bytes reported");
            }

            try (QueryResult r = conn.query(
                    "SELECT number FROM numbers(1000) ORDER BY number LIMIT 10")) {
                assertEquals(10, materialize(r).size());
                io.github.danielbunting.clickhouse.QuerySummary s = r.summary();
                assertTrue(s.appliedLimit(), "LIMIT was applied");
                assertEquals(1000L, s.rowsBeforeLimit(),
                        "rows_before_limit reports the pre-LIMIT cardinality");
            }
        }
    }

    /**
     * The configured {@link io.github.danielbunting.clickhouse.ClickHouseConfig#queryTimeout()}
     * is enforced SERVER-side as {@code max_execution_time} (was inert): a query running
     * past it aborts with TIMEOUT_EXCEEDED (159), and an explicit per-query
     * {@code max_execution_time} still wins over the config default.
     */
    @Test
    void configQueryTimeoutIsEnforcedServerSide() {
        io.github.danielbunting.clickhouse.ClickHouseConfig cfg =
                io.github.danielbunting.clickhouse.ClickHouseConfig.builder()
                        .host(clickHouseHost())
                        .port(clickHousePort())
                        .queryTimeout(Duration.ofSeconds(1))
                        .build();
        try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
            ServerException e = assertThrows(ServerException.class, () -> {
                try (QueryResult r = conn.query("SELECT sleep(2)")) {
                    materialize(r);
                }
            });
            assertEquals(159, e.code(), "TIMEOUT_EXCEEDED, got: " + e.getMessage());
            assertEquals(1L, conn.executeScalar("SELECT 1"),
                    "clean server-side abort; connection reusable");

            // A per-query override beats the config default.
            try (QueryResult r = conn.query("SELECT sleep(2)",
                    Map.of("max_execution_time", "10"))) {
                assertEquals(1, materialize(r).size(),
                        "explicit per-query max_execution_time wins over the config timeout");
            }
        }
    }

    /**
     * {@code ping()} (reference: ClickHouseClientTest#testPing): the protocol-level
     * Ping/Pong probe answers true on a live connection, false after close, and never
     * throws.
     */
    @Test
    void pingProbesLivenessWithoutThrowing() {
        ClickHouseConnection conn = ClickHouseConnection.open(config());
        assertTrue(conn.ping(), "live connection answers the Ping probe");
        assertTrue(conn.ping(), "ping is repeatable (the Pong is fully consumed)");
        assertEquals(1L, conn.executeScalar("SELECT 1"),
                "the connection still runs queries after pings");
        conn.close();
        assertFalse(conn.ping(), "a closed connection reports false, no throw");
    }
}
