package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration coverage for server packets that {@code NativeClientImpl.readMessage} must
 * consume without desyncing the stream but does not surface to the caller: {@code TOTALS}
 * and {@code EXTREMES} blocks (from {@code WITH TOTALS} / {@code extremes=1}) and interleaved
 * {@code LOG}/{@code PROFILE_EVENTS} telemetry (from {@code send_logs_level}). Previously these
 * paths were parsed-and-ignored with no test.
 *
 * <p>The invariant each test asserts is the robust one: the main result rows are present and
 * correct, and — critically — the <em>same connection</em> is usable for a follow-up query,
 * proving the extra packets were consumed at the right offsets and left the protocol in sync.
 *
 * <p><b>Behaviour note (characterised, not endorsed):</b> {@code QueryResultImpl.pull()}
 * currently yields {@code TOTALS} and {@code EXTREMES} blocks <em>inline</em> as ordinary
 * result rows (QueryResultImpl.kt), rather than exposing them separately the way the official
 * client does via {@code getTotals()}. These tests pin that current behaviour (extra rows are
 * surfaced) so that any future change to separate them out is caught deliberately.
 */
class SpecialPacketsIT extends TypeRoundTripBase {

    private static void seed(io.github.danielbunting.clickhouse.ClickHouseConnection conn, String table) {
        conn.execute("CREATE TABLE " + table + " (k UInt8, v UInt32) ENGINE = Memory");
        conn.execute("INSERT INTO " + table + " VALUES (1, 10), (1, 20), (2, 30)");
    }

    @Test
    void withTotals_mainRowsReturned_totalsBlockConsumed_streamStaysInSync() {
        withTable("totals", (conn, table) -> {
            seed(conn, table);

            // WITH TOTALS makes the server send a separate TOTALS packet after the data blocks.
            List<Object[]> rows = decode(conn,
                    "SELECT k, sum(v) AS s FROM " + table + " GROUP BY k WITH TOTALS ORDER BY k");

            // Two grouped rows, then the totals block surfaced inline as a third row (see note).
            assertEquals(3, rows.size(),
                    "two GROUP BY rows plus the inline TOTALS row");
            assertEquals(30L, ((Number) rows.get(0)[1]).longValue(), "sum for k=1");
            assertEquals(30L, ((Number) rows.get(1)[1]).longValue(), "sum for k=2");
            assertEquals(60L, ((Number) rows.get(2)[1]).longValue(),
                    "the trailing TOTALS row carries the grand total");

            // Reusing the connection proves the TOTALS packet did not desync the stream.
            assertEquals(3L, conn.executeScalar("SELECT count() FROM " + table),
                    "connection remains in sync after a WITH TOTALS query");
        });
    }

    @Test
    void extremes_mainRowsReturned_extremesBlockConsumed_streamStaysInSync() {
        withTable("extremes", (conn, table) -> {
            seed(conn, table);

            // extremes=1 makes the server send an EXTREMES packet (min/max block) after the data.
            List<Object[]> rows;
            try (var result = conn.query(
                    "SELECT v FROM " + table + " ORDER BY v", Map.of("extremes", "1"))) {
                rows = materialize(result);
            }

            // Three data rows, then the extremes block (min row + max row) surfaced inline.
            assertEquals(5, rows.size(),
                    "three data rows plus the inline EXTREMES min/max rows");
            assertEquals(10L, ((Number) rows.get(0)[0]).longValue(), "first data row (min v)");
            assertEquals(30L, ((Number) rows.get(2)[0]).longValue(), "last data row (max v)");
            assertEquals(10L, ((Number) rows.get(3)[0]).longValue(), "extremes min row");
            assertEquals(30L, ((Number) rows.get(4)[0]).longValue(), "extremes max row");

            // Reusing the connection proves the EXTREMES packet did not desync the stream.
            assertEquals(3L, conn.executeScalar("SELECT count() FROM " + table),
                    "connection remains in sync after an extremes=1 query");
        });
    }

    @Test
    void sendLogsLevel_logPacketsSkipped_resultCorrect_streamStaysInSync() {
        withTable("logs", (conn, table) -> {
            seed(conn, table);

            // send_logs_level=trace interleaves LOG (and PROFILE_EVENTS) packets in the response;
            // the client must skip them and stay positioned for the real data + end-of-stream.
            long count;
            try (var result = conn.query("SELECT count() FROM " + table,
                    Map.of("send_logs_level", "trace"))) {
                List<Object[]> rows = materialize(result);
                assertEquals(1, rows.size());
                count = ((Number) rows.get(0)[0]).longValue();
            }
            assertEquals(3L, count, "result is correct despite interleaved LOG packets");

            // Follow-up query on the same connection confirms the log packets left no desync.
            assertEquals(30L,
                    conn.executeScalar("SELECT sum(v) FROM " + table + " WHERE k = 1"),
                    "connection remains in sync after a send_logs_level query");
        });
    }
}
