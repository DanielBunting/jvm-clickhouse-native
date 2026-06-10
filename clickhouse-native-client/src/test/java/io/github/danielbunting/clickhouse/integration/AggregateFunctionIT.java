package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;

/**
 * Behaviour of a real {@code AggregateFunction(sum, UInt64)} column over the Native
 * protocol on ClickHouse 25.6.
 *
 * <p><b>Empirical finding.</b> The column's Native data is the raw, function-specific
 * intermediate aggregate STATE (for {@code sum(UInt64)} this is the 8-byte accumulator),
 * written with no per-row length prefix; the per-row width is function-specific and is not
 * encoded on the wire. The framing is therefore not self-delimiting and the state cannot
 * be decoded generically (even to an opaque {@code byte[]}). The client recognises the
 * type and fails with a <em>specific</em> message rather than the generic
 * "Unsupported ClickHouse type" catch-all.
 *
 * <p>The supported path is to finalize the aggregate in SQL ({@code -Merge}) and read the
 * resulting plain value type, which this test verifies round-trips correctly.
 *
 * <p>A failed decode leaves the connection's packet stream half-consumed, so (mirroring
 * {@link UnsupportedTypesIT}) the negative test discards that connection and runs cleanup
 * on a fresh one.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class AggregateFunctionIT extends IntegrationTestBase {

    private ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    /**
     * Selecting the raw {@code AggregateFunction} state column throws a specific
     * {@link ClickHouseException} naming the type and pointing at {@code -Merge}.
     */
    @Test
    void rawAggregateStateDecodeThrowsSpecificMessage() {
        String table = "agg_raw_" + System.nanoTime();
        try {
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                conn.execute("CREATE TABLE " + table + " ("
                        + " k UInt32, v AggregateFunction(sum, UInt64)"
                        + ") ENGINE = AggregatingMergeTree ORDER BY k");
                conn.execute("INSERT INTO " + table + " SELECT 1, sumState(toUInt64(5))");
                conn.execute("INSERT INTO " + table + " SELECT 2, sumState(toUInt64(300))");
            }

            // Fresh connection: the failed decode is expected and leaves the stream
            // half-consumed, so the connection is discarded afterwards.
            try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                ClickHouseException ex = assertThrows(ClickHouseException.class, () -> {
                    try (QueryResult r = conn.query("SELECT v FROM " + table + " ORDER BY k")) {
                        r.blocks().forEachRemaining(b -> { });
                    }
                }, "Raw AggregateFunction state must not decode");

                String msg = ex.getMessage();
                assertTrue(
                        msg != null && msg.contains("AggregateFunction")
                                && msg.contains("not supported"),
                        "Message must be the specific AggregateFunction message, was: " + msg);
            }
        } finally {
            dropQuietly(table);
        }
    }

    /**
     * Finalizing the aggregate with {@code sumMerge} yields a plain {@code UInt64} that
     * decodes normally — the documented supported path.
     */
    @Test
    void aggregateFunctionMergeFinalizesToUInt64() {
        String table = "agg_merge_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            try {
                conn.execute("CREATE TABLE " + table + " ("
                        + " k UInt32, v AggregateFunction(sum, UInt64)"
                        + ") ENGINE = AggregatingMergeTree ORDER BY k");
                conn.execute("INSERT INTO " + table + " SELECT 1, sumState(toUInt64(5))");
                conn.execute("INSERT INTO " + table + " SELECT 1, sumState(toUInt64(7))");
                conn.execute("INSERT INTO " + table + " SELECT 2, sumState(toUInt64(300))");

                List<Object[]> rows = decode(conn,
                        "SELECT k, sumMerge(v) AS s FROM " + table + " GROUP BY k ORDER BY k");
                assertEquals(2, rows.size());
                assertEquals(12L, ((Number) rows.get(0)[1]).longValue(),
                        "k=1 merges sumState(5)+sumState(7)=12");
                assertEquals(300L, ((Number) rows.get(1)[1]).longValue(),
                        "k=2 merges to 300");
            } finally {
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    /** Reads all blocks of {@code selectSql} into row-major boxed values. */
    private List<Object[]> decode(ClickHouseConnection conn, String selectSql) {
        List<Object[]> rows = new ArrayList<>();
        try (QueryResult result = conn.query(selectSql)) {
            Iterator<Block> blocks = result.blocks();
            while (blocks.hasNext()) {
                Block block = blocks.next();
                if (block.isEmpty()) {
                    continue;
                }
                int colCount = block.columnCount();
                int rowCount = block.rowCount();
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[colCount];
                    for (int c = 0; c < colCount; c++) {
                        Column col = block.column(c);
                        boolean isNull = col.nulls() != null && col.nulls()[r];
                        row[c] = isNull ? null : col.value(r);
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    /** Drops {@code table} on a fresh connection, ignoring any error. */
    private void dropQuietly(String table) {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("DROP TABLE IF EXISTS " + table);
        } catch (RuntimeException ignored) {
            // best-effort cleanup
        }
    }
}
