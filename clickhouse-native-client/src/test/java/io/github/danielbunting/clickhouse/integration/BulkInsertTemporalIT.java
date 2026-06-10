package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import io.github.danielbunting.clickhouse.types.Column;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Regression test for bulk-inserting records whose components are {@code java.time}
 * types ({@link Instant} for {@code DateTime}, {@link LocalDate} for {@code Date}).
 *
 * <p>The reflection mapper's write path must hand the raw Java value to the column
 * codec (which performs the Java&rarr;wire conversion); an earlier version converted
 * {@code Instant}&rarr;epoch {@code Long} in the mapper and the codec then threw
 * {@code ClassCastException}. No prior bulk-insert test used a temporal field, so the
 * gap escaped until the Wikimedia sample hit it.
 */
@Tag("integration")
class BulkInsertTemporalIT extends IntegrationTestBase {

    record TemporalRow(long id, Instant dt, LocalDate d) {
    }

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    @Test
    void bulkInsertTemporalRoundTrips() {
        String table = "bulk_temporal_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute("CREATE TABLE IF NOT EXISTS " + table
                    + " (id UInt64, dt DateTime('UTC'), d Date) ENGINE = MergeTree ORDER BY id");

            Instant dt = Instant.parse("2024-03-15T12:30:00Z");
            LocalDate d = LocalDate.of(2024, 3, 15);
            List<TemporalRow> rows = List.of(
                    new TemporalRow(1L, dt, d),
                    new TemporalRow(2L, dt.plusSeconds(60), d.plusDays(1)));

            try (BulkInserter<TemporalRow> ins = conn.createBulkInserter(table, TemporalRow.class)) {
                ins.init();
                ins.addRange(rows);
                ins.complete();
            }

            List<Object[]> out = new ArrayList<>();
            try (QueryResult r = conn.query("SELECT id, dt, d FROM " + table + " ORDER BY id")) {
                Iterator<Block> blocks = r.blocks();
                while (blocks.hasNext()) {
                    Block b = blocks.next();
                    if (b.isEmpty()) {
                        continue;
                    }
                    for (int row = 0; row < b.rowCount(); row++) {
                        out.add(new Object[] {
                                b.column(0).value(row), b.column(1).value(row), b.column(2).value(row)
                        });
                    }
                }
            }

            assertEquals(2, out.size(), "expected 2 rows");
            assertEquals(dt, out.get(0)[1], "DateTime bulk round-trip");
            assertEquals(d, out.get(0)[2], "Date bulk round-trip");
            assertEquals(dt.plusSeconds(60), out.get(1)[1], "DateTime row 2");
            assertEquals(d.plusDays(1), out.get(1)[2], "Date row 2");

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
