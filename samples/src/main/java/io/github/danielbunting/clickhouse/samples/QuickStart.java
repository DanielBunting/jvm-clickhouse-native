package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;

import java.util.List;

/**
 * Minimal end-to-end demo of the clickhouse-native-client library.
 *
 * <p>Steps performed:
 * <ol>
 *   <li>Connect to ClickHouse using {@link ClickHouseEnv}.</li>
 *   <li>Create (or reuse) the table {@code events}.</li>
 *   <li>Bulk-insert five sample rows via {@link BulkInserter}.</li>
 *   <li>Query the rows back and print each one.</li>
 *   <li>Print the row count returned by {@code SELECT count()}.</li>
 * </ol>
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runQuickStart
 * }</pre>
 *
 * <p>Override the target server with environment variables:
 * {@code CH_HOST}, {@code CH_PORT}, {@code CH_DB}, {@code CH_USER},
 * {@code CH_PASSWORD}.
 */
public final class QuickStart {

    /**
     * A record whose component names match the columns of the {@code events} table.
     * The {@link BulkInserter} mapper resolves columns by component name.
     *
     * @param id    row identifier
     * @param name  human-readable label
     * @param score arbitrary floating-point metric
     */
    public record Event(long id, String name, double score) {}

    /** The table targeted by this demo. */
    private static final String TABLE = "events";

    /** Private constructor — utility-style entry point. */
    private QuickStart() {}

    /**
     * Entry point. Connects, writes, reads, and prints results.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        System.out.println("=== clickhouse-native-client QuickStart ===");
        System.out.println("Connecting to ClickHouse at "
                + ClickHouseEnv.config().host() + ":" + ClickHouseEnv.config().port()
                + " (db=" + ClickHouseEnv.config().database() + ")");

        try (ClickHouseConnection conn = ClickHouseEnv.open()) {

            // ----------------------------------------------------------------
            // 1. DDL: ensure the table exists
            // ----------------------------------------------------------------
            System.out.println("\n[1] Creating table '" + TABLE + "' if not exists...");
            conn.execute(
                    "CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                    + "id    UInt64,"
                    + "name  String,"
                    + "score Float64"
                    + ") ENGINE = MergeTree ORDER BY id");
            System.out.println("    Done.");

            // ----------------------------------------------------------------
            // 2. Bulk insert sample rows
            // ----------------------------------------------------------------
            List<Event> rows = List.of(
                    new Event(1L, "alpha",   9.5),
                    new Event(2L, "beta",    7.3),
                    new Event(3L, "gamma",   8.1),
                    new Event(4L, "delta",   6.0),
                    new Event(5L, "epsilon", 10.0)
            );

            System.out.println("\n[2] Bulk-inserting " + rows.size() + " rows...");
            try (BulkInserter<Event> inserter = conn.createBulkInserter(TABLE, Event.class)) {
                inserter.init();
                inserter.addRange(rows);
                inserter.complete();
            }
            System.out.println("    Inserted " + rows.size() + " rows.");

            // ----------------------------------------------------------------
            // 3. Query back and print each row
            // ----------------------------------------------------------------
            System.out.println("\n[3] Querying rows (SELECT id, name, score FROM "
                    + TABLE + " ORDER BY id):");
            System.out.printf("    %-6s %-12s %s%n", "id", "name", "score");
            System.out.println("    " + "-".repeat(30));

            try (QueryResult result = conn.query(
                    "SELECT id, name, score FROM " + TABLE + " ORDER BY id")) {

                var blockIter = result.blocks();
                while (blockIter.hasNext()) {
                    Block block = blockIter.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    for (int row = 0; row < block.rowCount(); row++) {
                        Object idVal    = block.column(0).value(row);
                        Object nameVal  = block.column(1).value(row);
                        Object scoreVal = block.column(2).value(row);
                        System.out.printf("    %-6s %-12s %s%n", idVal, nameVal, scoreVal);
                    }
                }
            }

            // ----------------------------------------------------------------
            // 4. Scalar count
            // ----------------------------------------------------------------
            long count = conn.executeScalar("SELECT count() FROM " + TABLE);
            System.out.println("\n[4] Total rows in '" + TABLE + "': " + count);

        }

        System.out.println("\n=== QuickStart complete ===");
    }
}
