package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Tour of the read side of the native client — the query-shaped half of what
 * CH.Native's {@code Samples.Queries} demonstrates.
 *
 * <p>Steps performed:
 * <ol>
 *   <li>Create and fill a demo table from {@code system.numbers}.</li>
 *   <li>Scalar queries via {@code executeScalar}.</li>
 *   <li>The raw, column-major view: iterating a lazy {@link QueryResult}'s
 *       {@link Block}s — the shape the wire data actually arrives in.</li>
 *   <li>Typed mapping: {@code query(sql, Class)} streaming rows as records.</li>
 *   <li>Server-side {@link QueryParameters} — the injection-safe alternative to
 *       splicing values into SQL text.</li>
 *   <li>{@code queryAsync} for handing the blocking call to a background thread.</li>
 * </ol>
 *
 * <p>Run with:
 * <pre>{@code
 *   ./gradlew :samples:runQueries
 * }</pre>
 *
 * <p>Override the target server with {@code CH_HOST}, {@code CH_PORT}, {@code CH_DB},
 * {@code CH_USER}, {@code CH_PASSWORD}.
 */
public final class Queries {

    /**
     * A record whose component names match the demo table's columns; used by the
     * typed {@code query(sql, Class)} mapping in step 4.
     *
     * @param id    row identifier
     * @param name  generated label
     * @param score generated metric
     */
    public record Measurement(long id, String name, double score) {}

    /** The table targeted by this demo. */
    private static final String TABLE = "queries_demo";

    /** Private constructor — utility-style entry point. */
    private Queries() {}

    /**
     * Entry point. Connects, seeds the table, and walks the query surface.
     *
     * @param args ignored
     */
    public static void main(String[] args) {
        System.out.println("=== clickhouse-native-client Queries tour ===");

        try (ClickHouseConnection conn = ClickHouseEnv.open()) {

            // ----------------------------------------------------------------
            // 1. Seed: a deterministic table built server-side from numbers()
            // ----------------------------------------------------------------
            System.out.println("\n[1] Creating and filling '" + TABLE + "' (1000 rows)...");
            conn.execute("CREATE OR REPLACE TABLE " + TABLE + " ("
                    + "id    UInt64,"
                    + "name  String,"
                    + "score Float64"
                    + ") ENGINE = MergeTree ORDER BY id");
            conn.execute("INSERT INTO " + TABLE
                    + " SELECT number, concat('m-', toString(number)), number / 8"
                    + " FROM numbers(1000)");
            System.out.println("    Done.");

            // ----------------------------------------------------------------
            // 2. Scalars
            // ----------------------------------------------------------------
            long count = conn.executeScalar("SELECT count() FROM " + TABLE);
            long maxId = conn.executeScalar("SELECT max(id) FROM " + TABLE);
            System.out.println("\n[2] Scalars: count=" + count + ", max(id)=" + maxId);

            // ----------------------------------------------------------------
            // 3. The column-major view: blocks, not rows
            // ----------------------------------------------------------------
            // A QueryResult is lazy: blocks are decoded as you iterate, and the
            // connection stays "in use" until the result is drained or closed.
            System.out.println("\n[3] Raw block iteration (LIMIT 5) — data arrives column-major:");
            try (QueryResult result = conn.query(
                    "SELECT id, name, score FROM " + TABLE + " ORDER BY id LIMIT 5")) {
                var blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    System.out.println("    block: " + block.rowCount() + " rows x "
                            + block.columnCount() + " columns "
                            + result.columnNames());
                    for (int row = 0; row < block.rowCount(); row++) {
                        // column(i).value(row) is the boxed bridge; codecs also expose
                        // primitive accessors for hot paths (see docs/data-types.md).
                        System.out.printf("    %-4s %-8s %s%n",
                                block.column(0).value(row),
                                block.column(1).value(row),
                                block.column(2).value(row));
                    }
                }
            }

            // ----------------------------------------------------------------
            // 4. Typed mapping: rows as records
            // ----------------------------------------------------------------
            // query(sql, Class) streams rows mapped by column name -> record
            // component. The Stream is lazy too — close it (or drain it) to
            // release the connection.
            System.out.println("\n[4] Typed mapping to record Measurement (top 3 by score):");
            try (Stream<Measurement> rows = conn.query(
                    "SELECT id, name, score FROM " + TABLE + " ORDER BY score DESC LIMIT 3",
                    Measurement.class)) {
                rows.forEach(m -> System.out.println("    " + m));
            }

            // ----------------------------------------------------------------
            // 5. Server-side query parameters
            // ----------------------------------------------------------------
            // The SQL only ever contains {name:Type} placeholders; values travel
            // separately on the Query packet and the SERVER casts them. No string
            // splicing, no injection, no quoting bugs.
            QueryParameters params = QueryParameters.builder()
                    .bind("minScore", 120.0)
                    .bind("prefix", "m-99")
                    .build();
            long matching = conn.executeScalar(
                    "SELECT count() FROM " + TABLE
                            + " WHERE score >= {minScore:Float64}"
                            + " AND startsWith(name, {prefix:String})",
                    params);
            System.out.println("\n[5] Parameterized count (score >= 120, name like 'm-99%'): "
                    + matching);

            // ----------------------------------------------------------------
            // 6. Async: offload the blocking call
            // ----------------------------------------------------------------
            // queryAsync runs the same blocking query on a background thread and
            // completes a future. NOTE: it does not add concurrency to one
            // connection — don't issue another call until the future completes.
            System.out.println("\n[6] queryAsync:");
            CompletableFuture<Long> asyncCount = conn.queryAsync(
                            "SELECT count() FROM " + TABLE + " WHERE id % 2 = 0")
                    .thenApply(result -> {
                        try (result) {
                            Block block = result.blocks().next();
                            return ((Number) block.column(0).value(0)).longValue();
                        }
                    });
            System.out.println("    even ids: " + asyncCount.join());

        }

        System.out.println("\n=== Queries tour complete ===");
    }
}
