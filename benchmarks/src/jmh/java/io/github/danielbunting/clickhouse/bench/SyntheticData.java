package io.github.danielbunting.clickhouse.bench;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Deterministic synthetic data generator shared by every benchmark.
 *
 * <p>All field values are derived purely from the row index, so two calls to
 * {@link #generate(int)} with the same argument always produce identical data.
 * This guarantees that the native client and the competitor drivers are timed
 * against exactly the same payload, with no unseeded randomness perturbing the
 * results.</p>
 *
 * <p>This class is a stateless utility and cannot be instantiated.</p>
 */
public final class SyntheticData {

    /**
     * DDL for the benchmark target table. Created once per trial via
     * {@link ClickHouseResource#recreateTable(io.github.danielbunting.clickhouse.ClickHouseConnection)}.
     */
    public static final String DDL =
            "CREATE TABLE IF NOT EXISTS bench (id UInt64, ts DateTime, user String, "
            + "value Float64, status UInt8) ENGINE = MergeTree ORDER BY id";

    /** Name of the benchmark target table. */
    public static final String TABLE = "bench";

    /** Default row count for benchmarks. */
    public static final int ROWS = 1_000_000;

    /**
     * Fixed epoch base from which per-row timestamps are derived. Corresponds to
     * {@code 2024-01-01T00:00:00Z}. Each row adds {@code index} seconds to this base
     * so that timestamps land on whole-second boundaries (matching {@code DateTime}).
     */
    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");

    private SyntheticData() {
        throw new AssertionError("No instances.");
    }

    /**
     * Generates {@code n} deterministic {@link BenchRow} instances.
     *
     * <p>For row index {@code i} (0-based):</p>
     * <ul>
     *   <li>{@code id} = {@code i}</li>
     *   <li>{@code ts} = {@link #BASE} + {@code i} seconds</li>
     *   <li>{@code user} = {@code "user_" + (i % 1000)}</li>
     *   <li>{@code value} = {@code i * 1.5}</li>
     *   <li>{@code status} = {@code i % 5}</li>
     * </ul>
     *
     * @param n the number of rows to generate; must be non-negative
     * @return an immutable-in-practice {@link List} of exactly {@code n} rows
     * @throws IllegalArgumentException if {@code n} is negative
     */
    public static List<BenchRow> generate(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Row count must be non-negative: " + n);
        }
        List<BenchRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rows.add(new BenchRow(
                    i,
                    BASE.plusSeconds(i),
                    "user_" + (i % 1000),
                    i * 1.5,
                    i % 5));
        }
        return rows;
    }
}
