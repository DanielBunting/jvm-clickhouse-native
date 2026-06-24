package io.github.danielbunting.clickhouse.bench;

import java.util.ArrayList;
import java.util.List;

/**
 * Deterministic generator for the wide fixed-width benchmark table
 * ({@code bench_wide}: 15 × {@code UInt64} + 15 × {@code Float64}).
 *
 * <p>Like {@link SyntheticData}, all values derive purely from the row index so
 * every driver is timed against an identical payload. Raw row width is
 * 240 bytes (30 × 8), ~7.5× the {@code bench} table, so default row counts
 * produce proportionally larger transfers.</p>
 */
public final class WideSyntheticData {

    /** Name of the wide benchmark target table. */
    public static final String TABLE = "bench_wide";

    /** DDL for the wide benchmark table: l0..l14 UInt64, d0..d14 Float64. */
    public static final String DDL = buildDdl();

    private WideSyntheticData() {
        throw new AssertionError("No instances.");
    }

    private static String buildDdl() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + TABLE + " (");
        for (int c = 0; c < 15; c++) {
            sb.append("l").append(c).append(" UInt64, ");
        }
        for (int c = 0; c < 15; c++) {
            sb.append("d").append(c).append(" Float64");
            if (c < 14) {
                sb.append(", ");
            }
        }
        return sb.append(") ENGINE = MergeTree ORDER BY l0").toString();
    }

    /**
     * Generates {@code n} deterministic {@link WideRow} instances. For row index
     * {@code i}: {@code lK = i + K} and {@code dK = (i + K) * 1.5}.
     *
     * @param n the number of rows to generate; must be non-negative
     * @return a list of exactly {@code n} rows
     * @throws IllegalArgumentException if {@code n} is negative
     */
    public static List<WideRow> generate(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Row count must be non-negative: " + n);
        }
        List<WideRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rows.add(new WideRow(
                    i, i + 1, i + 2, i + 3, i + 4,
                    i + 5, i + 6, i + 7, i + 8, i + 9,
                    i + 10, i + 11, i + 12, i + 13, i + 14,
                    i * 1.5, (i + 1) * 1.5, (i + 2) * 1.5, (i + 3) * 1.5, (i + 4) * 1.5,
                    (i + 5) * 1.5, (i + 6) * 1.5, (i + 7) * 1.5, (i + 8) * 1.5, (i + 9) * 1.5,
                    (i + 10) * 1.5, (i + 11) * 1.5, (i + 12) * 1.5, (i + 13) * 1.5, (i + 14) * 1.5));
        }
        return rows;
    }
}
