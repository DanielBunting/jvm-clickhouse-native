package io.github.danielbunting.clickhouse.bench;

/**
 * Canonical row for the wide fixed-width benchmarks: 30 numeric columns
 * (15 × {@code UInt64} + 15 × {@code Float64}), no strings.
 *
 * <p>Wide fixed-width schemas isolate the per-cell costs that the 5-column
 * {@code bench} table dilutes: the server's row→column transpose for row-major
 * wire formats (RowBinary) scales with column count, as does the per-cell
 * dispatch in row-shaped driver APIs, while per-row fixed costs amortize.
 * Component names match the {@code bench_wide} columns (see
 * {@link WideSyntheticData#DDL}).</p>
 */
public record WideRow(
        long l0, long l1, long l2, long l3, long l4,
        long l5, long l6, long l7, long l8, long l9,
        long l10, long l11, long l12, long l13, long l14,
        double d0, double d1, double d2, double d3, double d4,
        double d5, double d6, double d7, double d8, double d9,
        double d10, double d11, double d12, double d13, double d14) {

    /**
     * Positional accessor for the K-th {@code UInt64} column ({@code l<k>}),
     * used by the JDBC benchmarks to bind parameters in a loop.
     *
     * @param k the long-column index, 0–14
     * @return the value of {@code l<k>}
     */
    public long longAt(int k) {
        return switch (k) {
            case 0 -> l0; case 1 -> l1; case 2 -> l2; case 3 -> l3; case 4 -> l4;
            case 5 -> l5; case 6 -> l6; case 7 -> l7; case 8 -> l8; case 9 -> l9;
            case 10 -> l10; case 11 -> l11; case 12 -> l12; case 13 -> l13; case 14 -> l14;
            default -> throw new IndexOutOfBoundsException(k);
        };
    }

    /**
     * Positional accessor for the K-th {@code Float64} column ({@code d<k>}),
     * used by the JDBC benchmarks to bind parameters in a loop.
     *
     * @param k the double-column index, 0–14
     * @return the value of {@code d<k>}
     */
    public double doubleAt(int k) {
        return switch (k) {
            case 0 -> d0; case 1 -> d1; case 2 -> d2; case 3 -> d3; case 4 -> d4;
            case 5 -> d5; case 6 -> d6; case 7 -> d7; case 8 -> d8; case 9 -> d9;
            case 10 -> d10; case 11 -> d11; case 12 -> d12; case 13 -> d13; case 14 -> d14;
            default -> throw new IndexOutOfBoundsException(k);
        };
    }
}
