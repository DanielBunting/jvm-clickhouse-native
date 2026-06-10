package io.github.danielbunting.clickhouse.bench;

import java.time.Instant;

/**
 * The canonical row type used across all ClickHouse client benchmarks.
 *
 * <p>The component names match the columns of the {@code bench} table
 * (see {@link SyntheticData#DDL}) so that the row can be used directly with
 * the native client's record-based bulk inserter, while the same field order
 * drives the competitor JDBC {@code PreparedStatement} binding.</p>
 *
 * <p>Mapping to ClickHouse column types:</p>
 * <ul>
 *   <li>{@code id} &rarr; {@code UInt64}</li>
 *   <li>{@code ts} &rarr; {@code DateTime}</li>
 *   <li>{@code user} &rarr; {@code String}</li>
 *   <li>{@code value} &rarr; {@code Float64}</li>
 *   <li>{@code status} &rarr; {@code UInt8}</li>
 * </ul>
 *
 * @param id     the row identifier, mapped to {@code UInt64}
 * @param ts     the event timestamp, mapped to {@code DateTime} (second precision)
 * @param user   an arbitrary user label, mapped to {@code String}
 * @param value  a numeric payload, mapped to {@code Float64}
 * @param status a small status code (0-4), mapped to {@code UInt8}
 */
public record BenchRow(long id, Instant ts, String user, double value, int status) {
}
