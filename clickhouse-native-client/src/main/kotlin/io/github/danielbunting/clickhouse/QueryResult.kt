package io.github.danielbunting.clickhouse

import io.github.danielbunting.clickhouse.protocol.Block

/**
 * Lazy result of a SELECT: a forward-only stream of [Block]s plus column
 * metadata. Iterating must not materialize the whole result — streaming 1M rows
 * should hold only a block at a time (the Java analog of CH.Native's lazy mode).
 *
 * **Contract frozen in W0.2.** Implementation is task W2.2.
 */
public interface QueryResult : AutoCloseable {

    public fun columnNames(): List<String>

    /** Raw ClickHouse type strings, positionally aligned with [columnNames]. */
    public fun columnTypes(): List<String>

    /** Lazy, forward-only iterator over result blocks. */
    public fun blocks(): Iterator<Block>

    /**
     * Aggregated server-side execution feedback (read rows/bytes, written rows,
     * `rows_before_limit`), assembled from the Progress/ProfileInfo packets interleaved
     * with the stream. A running snapshot until the result is fully consumed or closed;
     * implementations without wire feedback return [QuerySummary.EMPTY].
     */
    public fun summary(): io.github.danielbunting.clickhouse.QuerySummary {
        return io.github.danielbunting.clickhouse.QuerySummary.EMPTY
    }

    override fun close()
}
