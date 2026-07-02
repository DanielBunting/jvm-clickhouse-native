package io.github.danielbunting.clickhouse

/**
 * Aggregated server-side execution feedback for one query, assembled from the
 * `Progress` and `ProfileInfo` packets the server interleaves with the result stream.
 *
 * Progress counters are SUMMED across the query's progress packets (the server reports
 * increments); profile fields come from the final `ProfileInfo` packet. All fields are
 * `0`/`false` until the corresponding packets arrive, so a summary observed mid-stream is
 * a running snapshot — read it after the result is fully consumed (or closed) for final
 * numbers.
 *
 * @property readRows        rows read by the server so far
 * @property readBytes       uncompressed bytes read so far
 * @property totalRowsToRead server's estimate of total rows to read (0 if unknown)
 * @property writtenRows     rows written (INSERT/materialization progress; 0 otherwise)
 * @property writtenBytes    bytes written (0 if none reported)
 * @property elapsedNanos    server-reported elapsed wall-clock nanoseconds (0 if not reported)
 * @property appliedLimit    whether a LIMIT was applied to the result
 * @property rowsBeforeLimit rows the result had before the LIMIT was applied (meaningful
 *                           when [appliedLimit]; 0 otherwise)
 */
@JvmRecord
public data class QuerySummary(
    val readRows: Long,
    val readBytes: Long,
    val totalRowsToRead: Long,
    val writtenRows: Long,
    val writtenBytes: Long,
    val elapsedNanos: Long,
    val appliedLimit: Boolean,
    val rowsBeforeLimit: Long,
) {
    public companion object {
        /** The all-zero summary (no progress/profile packets observed yet). */
        @JvmField
        public val EMPTY: QuerySummary =
            QuerySummary(0L, 0L, 0L, 0L, 0L, 0L, false, 0L)
    }
}
