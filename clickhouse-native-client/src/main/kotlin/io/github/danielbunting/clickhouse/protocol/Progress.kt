package io.github.danielbunting.clickhouse.protocol

/**
 * Decoded body of a [ServerPacket.PROGRESS] packet: incremental query
 * progress reported by the server during execution.
 *
 * All counters are cumulative within a single query and are read as
 * `VarUInt` values (returned here as `long`, interpreted unsigned).
 * The `writtenRows`/`writtenBytes` and `elapsedNanos` fields
 * are only present on sufficiently new protocol revisions; when absent they are
 * reported as `0`.
 *
 * @property rows        rows read so far
 * @property bytes       uncompressed bytes read so far
 * @property totalRows   estimated total rows to read (0 if unknown)
 * @property writtenRows rows written so far (INSERT progress; 0 if not reported)
 * @property writtenBytes bytes written so far (INSERT progress; 0 if not reported)
 * @property elapsedNanos elapsed wall-clock time in nanoseconds (0 if not reported)
 */
@JvmRecord
public data class Progress(
    val rows: Long,
    val bytes: Long,
    val totalRows: Long,
    val writtenRows: Long,
    val writtenBytes: Long,
    val elapsedNanos: Long,
)
