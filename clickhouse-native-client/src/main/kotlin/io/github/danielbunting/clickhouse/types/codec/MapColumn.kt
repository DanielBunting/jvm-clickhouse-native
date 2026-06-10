package io.github.danielbunting.clickhouse.types.codec

/**
 * Backing holder for a [MapColumnCodec].
 *
 * On the READ path it holds the per-row cumulative `offsets` (one
 * `UInt64` per row, end-exclusive) and the flattened [entries]
 * `Tuple(K, V)` sub-column of [entryCount] rows; `get(row)` slices
 * `[offsets[row-1], offsets[row])` out of the entries column into a map.
 *
 * On the WRITE path `set(row, map)` stashes each row's [java.util.Map]
 * into [pending]; `write` then computes offsets and flattens the maps into
 * a fresh entries column.
 */
public class MapColumn internal constructor(
    /** Cumulative end-offsets, one per row (read path). */
    internal val offsets: LongArray,
) {

    /** Flattened `Tuple(K, V)` entries (read path); `null` until read. */
    internal var entries: TupleColumn? = null

    /** Number of flattened entries held in [entries]. */
    internal var entryCount: Int = 0

    /** Per-row source maps for the write path; lazily sized. */
    internal var pending: Array<Map<*, *>?>? = null

    /** Ensures the [pending] array can hold index `row`. */
    internal fun ensurePending(row: Int) {
        val p = pending
        if (p == null) {
            pending = arrayOfNulls(Math.max(offsets.size, row + 1))
        } else if (row >= p.size) {
            val grown = arrayOfNulls<Map<*, *>?>(row + 1)
            System.arraycopy(p, 0, grown, 0, p.size)
            pending = grown
        }
    }
}
