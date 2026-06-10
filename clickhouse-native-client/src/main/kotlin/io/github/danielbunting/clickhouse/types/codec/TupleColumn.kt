package io.github.danielbunting.clickhouse.types.codec

/**
 * Backing holder for a [TupleColumnCodec]: one inner backing array per tuple
 * element (column-major), plus the row count.
 *
 * `sub[i]` is whatever `elements[i].allocate(rowCount)` returns — a
 * primitive array, an `Object[]`, or another nested backing object. The Tuple
 * codec reads/writes each `sub[i]` as a complete sub-column.
 */
public class TupleColumn internal constructor(
    /** Per-element inner backing arrays, in tuple declaration order. */
    internal val sub: Array<Any>,
    /** Number of rows held in each inner array. */
    internal val rowCount: Int,
)
