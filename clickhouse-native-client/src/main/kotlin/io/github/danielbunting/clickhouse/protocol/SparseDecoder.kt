package io.github.danielbunting.clickhouse.protocol

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Decodes a single ClickHouse `Sparse`-serialized column stream into a full
 * **dense** column array, using the column's inner ("nested") [ColumnCodec]
 * for the non-default values.
 *
 * Sparse is a serialization *kind*, not a type: a column whose values are
 * overwhelmingly the type default (`0`, empty `String`, ...) may be stored
 * (and, in principle, transmitted) as a compact stream of the non-default values plus
 * their positions, rather than a dense array. On the Native protocol this is signalled
 * by the per-column `has_custom = 1` flag (revision >= 54454) followed by a
 * serialization-kind preamble of `SPARSE`. See [BlockCodec].
 *
 * ## Wire format (faithful to `SerializationSparse`)
 *
 * Two consecutive substreams:
 * 1. **SparseOffsets**: a sequence of `VarUInt` *group sizes*. Each
 *    non-terminator group size `g` means "`g` default values, then one
 *    non-default value". A final group size with the `END_OF_GRANULE_FLAG`
 *    (`1 << 62`) bit set carries (in its low bits) the count of trailing
 *    default values after the last non-default value and terminates the offsets.
 *    Concretely, ClickHouse serializes:
 *    ```
 *    for each non-default at absolute index off:
 *        writeVarUInt(off - start); start = off + 1;
 *    writeVarUInt((rows > start ? rows - start : 0) | END_OF_GRANULE_FLAG);
 *    ```
 * 2. **SparseElements**: the `N` non-default values, where `N` is the
 *    number of non-terminator group sizes read, serialized densely by the nested
 *    (non-sparse) serialization of the inner type.
 *
 * Reconstruction allocates a dense array of `rowCount` rows, reads the `N`
 * non-default values via the inner codec, and scatters them to their absolute positions;
 * every other slot holds the type default.
 *
 * ## Default fill
 *
 * For primitive-array backings (all integer/float/temporal codecs) the freshly
 * [allocated][ColumnCodec.allocate] dense array is already zero-filled, which is
 * exactly the ClickHouse default — only the non-default slots are written. For reference
 * backings the default is materialised once and written into every default slot:
 * `""` for `String`, otherwise `0` via the codec's boxed
 * [ColumnCodec.set] path. This covers the types for which sparse serialization is
 * realistically chosen (numerics and `String`); other reference types would fall
 * back to a `0`-derived default, which is documented rather than guaranteed.
 *
 * ## Reproducibility note
 *
 * Empirically, ClickHouse 25.6 does **not** emit sparse columns in `SELECT`
 * query results over the Native protocol — the read pipeline materialises sparse on-disk
 * columns to `Default` before sending them to a client (verified by capturing the
 * `Native` output of a confirmed-sparse column: the `has_custom` flag is
 * `0` and the payload is dense). This decoder is therefore exercised by an
 * authoritative unit test against hand-crafted bytes rather than an integration test.
 */
public object SparseDecoder {

    /**
     * High bit of a sparse offsets group-size `VarUInt` marking the granule
     * terminator. ClickHouse: `constexpr auto END_OF_GRANULE_FLAG = 1ULL << 62;`
     */
    private const val END_OF_GRANULE_FLAG = 1L shl 62

    /**
     * Reads a sparse-serialized column (`SparseOffsets` then `SparseElements`)
     * and expands it into a dense array of [rowCount] rows.
     *
     * @param in       reader positioned at the start of the sparse offsets substream
     * @param rowCount the dense row count for this column (from the block header)
     * @param inner    the codec for the column's (non-sparse) inner type
     * @param A        the inner codec's backing array type
     * @return a dense backing array of length [rowCount] with non-default values
     *         placed at their positions and defaults elsewhere
     * @throws IOException       if the underlying source fails
     * @throws ProtocolException if the offsets stream is malformed
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun <A : Any> decode(`in`: BinaryReader, rowCount: Int, inner: ColumnCodec<A>): A {
        val positions = readOffsets(`in`, rowCount)
        val numNonDefault = positions.size

        val dense = inner.allocate(rowCount)
        fillDefaults(inner, dense, rowCount, positions)

        if (numNonDefault > 0) {
            val values = inner.allocate(numNonDefault)
            inner.read(`in`, numNonDefault, values)
            @Suppress("UNCHECKED_CAST")
            val raw = inner as ColumnCodec<Any>
            for (i in 0 until numNonDefault) {
                raw.set(dense, positions[i], raw.get(values, i))
            }
        }
        return dense
    }

    /**
     * Reads the `SparseOffsets` substream and returns the absolute indices of the
     * non-default values, in ascending order.
     *
     * @throws ProtocolException if a group size is out of range, a position exceeds the
     *                           row count, or the terminator's trailing-default count does
     *                           not land exactly on [rowCount]
     */
    @Throws(IOException::class)
    private fun readOffsets(`in`: BinaryReader, rowCount: Int): IntArray {
        var positions = IntArray(minOf(maxOf(rowCount, 1), 16))
        var count = 0
        var position = 0L // absolute index of the next undecided row

        while (true) {
            val group = `in`.readVarUInt()
            val endOfGranule = (group and END_OF_GRANULE_FLAG) != 0L
            val skip = group and END_OF_GRANULE_FLAG.inv()

            if (skip < 0 || skip > rowCount) {
                throw ProtocolException(
                    "Sparse offsets group size $skip out of range for $rowCount rows"
                )
            }

            position += skip // these 'skip' rows keep the default value

            if (endOfGranule) {
                // Trailing defaults consumed; the granule (== this block's column) ends.
                if (position != rowCount.toLong()) {
                    throw ProtocolException(
                        "Sparse offsets terminated at row $position but column has $rowCount rows"
                    )
                }
                break
            }

            // The row at 'position' is a non-default value.
            if (position >= rowCount) {
                throw ProtocolException(
                    "Sparse non-default position $position exceeds row count $rowCount"
                )
            }
            if (count == positions.size) {
                val grown = IntArray(positions.size + (positions.size shr 1) + 1)
                System.arraycopy(positions, 0, grown, 0, count)
                positions = grown
            }
            positions[count++] = position.toInt()
            position++ // advance past the recorded non-default value
        }

        if (count == positions.size) {
            return positions
        }
        val trimmed = IntArray(count)
        System.arraycopy(positions, 0, trimmed, 0, count)
        return trimmed
    }

    /**
     * Fills the default slots of [dense]. For primitive-array backings the array is
     * already zero-filled (the correct default), so this is a no-op; for reference backings
     * every slot that is not a non-default position is set to the type default.
     */
    private fun <A : Any> fillDefaults(
        inner: ColumnCodec<A>,
        dense: A,
        rowCount: Int,
        positions: IntArray,
    ) {
        if (dense.javaClass.isArray && dense.javaClass.componentType.isPrimitive) {
            return // zero-initialised primitive array == default value everywhere
        }
        val def = defaultValue(inner)
        val isNonDefault = BooleanArray(rowCount)
        for (p in positions) {
            isNonDefault[p] = true
        }
        @Suppress("UNCHECKED_CAST")
        val raw = inner as ColumnCodec<Any>
        for (row in 0 until rowCount) {
            if (!isNonDefault[row]) {
                raw.set(dense, row, def)
            }
        }
    }

    /**
     * The default value for a reference-backed inner codec: `""` for `String`,
     * otherwise boxed `0L` (accepted by numeric/temporal `set` paths).
     */
    private fun defaultValue(inner: ColumnCodec<*>): Any {
        if (inner.javaType() == String::class.java) {
            return ""
        }
        return 0L
    }
}
