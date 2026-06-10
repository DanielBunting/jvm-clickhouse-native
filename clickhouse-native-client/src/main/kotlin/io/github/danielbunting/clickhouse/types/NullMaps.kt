package io.github.danielbunting.clickhouse.types

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import java.io.IOException

/**
 * Reads and writes the per-row null-map of a `Nullable(T)` column.
 *
 * In the ClickHouse native protocol a `Nullable(T)` column is serialized
 * as a null-map of exactly `rowCount` bytes (one byte per row, `1` = null,
 * `0` = non-null) followed by the inner `T` value array. The null-map is
 * always emitted **before** the inner values.
 *
 * This helper lives at the block layer so that `ColumnCodec` implementations
 * can stay null-agnostic: the block codec (task D3) reads/writes the null-map via this
 * class and then delegates the value array to the inner codec.
 *
 * The `boolean[]` representation uses `true` to mean "this row is null" to mirror
 * the wire convention (`1` = null).
 *
 * **Contract frozen in W0.2.** Implementation is task W1.B3.
 */
public object NullMaps {

    /**
     * Reads a null-map of [rowCount] bytes from [reader].
     *
     * Each byte is interpreted as a flag: any non-zero value marks the row as null
     * (`true`), zero marks it as non-null (`false`). ClickHouse only ever
     * emits `0` or `1`, but non-`1` non-zero bytes are tolerated.
     *
     * @param reader   the source to read from
     * @param rowCount the number of rows (bytes) in the null-map; must be `>= 0`
     * @return a freshly allocated `boolean[rowCount]` where `true` = null
     * @throws IOException              if the underlying source fails
     * @throws IllegalArgumentException if [rowCount] is negative
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun read(reader: BinaryReader, rowCount: Int): BooleanArray {
        if (rowCount < 0) {
            throw IllegalArgumentException("rowCount must be non-negative: " + rowCount)
        }
        val nulls = BooleanArray(rowCount)
        if (rowCount == 0) {
            return nulls
        }
        // Read the whole map in one shot to stay allocation-lean.
        val raw = reader.readBytes(rowCount)
        for (i in 0 until rowCount) {
            nulls[i] = raw[i].toInt() != 0
        }
        return nulls
    }

    /**
     * Writes a null-map of [rowCount] bytes to [writer].
     *
     * Emits `1` for each row flagged null (`nulls[i] == true`) and
     * `0` otherwise.
     *
     * @param writer   the sink to write to
     * @param nulls    the null flags, where `true` = null; must have at least
     *                 [rowCount] elements
     * @param rowCount the number of rows (bytes) to emit; must be `>= 0`
     * @throws IOException              if the underlying sink fails
     * @throws IllegalArgumentException if [rowCount] is negative or [nulls]
     *                                  is shorter than [rowCount]
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun write(writer: BinaryWriter, nulls: BooleanArray, rowCount: Int) {
        if (rowCount < 0) {
            throw IllegalArgumentException("rowCount must be non-negative: " + rowCount)
        }
        if (nulls.size < rowCount) {
            throw IllegalArgumentException(
                "nulls length " + nulls.size + " is smaller than rowCount " + rowCount)
        }
        if (rowCount == 0) {
            return
        }
        // Materialize the byte map once, then emit in a single write.
        val raw = ByteArray(rowCount)
        for (i in 0 until rowCount) {
            raw[i] = if (nulls[i]) 1 else 0
        }
        writer.writeBytes(raw, 0, rowCount)
    }
}
