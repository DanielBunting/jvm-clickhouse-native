package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import io.github.danielbunting.clickhouse.types.NullMaps
import java.io.IOException

/**
 * Codec for a **nested** `Nullable(T)` — a Nullable that appears inside a
 * container such as `Array(Nullable(T))`, `Map(...)` or `Tuple(...)`.
 *
 * Top-level `Nullable(T)` columns are handled directly by the block layer:
 * `BlockCodec` reads the leading null-map into
 * [io.github.danielbunting.clickhouse.types.Column.nulls] and decodes the values with
 * the inner `T` codec into a primitive array (so non-null columns stay
 * allocation-lean). But when nullability is *nested* there is no block layer to
 * read the inner null-map — the container codec delegates to this wrapper, which reads
 * and writes the `rowCount`-byte null-map that precedes the inner values and
 * materialises an `Object[]` with `null` entries.
 *
 * Backing array: `Object[]` so `null` is representable even when `T`
 * is a primitive type (e.g. `Array(Nullable(Int32))`). On the wire the null-map of
 * `rowCount` bytes (1 = null) precedes the full run of `rowCount` inner
 * values; ClickHouse stores a default value at null positions, which this codec writes
 * (and ignores on read).
 */
public class NullableColumnCodec(inner: ColumnCodec<*>?) : ColumnCodec<Array<Any?>> {

    private val inner: ColumnCodec<*>

    /**
     * @param inner the codec for the non-null inner type `T`; must not be `null`
     */
    init {
        if (inner == null) {
            throw NullPointerException("inner codec must not be null")
        }
        this.inner = inner
    }

    /** Returns the inner (non-null) element codec. */
    public fun inner(): ColumnCodec<*> {
        return inner
    }

    override fun typeName(): String {
        return "Nullable(" + inner.typeName() + ")"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        // Nullable's state prefix is its inner type's state prefix (the null-map is part
        // of the bulk data, not the prefix), so a LowCardinality(Nullable(...)) or
        // Array(Nullable(LowCardinality(...))) version lands in the right place.
        inner.readStatePrefix(`in`)
    }

    @Throws(IOException::class)
    override fun writeStatePrefix(out: BinaryWriter) {
        inner.writeStatePrefix(out)
    }

    @Throws(IOException::class)
    override fun read(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        val nulls = NullMaps.read(`in`, rowCount)
        @Suppress("UNCHECKED_CAST")
        val c = inner as ColumnCodec<Any>
        val innerArray = c.allocate(rowCount)
        c.read(`in`, rowCount, innerArray)
        for (i in 0 until rowCount) {
            dest[i] = if (nulls[i]) null else c.get(innerArray, i)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        val nulls = BooleanArray(rowCount)
        @Suppress("UNCHECKED_CAST")
        val c = inner as ColumnCodec<Any>
        val innerArray = c.allocate(rowCount) // default/zero-filled
        for (i in 0 until rowCount) {
            if (src[i] == null) {
                nulls[i] = true // leave the allocated default at this position
            } else {
                c.set(innerArray, i, src[i])
            }
        }
        NullMaps.write(out, nulls, rowCount)
        c.write(out, innerArray, rowCount)
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return inner.javaType()
    }
}
