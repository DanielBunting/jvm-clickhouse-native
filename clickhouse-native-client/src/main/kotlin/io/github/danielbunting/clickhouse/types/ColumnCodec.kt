package io.github.danielbunting.clickhouse.types

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import java.io.IOException

/**
 * Bidirectional, **column-major** codec between a ClickHouse column and a Java
 * array. This is the single most performance-critical abstraction in the library:
 * a column of `N` rows is read/written as one contiguous array, never
 * value-by-value into boxed objects.
 *
 * The array payload type [A] is a primitive array where possible
 * (`long[]` for UInt32, `int[]` for Int32, `double[]` for
 * Float64, ...) and an `Object[]` only where a reference type is unavoidable
 * (String, BigDecimal, nested Array, ...). Null handling lives one level up in
 * [Column.nulls]; a `Nullable(T)` codec reads the null-map then
 * delegates the values to the inner `T` codec.
 *
 * [get]/[set] bridge the columnar array to row-oriented callers
 * (JDBC `ResultSet`, the object mapper) without forcing them to know the
 * concrete array type.
 *
 * **Contract frozen in W0.2.** Implementations: W1.B1–B6 (leaf codecs),
 * W1.B3 (Nullable wrapper), W1.B5 (Array), composed by W1.B7 (TypeParser).
 *
 * @param A the backing array type, e.g. `long[]`, `int[]`, `String[]`
 */
public interface ColumnCodec<A> {

    /** The canonical ClickHouse type name this codec handles, e.g. `"UInt32"`. */
    public fun typeName(): String

    /** Allocates the backing array for [rowCount] rows. */
    public fun allocate(rowCount: Int): A

    /** Reads [rowCount] values from the wire into [dest]. */
    @Throws(IOException::class)
    public fun read(input: BinaryReader, rowCount: Int, dest: A)

    /** Writes the first [rowCount] values of [src] to the wire. */
    @Throws(IOException::class)
    public fun write(out: BinaryWriter, src: A, rowCount: Int)

    /**
     * Reads this codec's **serialization state prefix** — the per-column metadata that
     * ClickHouse writes *once*, recursively, before any of the column's bulk data
     * (its `serializeBinaryBulkStatePrefix`). The block layer calls this once per
     * column, after the type / custom-serialization flag and before [read].
     *
     * Most codecs have no state prefix and inherit the empty default. The notable
     * exception is `LowCardinality(T)`, whose `KeysSerializationVersion`
     * (`UInt64 = 1`) lives here — at *every* nesting level it appears.
     * Container codecs (`Array`, `Map`, `Tuple`, `Nullable`)
     * override this to recurse into their inner codec(s), so a nested
     * `LowCardinality` contributes its version in the right place.
     *
     * @param in the reader positioned at the start of this column's state prefix
     * @throws IOException if the underlying source fails
     */
    @Throws(IOException::class)
    public fun readStatePrefix(`in`: BinaryReader) {
    }

    /**
     * Writes this codec's **serialization state prefix** — the mirror of
     * [readStatePrefix]. The block layer calls this once per column, after the
     * type / custom-serialization flag and before [write].
     *
     * @param out the writer to emit the state prefix to
     * @throws IOException if the underlying sink fails
     */
    @Throws(IOException::class)
    public fun writeStatePrefix(out: BinaryWriter) {
    }

    /** Returns the value at [row] boxed as its mapped Java type (see [javaType]). */
    public fun get(array: A, row: Int): Any?

    /** Sets the value at [row] from a boxed Java value (inverse of [get]). */
    public fun set(array: A, row: Int, value: Any?)

    /**
     * Reads the value at [row] as a primitive `long`, with no boxing.
     *
     * The default falls back to the boxed [get] path; integer and temporal
     * codecs override this to read their primitive backing array directly. The
     * returned value is the codec's stored numeric: for `Date` this is the
     * day-offset since 1970-01-01, for `DateTime` the epoch-second count, for
     * `DateTime64` the raw tick count — *not* a re-derived value. For
     * `UInt64` the raw 64-bit pattern is returned as-is; callers interpret its
     * unsignedness (e.g. via [Long.toUnsignedString]).
     *
     * @param array the backing array
     * @param row   the row index
     * @return the value at [row] as a `long`
     */
    public fun getLong(array: A, row: Int): Long {
        return (get(array, row) as Number).toLong()
    }

    /**
     * Reads the value at [row] as a primitive `double`, with no boxing.
     *
     * The default falls back to the boxed [get] path; float codecs override
     * this to read their primitive backing array directly (`Float32` widens
     * `float` to `double`).
     *
     * @param array the backing array
     * @param row   the row index
     * @return the value at [row] as a `double`
     */
    public fun getDouble(array: A, row: Int): Double {
        return (get(array, row) as Number).toDouble()
    }

    /**
     * Writes a primitive `long` at [row], with no boxing.
     *
     * The default falls back to the boxed [set] path; integer and temporal
     * codecs override this to write their primitive backing array directly. The value
     * is stored as the codec's raw numeric (day-offset / epoch-second / tick / raw
     * bits) — the inverse of [getLong].
     *
     * @param array the backing array
     * @param row   the row index
     * @param v     the value to store
     */
    public fun setLong(array: A, row: Int, v: Long) {
        set(array, row, v)
    }

    /**
     * Writes a primitive `double` at [row], with no boxing.
     *
     * The default falls back to the boxed [set] path; float codecs override
     * this to write their primitive backing array directly.
     *
     * @param array the backing array
     * @param row   the row index
     * @param v     the value to store
     */
    public fun setDouble(array: A, row: Int, v: Double) {
        set(array, row, v)
    }

    /** The boxed Java element type produced by [get] (e.g. `Long.class`). */
    public fun javaType(): Class<*>
}
