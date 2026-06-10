package io.github.danielbunting.clickhouse.types

import io.github.danielbunting.clickhouse.types.codec.StringColumn
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec

/**
 * One column of a [io.github.danielbunting.clickhouse.protocol.Block]: a name,
 * its raw ClickHouse type string, the resolved [ColumnCodec], the columnar
 * value array, and (only for `Nullable(...)` columns) a parallel null-map.
 *
 * The [values] object is whatever the codec's `allocate()`
 * returns — a primitive array or `Object[]` of length [rowCount].
 *
 * **Contract frozen in W0.2.** Mutable, single-threaded, reused across blocks.
 */
public class Column(private val name: String, private val type: String) {

    private var codec: ColumnCodec<*>? = null
    private var values: Any? = null
    private var nulls: BooleanArray? = null
    private var rowCount: Int = 0

    public fun name(): String {
        return name
    }

    /** Raw ClickHouse type string, e.g. `"Nullable(Array(UInt32))"`. */
    public fun type(): String {
        return type
    }

    public fun codec(): ColumnCodec<*>? {
        return codec
    }

    public fun codec(codec: ColumnCodec<*>?) {
        this.codec = codec
    }

    /** Backing value array (primitive array or `Object[]`); see [ColumnCodec]. */
    public fun values(): Any? {
        return values
    }

    public fun values(values: Any?) {
        this.values = values
    }

    /** Parallel null-map of length [rowCount], or `null` if not nullable. */
    public fun nulls(): BooleanArray? {
        return nulls
    }

    public fun nulls(nulls: BooleanArray?) {
        this.nulls = nulls
    }

    public val isNullable: Boolean
        get() = nulls != null

    public fun rowCount(): Int {
        return rowCount
    }

    public fun rowCount(rowCount: Int) {
        this.rowCount = rowCount
    }

    /**
     * Returns the boxed value at [row], honoring the null-map: `null`
     * if this is a nullable column and the row is null, otherwise the codec's
     * boxed element ([ColumnCodec.get]). Convenience for row-oriented
     * callers (JDBC, mappers) that hold a `Column` with a wildcard codec.
     */
    public fun value(row: Int): Any? {
        val nulls = this.nulls
        if (nulls != null && nulls[row]) {
            return null
        }
        @Suppress("UNCHECKED_CAST")
        val rawCodec = codec as ColumnCodec<Any?> // raw to bridge the wildcard capture on values
        return rawCodec.get(values, row)
    }

    /**
     * Returns the value at [row] as a primitive `long` with no boxing,
     * reading the backing array directly via [ColumnCodec.getLong].
     *
     * Since a `long` cannot represent SQL `NULL`, this method throws
     * if the row is null in a nullable column — callers that may hit nulls must
     * guard with [nulls] / [value] first. The numeric value
     * returned matches what [value] produces (e.g. the day-offset for
     * `Date`, the epoch-second for `DateTime`, the raw bits for
     * `UInt64`).
     *
     * @param row the row index
     * @return the value at [row] as a `long`
     * @throws NullPointerException if the row is null in a nullable column
     */
    public fun longAt(row: Int): Long {
        val nulls = this.nulls
        if (nulls != null && nulls[row]) {
            throw NullPointerException(
                "Cannot read null cell as primitive long at row " + row
                    + " in column '" + name + "'; check nulls()/value() first")
        }
        @Suppress("UNCHECKED_CAST")
        val rawCodec = codec as ColumnCodec<Any?>
        return rawCodec.getLong(values, row)
    }

    /**
     * Returns the value at [row] as a [String], honoring the null-map:
     * `null` if this is a nullable column and the row is null. For a
     * `String` column this routes to the codec's lazy
     * [getString][io.github.danielbunting.clickhouse.types.codec.StringColumnCodec.getString],
     * decoding the cell's UTF-8 bytes on demand rather than forcing
     * materialisation of the whole column. For non-String columns it falls back to
     * [value]`.toString()` (still null-guarded).
     *
     * @param row the row index
     * @return the string value, or `null` for a null cell
     */
    public fun stringAt(row: Int): String? {
        val nulls = this.nulls
        if (nulls != null && nulls[row]) {
            return null
        }
        val codec = this.codec
        if (codec is StringColumnCodec) {
            return codec.getString(values as StringColumn, row)
        }
        @Suppress("UNCHECKED_CAST")
        val rawCodec = codec as ColumnCodec<Any?>
        val v = rawCodec.get(values, row)
        return v?.toString()
    }

    /**
     * Returns the value at [row] as a primitive `double` with no boxing,
     * reading the backing array directly via [ColumnCodec.getDouble].
     *
     * Since a `double` cannot represent SQL `NULL`, this method throws
     * if the row is null in a nullable column — callers that may hit nulls must
     * guard with [nulls] / [value] first.
     *
     * @param row the row index
     * @return the value at [row] as a `double`
     * @throws NullPointerException if the row is null in a nullable column
     */
    public fun doubleAt(row: Int): Double {
        val nulls = this.nulls
        if (nulls != null && nulls[row]) {
            throw NullPointerException(
                "Cannot read null cell as primitive double at row " + row
                    + " in column '" + name + "'; check nulls()/value() first")
        }
        @Suppress("UNCHECKED_CAST")
        val rawCodec = codec as ColumnCodec<Any?>
        return rawCodec.getDouble(values, row)
    }
}
