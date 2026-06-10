package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.types.Column

/**
 * A typed, row-oriented view over a single row of a column-major [Block].
 *
 * Accessing a value reads straight from the column's backing array — primitive getters
 * ([long], [int], [double]) avoid boxing entirely. A [ResultRow] is a lightweight cursor
 * `(block, rowIndex)`; the heavy column arrays are shared by reference with the [Block] it came
 * from. It is only valid while the originating [Block] is reachable, which the surrounding
 * [Flow][kotlinx.coroutines.flow.Flow] guarantees for the duration of a `collect`.
 */
class ResultRow internal constructor(
    private val block: Block,
    private val rowIndex: Int,
    private val nameToIndex: Map<String, Int>,
) {
    /** Number of columns in this row. */
    val columnCount: Int get() = block.columnCount()

    private fun column(name: String): Column {
        val idx = nameToIndex[name]
            ?: throw IllegalArgumentException("No column named '$name' (have ${nameToIndex.keys})")
        return block.column(idx)
    }

    private fun column(index: Int): Column = block.column(index)

    /** True if the cell is SQL `NULL` (only possible for `Nullable(...)` columns). */
    fun isNull(name: String): Boolean = isNull(column(name))

    fun isNull(index: Int): Boolean = isNull(column(index))

    private fun isNull(col: Column): Boolean = col.isNullable && col.nulls()!![rowIndex]

    /** Reads the cell as a primitive `long` with no boxing. Throws if the cell is `NULL`. */
    fun long(name: String): Long = column(name).longAt(rowIndex)

    fun long(index: Int): Long = column(index).longAt(rowIndex)

    /** Reads the cell as a `Long`, or `null` if the cell is `NULL`. */
    fun longOrNull(name: String): Long? = column(name).let { if (isNull(it)) null else it.longAt(rowIndex) }

    fun longOrNull(index: Int): Long? = column(index).let { if (isNull(it)) null else it.longAt(rowIndex) }

    /** Reads the cell as a primitive `int` (narrowed from the column's `long`). Throws if `NULL`. */
    fun int(name: String): Int = column(name).longAt(rowIndex).toInt()

    fun int(index: Int): Int = column(index).longAt(rowIndex).toInt()

    /** Reads the cell as a primitive `double` with no boxing. Throws if the cell is `NULL`. */
    fun double(name: String): Double = column(name).doubleAt(rowIndex)

    fun double(index: Int): Double = column(index).doubleAt(rowIndex)

    fun doubleOrNull(name: String): Double? = column(name).let { if (isNull(it)) null else it.doubleAt(rowIndex) }

    fun doubleOrNull(index: Int): Double? = column(index).let { if (isNull(it)) null else it.doubleAt(rowIndex) }

    /** Reads the cell as a [String] (lazily decoded for `String` columns), or `null` if `NULL`. */
    fun string(name: String): String? = column(name).stringAt(rowIndex)

    fun string(index: Int): String? = column(index).stringAt(rowIndex)

    /** Reads the cell as its boxed value, honoring the null-map (`null` for a `NULL` cell). */
    operator fun get(name: String): Any? = column(name).value(rowIndex)

    operator fun get(index: Int): Any? = column(index).value(rowIndex)
}
