package io.github.danielbunting.clickhouse.protocol

import io.github.danielbunting.clickhouse.types.Column

/**
 * A native-protocol data block: an ordered set of columns sharing a row count.
 * The unit of transfer for both SELECT results and INSERT payloads. An
 * **empty** block (no columns / zero rows) terminates an insert stream.
 *
 * **Contract frozen in W0.2.** Read/write is task W1.D3.
 */
public class Block {

    private val columns = ArrayList<Column>()
    private var rowCount = 0

    public fun columns(): List<Column> = columns

    public fun column(index: Int): Column = columns[index]

    public fun addColumn(column: Column) {
        columns.add(column)
    }

    public fun columnCount(): Int = columns.size

    public fun rowCount(): Int = rowCount

    public fun rowCount(rowCount: Int) {
        this.rowCount = rowCount
    }

    /** True when this block carries no data (used to detect the insert terminator). */
    public val isEmpty: Boolean
        get() = columns.isEmpty() || rowCount == 0
}
