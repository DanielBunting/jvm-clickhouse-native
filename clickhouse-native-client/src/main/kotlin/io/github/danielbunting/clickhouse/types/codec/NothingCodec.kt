package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Codec for the ClickHouse `Nothing` type — the bottom type that carries no
 * values.
 *
 * `Nothing` is never a stored column on its own; it surfaces only as the
 * element type of an empty / NULL-only array, e.g. `SELECT []` yields an
 * `Array(Nothing)` column whose flattened value section is empty. Because the
 * flattened element count for such a column is always `0`, this codec's
 * [read]/[write] are no-ops over zero elements and [get]
 * returns `null`.
 *
 * Providing this codec lets [io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec]
 * resolve `Array(Nothing)` (empty arrays) instead of throwing on the unknown
 * inner type.
 *
 * Backing array type: `Object[]` (always length 0 in practice).
 */
public class NothingCodec : ColumnCodec<Array<Any?>> {

    override fun typeName(): String {
        return "Nothing"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        // Nothing carries no values; a Nothing column always has rowCount == 0.
        // Nothing to read.
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        // No values to write.
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return null
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        // No-op: Nothing holds no value.
    }

    override fun javaType(): Class<*> {
        return Any::class.java
    }
}
