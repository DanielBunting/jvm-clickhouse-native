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
 * element type of an empty or NULL-only array: `SELECT []` yields
 * `Array(Nothing)` (zero elements), and `SELECT [NULL]` yields
 * `Array(Nullable(Nothing))` (N elements, all NULL).
 *
 * On the wire ClickHouse's `SerializationNothing` is **one dummy byte per
 * value** (`serializeBinaryBulk` writes a zero byte per row and
 * `deserializeBinaryBulk` skips them), so [read]/[write] must consume/emit
 * exactly `rowCount` bytes — a mismatch desyncs the whole stream. The bytes
 * carry no information; [get] always returns `null`.
 *
 * Providing this codec lets [io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec]
 * resolve `Array(Nothing)` / `Array(Nullable(Nothing))` instead of throwing
 * on the unknown inner type.
 *
 * Backing array type: `Object[]` (contents ignored).
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
        // One dummy byte per value; content is meaningless.
        if (rowCount > 0) {
            input.readBytes(rowCount)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        // One dummy (zero) byte per value, mirroring SerializationNothing.
        if (rowCount > 0) {
            out.writeBytes(ByteArray(rowCount), 0, rowCount)
        }
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
