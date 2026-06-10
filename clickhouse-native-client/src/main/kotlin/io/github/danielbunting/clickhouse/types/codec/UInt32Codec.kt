package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `UInt32` (unsigned 32-bit integer, range [0, 2^32-1]).
 *
 * Backing array type: `long[]` — widened to avoid sign artifacts, matching the
 * convention that [BinaryReader.readUInt32] returns a `long` in [0, 2^32-1].
 * [get] boxes to [Long]; [set] accepts any [Number].
 *
 * Wire format: 4 bytes little-endian per value (unsigned).
 *
 * @constructor Public no-arg constructor required by the agreed class contract (B1).
 */
public class UInt32Codec : ColumnCodec<LongArray> {

    override fun typeName(): String {
        return "UInt32"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    // Left element-wise (improvement 04): the wire width is 4 bytes but the backing
    // array is long[] (8 bytes/element) to hold the widened [0,2^32-1] value, so
    // there is no same-width bulk transfer (readInto(long[]) would consume 8 bytes
    // per value). Element-wise readUInt32() widens each 4-byte value into the long[].
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        for (i in 0 until rowCount) {
            dest[i] = input.readUInt32()
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeUInt32(src[i])
        }
    }

    override fun get(array: LongArray, row: Int): Any {
        return array[row]
    }

    override fun set(array: LongArray, row: Int, value: Any?) {
        array[row] = IntegerRanges.requireBoxed(value, 0L, 0xFFFFFFFFL, "UInt32")
    }

    /** Backing `long[]` already holds the widened [0, 2^32-1] value. */
    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = IntegerRanges.require(v, 0L, 0xFFFFFFFFL, "UInt32")
    }

    override fun javaType(): Class<*> {
        return Long::class.javaObjectType
    }
}
