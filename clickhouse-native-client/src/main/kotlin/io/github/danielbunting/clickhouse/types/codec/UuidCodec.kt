package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.util.UUID

/**
 * Codec for the ClickHouse `UUID` type.
 *
 * Wire format: 16 bytes per value, split into two 64-bit little-endian words.
 * ClickHouse stores a UUID as two UInt64 halves where the *most-significant*
 * 64 bits are written first, then the least-significant 64 bits. Each 64-bit half
 * is in standard little-endian byte order on the wire.
 *
 * Concretely, for the UUID `aabbccdd-eeff-1122-3344-556677889900`:
 *
 *  - First 8 bytes on wire = `0xAABBCCDDEEFF1122` in LE (most-significant half)
 *  - Next  8 bytes on wire = `0x3344556677889900` in LE (least-significant half)
 *
 * The backing array is `UUID[]`.
 *
 * // VERIFY against CH.Native: confirm that MSB half is written first (before LSB half),
 * // and that each 64-bit half uses standard little-endian UInt64 byte order.
 * // The .NET CH.Native source writes msb then lsb; Java's UUID.getMostSignificantBits()
 * // maps to the first 8 bytes of the standard RFC 4122 representation, which matches.
 */
public class UuidCodec : ColumnCodec<Array<UUID?>> {

    override fun typeName(): String {
        return "UUID"
    }

    override fun allocate(rowCount: Int): Array<UUID?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: Array<UUID?>) {
        for (i in 0 until rowCount) {
            // VERIFY against CH.Native: MSB half is the first UInt64 on the wire.
            val msb = input.readUInt64()
            val lsb = input.readUInt64()
            dest[i] = UUID(msb, lsb)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<UUID?>, rowCount: Int) {
        for (i in 0 until rowCount) {
            var uuid = src[i]
            if (uuid == null) {
                uuid = UUID(0L, 0L)
            }
            // VERIFY against CH.Native: MSB half is written first (before LSB half).
            out.writeUInt64(uuid.mostSignificantBits)
            out.writeUInt64(uuid.leastSignificantBits)
        }
    }

    override fun get(array: Array<UUID?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<UUID?>, row: Int, value: Any?) {
        if (value is UUID) {
            array[row] = value
        } else if (value is String) {
            array[row] = UUID.fromString(value)
        } else if (value == null) {
            array[row] = null
        } else {
            throw IllegalArgumentException("Cannot set UUID from: " + value.javaClass.name)
        }
    }

    override fun javaType(): Class<*> {
        return UUID::class.java
    }
}
