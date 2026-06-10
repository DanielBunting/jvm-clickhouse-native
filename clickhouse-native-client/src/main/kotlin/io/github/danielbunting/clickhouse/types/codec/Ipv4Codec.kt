package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.net.Inet4Address
import java.net.InetAddress
import java.net.UnknownHostException

/**
 * Column-major codec for the ClickHouse `IPv4` type.
 *
 * Wire format: identical to `UInt32` — 4 bytes little-endian per value.
 * The decoded numeric value `V` maps to the dotted-quad `a.b.c.d` by
 * `V = (a<<24) | (b<<16) | (c<<8) | d`, i.e. the most-significant byte of
 * `V` is the first (network/big-endian) octet of the address.
 *
 * Backing array type: `long[]` (matching [UInt32Codec]) so the
 * unsigned 32-bit value is held without sign artifacts.
 *
 * [get] boxes to a [Inet4Address]. [set] accepts an
 * [Inet4Address]/[InetAddress] (its 4-byte address), a [String]
 * dotted-quad/host (resolved via [InetAddress.getByName]), or any
 * [Number] (the raw UInt32 value).
 *
 * Byte order confirmed empirically against a real server: inserting the literal
 * `'192.168.1.1'` (UInt32 = 3232235777 = 0xC0A80101) decodes to bytes
 * `{0xC0, 0xA8, 0x01, 0x01}` = `InetAddress.getByName("192.168.1.1")`.
 */
public class Ipv4Codec : ColumnCodec<LongArray> {

    override fun typeName(): String {
        return "IPv4"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

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
        val v = array[row]
        val addr = byteArrayOf(
            (v ushr 24).toByte(),
            (v ushr 16).toByte(),
            (v ushr 8).toByte(),
            v.toByte()
        )
        try {
            return InetAddress.getByAddress(addr)
        } catch (e: UnknownHostException) {
            // getByAddress only throws for an illegal-length array; 4 bytes is always valid.
            throw IllegalStateException("Unexpected IPv4 decode failure", e)
        }
    }

    override fun set(array: LongArray, row: Int, value: Any?) {
        array[row] = toUInt32(value)
    }

    /** Backing `long[]` already holds the unsigned 32-bit value. */
    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v and 0xFFFFFFFFL
    }

    override fun javaType(): Class<*> {
        return Inet4Address::class.java
    }

    private companion object {

        private fun toUInt32(value: Any?): Long {
            if (value == null) {
                return 0L
            }
            if (value is Inet4Address) {
                return fromBytes(value.address)
            }
            if (value is InetAddress) {
                val b = value.address
                if (b.size != 4) {
                    throw IllegalArgumentException(
                        "IPv4 requires a 4-byte address, got " + b.size + " bytes: " + value
                    )
                }
                return fromBytes(b)
            }
            if (value is String) {
                try {
                    val parsed = InetAddress.getByName(value)
                    val b = parsed.address
                    if (b.size != 4) {
                        throw IllegalArgumentException("Not an IPv4 address: " + value)
                    }
                    return fromBytes(b)
                } catch (e: UnknownHostException) {
                    throw IllegalArgumentException("Cannot parse IPv4 address: " + value, e)
                }
            }
            if (value is Number) {
                return value.toLong() and 0xFFFFFFFFL
            }
            throw IllegalArgumentException("Cannot set IPv4 from: " + value.javaClass.name)
        }

        private fun fromBytes(b: ByteArray): Long {
            return ((b[0].toInt() and 0xFF).toLong() shl 24) or
                ((b[1].toInt() and 0xFF).toLong() shl 16) or
                ((b[2].toInt() and 0xFF).toLong() shl 8) or
                (b[3].toInt() and 0xFF).toLong()
        }
    }
}
