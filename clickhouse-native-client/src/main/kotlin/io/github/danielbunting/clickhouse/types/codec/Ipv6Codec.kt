package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.net.Inet6Address
import java.net.InetAddress
import java.net.UnknownHostException

/**
 * Column-major codec for the ClickHouse `IPv6` type.
 *
 * Wire format: 16 raw bytes per value, in network (big-endian) order — the
 * same layout as `FixedString(16)`. The bytes are exactly the standard
 * 16-byte representation accepted by [InetAddress.getByAddress].
 *
 * Backing array type: `Object[]`, each slot holding the raw `byte[16]`
 * (a reference type not representable as a Java primitive array). [get]
 * decodes to an [InetAddress] (an [Inet6Address], or an
 * `Inet4Address` for IPv4-mapped forms such as `::ffff:192.168.0.1`,
 * matching [InetAddress]'s own equality semantics).
 *
 * [set] accepts an [InetAddress] (its address bytes, padded/validated
 * to 16), a [String] host/literal (resolved via
 * [InetAddress.getByName]), or a raw `byte[16]`.
 *
 * Byte order confirmed empirically against a real server: inserting the literals
 * `'::1'`, `'2001:db8::1'` and `'::ffff:192.168.0.1'` decodes back
 * to addresses equal to `InetAddress.getByName(...)` of the same literal.
 */
public class Ipv6Codec : ColumnCodec<Array<Any?>> {

    override fun typeName(): String {
        return "IPv6"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        for (i in 0 until rowCount) {
            dest[i] = input.readBytes(WIDTH)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        for (i in 0 until rowCount) {
            val b = toBytes(src[i])
            out.writeBytes(b, 0, WIDTH)
        }
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        val raw = array[row] ?: return null
        val b = raw as ByteArray
        try {
            return InetAddress.getByAddress(b)
        } catch (e: UnknownHostException) {
            // getByAddress only throws for an illegal-length array; 16 bytes is always valid.
            throw IllegalStateException("Unexpected IPv6 decode failure", e)
        }
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = if (value == null) null else toBytes(value)
    }

    override fun javaType(): Class<*> {
        return Inet6Address::class.java
    }

    private companion object {

        private const val WIDTH = 16

        private fun toBytes(value: Any?): ByteArray {
            if (value == null) {
                return ByteArray(WIDTH)
            }
            if (value is ByteArray) {
                if (value.size != WIDTH) {
                    throw IllegalArgumentException(
                        "IPv6 requires a 16-byte address, got " + value.size + " bytes"
                    )
                }
                return value
            }
            if (value is InetAddress) {
                return normalize(value.address)
            }
            if (value is String) {
                try {
                    return normalize(InetAddress.getByName(value).address)
                } catch (e: UnknownHostException) {
                    throw IllegalArgumentException("Cannot parse IPv6 address: " + value, e)
                }
            }
            throw IllegalArgumentException("Cannot set IPv6 from: " + value.javaClass.name)
        }

        /**
         * Normalizes an address to 16 bytes: a 16-byte address is returned as-is; a
         * 4-byte (IPv4) address is mapped to its IPv4-mapped IPv6 form
         * (::ffff:a.b.c.d), matching how ClickHouse stores IPv4 values in an IPv6 column.
         */
        private fun normalize(addr: ByteArray): ByteArray {
            if (addr.size == WIDTH) {
                return addr
            }
            if (addr.size == 4) {
                val mapped = ByteArray(WIDTH)
                mapped[10] = 0xFF.toByte()
                mapped[11] = 0xFF.toByte()
                System.arraycopy(addr, 0, mapped, 12, 4)
                return mapped
            }
            throw IllegalArgumentException("Unsupported address length for IPv6: " + addr.size)
        }
    }
}
