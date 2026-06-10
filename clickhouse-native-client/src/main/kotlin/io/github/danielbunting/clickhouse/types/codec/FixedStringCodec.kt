package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.nio.charset.StandardCharsets

/**
 * Codec for the ClickHouse `FixedString(N)` type.
 *
 * Wire format: exactly `N` bytes per value, zero-padded on write,
 * trailing NUL bytes stripped on read. The backing array is `byte[][]`,
 * where each element is the raw `N`-byte chunk (pre-stripped on read).
 * [get] returns the value as a [String] (UTF-8, NUL-stripped).
 *
 * Design note: `Object[]` is used as the generic array type because
 * `byte[][]` is itself a reference type and not representable as a
 * Java primitive array.
 */
public class FixedStringCodec(length: Int) : ColumnCodec<Array<Any?>> {

    private val length: Int

    /**
     * Constructs a `FixedStringCodec` for values of exactly `length` bytes.
     *
     * @param length the fixed byte width (`N` in `FixedString(N)`)
     */
    init {
        if (length <= 0) {
            throw IllegalArgumentException("FixedString length must be positive, got: " + length)
        }
        this.length = length
    }

    override fun typeName(): String {
        return "FixedString(" + length + ")"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        for (i in 0 until rowCount) {
            val raw = input.readBytes(length)
            // Strip trailing NUL padding to produce the logical string value. This
            // matches ClickHouse's own semantics (FixedString comparisons treat
            // trailing zeros as insignificant padding). Binary FixedString data with
            // significant trailing zeros should be read via a future getBytes() path.
            var end = length
            while (end > 0 && raw[end - 1].toInt() == 0) {
                end--
            }
            dest[i] = String(raw, 0, end, StandardCharsets.UTF_8)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        val buf = ByteArray(length)
        for (i in 0 until rowCount) {
            val s = src[i] as String?
            val encoded = if (s == null) ByteArray(0) else s.toByteArray(StandardCharsets.UTF_8)
            // Clear the buffer (zero-pad)
            for (j in 0 until length) {
                buf[j] = 0
            }
            val copyLen = Math.min(encoded.size, length)
            System.arraycopy(encoded, 0, buf, 0, copyLen)
            out.writeBytes(buf, 0, length)
        }
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return String::class.java
    }

    /**
     * Returns the fixed byte length `N` for this codec.
     *
     * @return the fixed length in bytes
     */
    public fun fixedLength(): Int {
        return length
    }
}
