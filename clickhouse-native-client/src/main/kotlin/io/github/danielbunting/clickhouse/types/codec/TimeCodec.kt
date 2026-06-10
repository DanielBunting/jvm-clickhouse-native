package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.Duration

/**
 * Column-major codec for the ClickHouse `Time` type (experimental;
 * server-gated by `SET enable_time_time64_type = 1`).
 *
 * Wire format: one signed 32-bit integer per value (Int32, little-endian),
 * representing a number of *seconds* within a day. The value is signed and
 * can exceed a single calendar day; ClickHouse's documented range is roughly
 * `[-999:59:59, +999:59:59]` (i.e. `+/- 3_599_999` seconds).
 *
 * Backing array: `int[]` (the raw second count).
 *
 * Java type: [Duration]. [get] returns
 * [Duration.ofSeconds]; [set] accepts a [Duration]
 * ([Duration.toSeconds]), any [Number] (seconds), or an
 * `"HH:MM:SS"` string.
 */
public class TimeCodec
/** Public no-arg constructor required by the agreed class contract (B1). */
public constructor() : ColumnCodec<IntArray> {

    override fun typeName(): String {
        return "Time"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        // Wire width (4B) == int[] element width: bulk little-endian transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: IntArray, row: Int): Any {
        return Duration.ofSeconds(array[row].toLong())
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        array[row] = toSeconds(value)
    }

    /** Stores the raw second count directly. */
    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    /** Stores the raw second count directly. */
    override fun setLong(array: IntArray, row: Int, v: Long) {
        array[row] = v.toInt()
    }

    override fun javaType(): Class<*> {
        return Duration::class.java
    }

    private companion object {

        fun toSeconds(value: Any?): Int {
            if (value == null) {
                return 0
            }
            if (value is Duration) {
                return value.toSeconds().toInt()
            }
            if (value is Number) {
                return value.toInt()
            }
            if (value is CharSequence) {
                return parseHms(value.toString())
            }
            throw IllegalArgumentException(
                "Cannot convert " + value.javaClass.name + " to Time"
            )
        }

        /** Parses `"HH:MM:SS"` (or `"-HH:MM:SS"`) into a signed second count. */
        fun parseHms(s: String): Int {
            var t = s.trim()
            val negative = t.startsWith("-")
            if (negative) {
                t = t.substring(1)
            }
            // Java String.split semantics: trailing empty strings are removed.
            val parts = t.split(":").dropLastWhile { it.isEmpty() }
            if (parts.size != 3) {
                throw IllegalArgumentException("Time string must be HH:MM:SS, got: " + s)
            }
            val h = parts[0].trim().toLong()
            val m = parts[1].trim().toLong()
            val sec = parts[2].trim().toLong()
            val total = h * 3600 + m * 60 + sec
            return (if (negative) -total else total).toInt()
        }
    }
}
