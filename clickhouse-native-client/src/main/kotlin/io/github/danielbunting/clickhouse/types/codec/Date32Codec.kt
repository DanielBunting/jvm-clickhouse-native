package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.LocalDate

/**
 * Codec for the ClickHouse `Date32` type.
 *
 * Wire format: one signed 32-bit integer per value (Int32, little-endian),
 * representing the number of days since the Unix epoch (1970-01-01). The value
 * is *signed*, so dates before 1970 are negative. ClickHouse's supported
 * display range for `Date32` is 1900-01-01..2299-12-31.
 *
 * Backing array: `int[]` (4 bytes per row, matching the wire width and
 * allowing a bulk little-endian transfer).
 *
 * Java type: [LocalDate].
 */
public class Date32Codec
/** Public no-arg constructor required by the codec registry. */
public constructor() : ColumnCodec<IntArray> {

    override fun typeName(): String {
        return "Date32"
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
        return LocalDate.ofEpochDay(array[row].toLong())
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        if (value == null) {
            array[row] = 0
        } else if (value is LocalDate) {
            array[row] = value.toEpochDay().toInt()
        } else if (value is Number) {
            array[row] = value.toInt()
        } else if (value is String) {
            array[row] = LocalDate.parse(value.trim()).toEpochDay().toInt()
        } else {
            throw IllegalArgumentException(
                "Cannot coerce " + value.javaClass.name + " to Date32"
            )
        }
    }

    /** Returns the stored day-offset since 1970-01-01 (the raw signed `int`). */
    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    /** Stores the day-offset since 1970-01-01 directly. */
    override fun setLong(array: IntArray, row: Int, v: Long) {
        array[row] = v.toInt()
    }

    override fun javaType(): Class<*> {
        return LocalDate::class.java
    }
}
