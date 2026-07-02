package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.LocalDate

/**
 * Codec for the ClickHouse `Date` type.
 *
 * Wire format: one unsigned 16-bit integer per value (UInt16), representing the
 * number of days since the Unix epoch (1970-01-01). Values are stored little-endian
 * and fit in the range [0, 65535], corresponding to dates up to ~2149.
 *
 * Backing array: `int[]` (Java has no `short[]` that avoids sign
 * extension without masking; `int[]` keeps [get]/[set] simple
 * and avoids the overhead of masking every access).
 *
 * Java type: [LocalDate].
 */
public class DateCodec
/** Public no-arg constructor required by the codec registry. */
public constructor() : ColumnCodec<IntArray> {

    override fun typeName(): String {
        return "Date"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    /**
     * Reads `rowCount` UInt16 day-offsets from the wire into `dest`.
     * Each value is read as an unsigned 16-bit integer (widened to `int`).
     */
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        for (i in 0 until rowCount) {
            dest[i] = input.readUInt16()
        }
    }

    /**
     * Writes the first `rowCount` day-offsets from `src` to the wire.
     * Each value is written as an unsigned 16-bit integer.
     */
    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeUInt16(src[i])
        }
    }

    /**
     * Returns the value at `row` as a [LocalDate].
     * The stored integer is a count of days since 1970-01-01.
     */
    override fun get(array: IntArray, row: Int): Any {
        return EPOCH.plusDays(array[row].toLong())
    }

    /**
     * Sets the value at `row` from a [LocalDate] (or `null` for 0).
     * Stores the number of days since 1970-01-01 as an `int`.
     */
    override fun set(array: IntArray, row: Int, value: Any?) {
        if (value == null) {
            array[row] = 0
        } else {
            val date = value as LocalDate
            val day = date.toEpochDay()
            // Date is an unsigned 16-bit day count; reject anything that would
            // wrap/truncate on the cast rather than silently corrupting the value.
            require(day in 0..MAX_DAY) {
                "Date value $date is outside the supported range " +
                    "$EPOCH..${EPOCH.plusDays(MAX_DAY)}"
            }
            array[row] = day.toInt()
        }
    }

    /**
     * Returns the stored day-offset since 1970-01-01 (the raw `int` value),
     * not a re-derived value. This is `value(row).toEpochDay()`.
     */
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

    private companion object {
        val EPOCH: LocalDate = LocalDate.of(1970, 1, 1)

        /** Largest day-offset representable in an unsigned 16-bit `Date` (2149-06-06). */
        const val MAX_DAY: Long = 65535L
    }
}
