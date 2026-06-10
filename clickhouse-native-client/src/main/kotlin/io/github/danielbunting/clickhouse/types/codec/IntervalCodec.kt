package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.Duration
import java.time.Period

/**
 * Column-major codec for all eleven ClickHouse `Interval*` types.
 *
 * **Wire format.** Every interval, regardless of unit, is serialized as a
 * fixed 8-byte little-endian `Int64` count of its unit (default
 * serialization) — identical on the wire to [Int64Codec]. The unit is
 * carried entirely in the type name, never in the payload, so a single codec
 * parameterized by [Unit] handles all eleven variants. Backing array type:
 * `long[]`.
 *
 * **Java mapping.** Units are split into two families:
 *  - **Fixed-length** units (Nanosecond, Microsecond, Millisecond, Second,
 *    Minute, Hour, Day, Week) map to [java.time.Duration]: the stored
 *    count is multiplied by the unit's fixed duration.
 *  - **Calendar** units (Month, Quarter, Year) map to [java.time.Period]:
 *    these have no fixed second-count, so `Period.ofMonths`/`ofYears`
 *    is used (`Quarter` = 3 months).
 *
 * [set] accepts a [Duration], [Period], or any [Number]
 * and reduces it back to the `Int64` unit count.
 *
 * **Storability.** `Interval*` is often described as a transient
 * expression type, but on ClickHouse 25.6 it is storable: a `MergeTree` table
 * with an `IntervalDay` column accepts `INSERT ... toIntervalDay(5)` and
 * the column reads back with `toTypeName` = `IntervalDay`. Both reachable
 * paths — an expression result column (`SELECT toIntervalDay(5)` /
 * `SELECT INTERVAL 5 DAY`) and a stored column — emit the same fixed-width
 * `Int64` unit count, which this codec decodes into the mapped temporal value.
 */
public class IntervalCodec public constructor(unit: Unit?) : ColumnCodec<LongArray> {

    /**
     * The eleven interval units, each carrying its canonical ClickHouse leaf name,
     * whether it is a calendar unit (Period) or a fixed unit (Duration), and the
     * scale used to map the raw count to its Java temporal value.
     */
    public enum class Unit(
        private val typeName: String,
        private val calendar: Boolean
    ) {
        NANOSECOND("IntervalNanosecond", false),
        MICROSECOND("IntervalMicrosecond", false),
        MILLISECOND("IntervalMillisecond", false),
        SECOND("IntervalSecond", false),
        MINUTE("IntervalMinute", false),
        HOUR("IntervalHour", false),
        DAY("IntervalDay", false),
        WEEK("IntervalWeek", false),
        MONTH("IntervalMonth", true),
        QUARTER("IntervalQuarter", true),
        YEAR("IntervalYear", true);

        /** The canonical ClickHouse type name, e.g. `"IntervalDay"`. */
        public fun typeName(): String {
            return typeName
        }

        /** `true` for Month/Quarter/Year (mapped to [Period]). */
        public fun isCalendar(): Boolean {
            return calendar
        }

        /** Builds the mapped Java value (Duration or Period) for a raw unit count. */
        internal fun toJava(count: Long): Any {
            return when (this) {
                NANOSECOND -> Duration.ofNanos(count)
                MICROSECOND -> Duration.ofNanos(Math.multiplyExact(count, 1_000L))
                MILLISECOND -> Duration.ofMillis(count)
                SECOND -> Duration.ofSeconds(count)
                MINUTE -> Duration.ofMinutes(count)
                HOUR -> Duration.ofHours(count)
                DAY -> Duration.ofDays(count)
                WEEK -> Duration.ofDays(Math.multiplyExact(count, 7L))
                MONTH -> Period.ofMonths(Math.toIntExact(count))
                QUARTER -> Period.ofMonths(Math.toIntExact(Math.multiplyExact(count, 3L)))
                YEAR -> Period.ofYears(Math.toIntExact(count))
            }
        }

        /** Reduces a [Duration] back to this unit's raw count. */
        internal fun fromDuration(d: Duration): Long {
            return when (this) {
                NANOSECOND -> d.toNanos()
                MICROSECOND -> d.toNanos() / 1_000L
                MILLISECOND -> d.toMillis()
                SECOND -> d.toSeconds()
                MINUTE -> d.toMinutes()
                HOUR -> d.toHours()
                DAY -> d.toDays()
                WEEK -> d.toDays() / 7L
                else -> throw IllegalArgumentException(
                    "Cannot set a Duration on calendar interval " + this
                )
            }
        }

        /** Reduces a [Period] back to this unit's raw count. */
        internal fun fromPeriod(p: Period): Long {
            return when (this) {
                MONTH -> p.toTotalMonths()
                QUARTER -> p.toTotalMonths() / 3L
                YEAR -> p.years.toLong()
                else -> throw IllegalArgumentException(
                    "Cannot set a Period on fixed-length interval " + this
                )
            }
        }
    }

    private val unit: Unit

    /** Creates a codec for the given interval `unit`. */
    init {
        if (unit == null) {
            throw NullPointerException("unit must not be null")
        }
        this.unit = unit
    }

    /** The interval unit this codec handles. */
    public fun unit(): IntervalCodec.Unit {
        return unit
    }

    override fun typeName(): String {
        return unit.typeName()
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        // Identical wire layout to Int64: bulk 8B little-endian transfer.
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: LongArray, row: Int): Any {
        return unit.toJava(array[row])
    }

    override fun set(array: LongArray, row: Int, value: Any?) {
        array[row] = toCount(value)
    }

    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return if (unit.isCalendar()) Period::class.java else Duration::class.java
    }

    private fun toCount(value: Any?): Long {
        if (value is Duration) {
            return unit.fromDuration(value)
        }
        if (value is Period) {
            return unit.fromPeriod(value)
        }
        if (value is Number) {
            return value.toLong()
        }
        throw IllegalArgumentException(
            "Interval value must be a Duration, Period, or Number, got " +
                (if (value == null) "null" else value.javaClass.name)
        )
    }
}
