package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.Duration

/**
 * Column-major codec for the ClickHouse `Time64(precision)` type
 * (experimental; server-gated by `SET enable_time_time64_type = 1`).
 *
 * Wire format: one signed 64-bit integer per value (Int64, little-endian),
 * representing a number of "ticks" within a day where one tick equals
 * `10^(-precision)` seconds — analogous to `DateTime64` but a
 * time-of-day offset rather than an instant since the epoch.
 *
 * Examples:
 *  - `Time64(3)` — milliseconds (10^-3 s per tick)
 *  - `Time64(6)` — microseconds
 *  - `Time64(9)` — nanoseconds
 *
 * Backing array: `long[]` (the raw tick count).
 *
 * Java type: [Duration]. [get] converts ticks into seconds plus a
 * nanosecond remainder; [set] accepts a [Duration], any
 * [Number] (seconds), or an `"HH:MM:SS[.fraction]"` string.
 *
 * **Rounding contract:** sub-tick precision is *truncated toward zero* on
 * [set] (consistent with [DateTime64Codec]), not rounded HALF_UP as in
 * [DecimalCodec].
 */
public class Time64Codec
/**
 * Creates a `Time64Codec` for the given sub-second precision.
 *
 * @param precision sub-second precision in decimal digits [0..9]; a tick is
 *                  `10^(-precision)` seconds.
 */
public constructor(precision: Int) : ColumnCodec<LongArray> {

    private val precision: Int
    private val ticksPerSecond: Long
    private val nanosPerTick: Long

    init {
        if (precision < 0 || precision > 9) {
            throw IllegalArgumentException(
                "Time64 precision must be in [0, 9], got: " + precision
            )
        }
        this.precision = precision
        this.ticksPerSecond = POWERS_OF_10[precision]
        this.nanosPerTick = POWERS_OF_10[9 - precision]
    }

    /** Returns the sub-second precision of this codec [0..9]. */
    public fun precision(): Int {
        return precision
    }

    override fun typeName(): String {
        return "Time64(" + precision + ")"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        input.readInto(dest, rowCount)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        out.writeFrom(src, rowCount)
    }

    override fun get(array: LongArray, row: Int): Any {
        val ticks = array[row]
        val seconds = Math.floorDiv(ticks, ticksPerSecond)
        val remainderTicks = Math.floorMod(ticks, ticksPerSecond)
        val nanos = remainderTicks * nanosPerTick
        return Duration.ofSeconds(seconds, nanos)
    }

    override fun set(array: LongArray, row: Int, value: Any?) {
        array[row] = toTicks(value)
    }

    /** Returns the stored raw tick count. */
    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    /** Stores the raw tick count directly. */
    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return Duration::class.java
    }

    private fun toTicks(value: Any?): Long {
        if (value == null) {
            return 0L
        }
        val d: Duration
        if (value is Duration) {
            d = value
        } else if (value is Number) {
            d = Duration.ofSeconds(value.toLong())
        } else if (value is CharSequence) {
            d = parseHms(value.toString())
        } else {
            throw IllegalArgumentException(
                "Cannot convert " + value.javaClass.name + " to Time64"
            )
        }
        val seconds = d.seconds
        val nano = d.nano.toLong() // [0, 999_999_999]; Duration normalizes nano non-negative
        val ticksFromSeconds = seconds * ticksPerSecond
        val ticksFromNanos = nano / nanosPerTick
        return ticksFromSeconds + ticksFromNanos
    }

    private companion object {
        val POWERS_OF_10: LongArray = longArrayOf(
            1L,             // 10^0
            10L,            // 10^1
            100L,           // 10^2
            1_000L,         // 10^3
            10_000L,        // 10^4
            100_000L,       // 10^5
            1_000_000L,     // 10^6
            10_000_000L,    // 10^7
            100_000_000L,   // 10^8
            1_000_000_000L  // 10^9
        )

        /** Parses `"HH:MM:SS[.fraction]"` (optionally signed) into a [Duration]. */
        fun parseHms(s: String): Duration {
            var t = s.trim()
            val negative = t.startsWith("-")
            if (negative) {
                t = t.substring(1)
            }
            var fraction = ""
            val dot = t.indexOf('.')
            if (dot >= 0) {
                fraction = t.substring(dot + 1)
                t = t.substring(0, dot)
            }
            // Java String.split semantics: trailing empty strings are removed.
            val parts = t.split(":").dropLastWhile { it.isEmpty() }
            if (parts.size != 3) {
                throw IllegalArgumentException("Time string must be HH:MM:SS[.frac], got: " + s)
            }
            val h = parts[0].trim().toLong()
            val m = parts[1].trim().toLong()
            val sec = parts[2].trim().toLong()
            val totalSeconds = h * 3600 + m * 60 + sec
            var nanos = 0L
            if (!fraction.isEmpty()) {
                // Pad/truncate the fractional digits to 9 (nanoseconds).
                val padded = (fraction + "000000000").substring(0, 9)
                nanos = padded.toLong()
            }
            val d = Duration.ofSeconds(totalSeconds, nanos)
            return if (negative) d.negated() else d
        }
    }
}
