package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.Instant
import java.time.ZoneId

/**
 * Codec for the ClickHouse `DateTime64(precision[, timezone])` type.
 *
 * Wire format: one signed 64-bit integer per value (Int64), representing the
 * number of "ticks" since the Unix epoch where one tick equals
 * `10^(-precision)` seconds. Values are stored little-endian.
 *
 * Examples:
 *  - `DateTime64(0)` — whole seconds, same resolution as `DateTime`
 *  - `DateTime64(3)` — milliseconds (10^-3 s per tick)
 *  - `DateTime64(6)` — microseconds
 *  - `DateTime64(9)` — nanoseconds
 *
 * Conversion to/from [Instant]: an `Instant` is split into
 * [Instant.getEpochSecond] and [Instant.getNano], then recombined
 * into the tick count. The division is performed with
 * `Math.floorDiv`/`Math.floorMod` so that negative timestamps
 * (pre-epoch) are handled correctly.
 *
 * **Rounding contract:** on [set], sub-tick nanoseconds are
 * *truncated toward zero*, not rounded — a `DateTime64(3)` given
 * `…001_500_000ns` stores 1 ms, not 2. This is a deliberate, documented choice
 * (a timestamp should not silently advance to a later instant) and differs from
 * [DecimalCodec], which rounds [java.math.RoundingMode.HALF_UP]. Because
 * [Instant.getNano] is always non-negative, the truncation is exact even for
 * pre-epoch instants, and [get] inverts it with `floorDiv`/`floorMod`.
 *
 * Precision must be in the range [0, 9]; ClickHouse supports up to nanosecond
 * resolution. Values outside this range are accepted but may produce incorrect
 * results.
 *
 * Backing array: `long[]`.
 *
 * Java type: [Instant].
 */
public class DateTime64Codec
/**
 * Creates a `DateTime64Codec` for the given precision and timezone.
 *
 * @param precision the sub-second precision in decimal digits [0..9];
 *                  a tick is `10^(-precision)` seconds.
 * @param zoneId    the timezone from the server Hello or column type string;
 *                  if `null`, defaults to `UTC`.
 */
public constructor(precision: Int, zoneId: ZoneId?) : ColumnCodec<LongArray> {

    private val precision: Int
    private val zoneId: ZoneId

    /**
     * Ticks per second (10^precision). Precomputed for performance.
     * For precision == 0 this is 1, for precision == 9 this is 10^9.
     */
    private val ticksPerSecond: Long

    /**
     * Nanoseconds per tick. For precision <= 9, this is `10^(9-precision)`.
     * Used when converting ticks → Instant.
     */
    private val nanosPerTick: Long

    init {
        if (precision < 0 || precision > 9) {
            throw IllegalArgumentException(
                "DateTime64 precision must be in [0, 9], got: " + precision
            )
        }
        this.precision = precision
        this.zoneId = zoneId ?: ZoneId.of("UTC")
        this.ticksPerSecond = POWERS_OF_10[precision]
        this.nanosPerTick = POWERS_OF_10[9 - precision]
    }

    /**
     * Returns the sub-second precision of this codec.
     *
     * @return precision in decimal digits [0..9]
     */
    public fun precision(): Int {
        return precision
    }

    /**
     * Returns the [ZoneId] associated with this codec.
     *
     * @return the zone ID, never `null`
     */
    public fun zoneId(): ZoneId {
        return zoneId
    }

    override fun typeName(): String {
        return "DateTime64"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    /**
     * Reads `rowCount` Int64 tick values from the wire into `dest`.
     */
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        for (i in 0 until rowCount) {
            dest[i] = input.readInt64()
        }
    }

    /**
     * Writes the first `rowCount` tick values from `src` to the wire.
     */
    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeInt64(src[i])
        }
    }

    /**
     * Returns the value at `row` as an [Instant].
     *
     * Converts tick count to an `Instant` using floor division so that
     * pre-epoch (negative) tick values produce a consistent result.
     */
    override fun get(array: LongArray, row: Int): Any {
        val ticks = array[row]
        // floor division: for negative ticks this gives the correct epoch-second
        val epochSecond = Math.floorDiv(ticks, ticksPerSecond)
        // remainder is always in [0, ticksPerSecond)
        val remainderTicks = Math.floorMod(ticks, ticksPerSecond)
        val nanoAdjust = remainderTicks * nanosPerTick
        return Instant.ofEpochSecond(epochSecond, nanoAdjust)
    }

    /**
     * Sets the value at `row` from an [Instant] (or `null` for 0).
     *
     * Precision loss: nanoseconds are truncated to the codec's precision.
     * For example a `DateTime64(3)` codec stores milliseconds; nanoseconds
     * beyond that resolution are dropped.
     *
     * Pre-epoch `Instant` values are handled correctly via floor division.
     */
    override fun set(array: LongArray, row: Int, value: Any?) {
        if (value == null) {
            array[row] = 0L
            return
        }
        val instant = value as Instant
        val epochSecond = instant.epochSecond
        val nano = instant.nano // always in [0, 999_999_999]

        try {
            // Convert epoch-second component to ticks. At high precision the Int64 tick
            // count overflows for far-away instants (±292 years at precision 9); reject
            // those rather than silently wrapping the wire value.
            val ticksFromSeconds = Math.multiplyExact(epochSecond, ticksPerSecond)
            // Convert nanosecond component to ticks (truncating toward zero is correct
            // here because nano is always non-negative — Instant guarantees this)
            val ticksFromNanos = nano / nanosPerTick
            array[row] = Math.addExact(ticksFromSeconds, ticksFromNanos)
        } catch (e: ArithmeticException) {
            throw IllegalArgumentException(
                "DateTime64($precision) value $instant is outside the representable range: " +
                    "the Int64 tick count (one tick = 10^-$precision s) overflows", e)
        }
    }

    /**
     * Returns the stored raw tick count (the `long` value on the wire), not a
     * re-derived value. A tick is `10^(-precision)` seconds.
     */
    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    /** Stores the raw tick count directly. */
    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return Instant::class.java
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

        /** Nanoseconds per second. */
        @Suppress("unused")
        const val NANOS_PER_SECOND: Long = 1_000_000_000L
    }
}
