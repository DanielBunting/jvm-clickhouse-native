package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.time.Instant
import java.time.ZoneId

/**
 * Codec for the ClickHouse `DateTime(timezone)` type.
 *
 * Wire format: one unsigned 32-bit integer per value (UInt32), representing the
 * number of seconds since the Unix epoch (1970-01-01T00:00:00Z). Values are stored
 * little-endian.
 *
 * The [ZoneId] is conveyed in the column's type string and/or in the server
 * Hello message. It is stored here to allow callers to reconstruct a
 * [java.time.ZonedDateTime] if desired, but the backing value is always an
 * epoch-second count and the returned Java type is always [Instant] (which is
 * timezone-independent). Callers that need a zoned view can call
 * `instant.atZone(codec.zoneId())`.
 *
 * Backing array: `long[]` (UInt32 → widened to `long` to avoid sign
 * issues near 2^31; also consistent with UInt32Codec).
 *
 * Java type: [Instant].
 */
public class DateTimeCodec
/**
 * Creates a `DateTimeCodec` for the given timezone.
 *
 * @param zoneId the timezone from the server Hello or column type string;
 *               if `null`, defaults to [UTC][ZoneId.of].
 */
public constructor(zoneId: ZoneId?) : ColumnCodec<LongArray> {

    private val zoneId: ZoneId = zoneId ?: ZoneId.of("UTC")

    /**
     * Returns the [ZoneId] associated with this codec, for callers that need
     * to reconstruct a zoned view.
     *
     * @return the zone ID, never `null`
     */
    public fun zoneId(): ZoneId {
        return zoneId
    }

    override fun typeName(): String {
        return "DateTime"
    }

    override fun allocate(rowCount: Int): LongArray {
        return LongArray(rowCount)
    }

    /**
     * Reads `rowCount` UInt32 epoch-second values from the wire into `dest`.
     * Each value is read as an unsigned 32-bit integer (widened to `long`).
     */
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: LongArray) {
        for (i in 0 until rowCount) {
            dest[i] = input.readUInt32()
        }
    }

    /**
     * Writes the first `rowCount` epoch-second values from `src` to the
     * wire. Each value is written as an unsigned 32-bit integer.
     */
    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: LongArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeUInt32(src[i])
        }
    }

    /**
     * Returns the value at `row` as an [Instant] (epoch-second precision).
     */
    override fun get(array: LongArray, row: Int): Any {
        return Instant.ofEpochSecond(array[row])
    }

    /**
     * Sets the value at `row` from an [Instant] (or `null` for 0).
     * Stores only whole seconds; any sub-second component of the `Instant` is
     * truncated (ClickHouse `DateTime` has 1-second resolution).
     *
     * `DateTime` is an unsigned 32-bit epoch-second count
     * (1970-01-01T00:00:00Z..2106-02-07T06:28:15Z); an out-of-range instant is
     * rejected rather than silently wrapped by the UInt32 wire cast (matching the
     * `Date`/`Date32` codecs' range checks).
     */
    override fun set(array: LongArray, row: Int, value: Any?) {
        if (value == null) {
            array[row] = 0L
        } else {
            val instant = value as Instant
            val seconds = instant.epochSecond
            require(seconds in 0..MAX_EPOCH_SECOND) {
                "DateTime value $instant is outside the supported range " +
                    "${Instant.EPOCH}..${Instant.ofEpochSecond(MAX_EPOCH_SECOND)}"
            }
            array[row] = seconds
        }
    }

    /**
     * Returns the stored epoch-second count (the raw `long` value), not a
     * re-derived value. This is `value(row).getEpochSecond()`.
     */
    override fun getLong(array: LongArray, row: Int): Long {
        return array[row]
    }

    /** Stores the epoch-second count directly. */
    override fun setLong(array: LongArray, row: Int, v: Long) {
        array[row] = v
    }

    override fun javaType(): Class<*> {
        return Instant::class.java
    }

    private companion object {
        /** Largest epoch second representable in an unsigned 32-bit `DateTime` (2106-02-07T06:28:15Z). */
        const val MAX_EPOCH_SECOND: Long = 0xFFFF_FFFFL
    }
}
