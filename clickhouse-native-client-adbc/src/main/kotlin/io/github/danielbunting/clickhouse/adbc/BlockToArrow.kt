package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.types.Column
import io.github.danielbunting.clickhouse.types.codec.DateTime64Codec
import io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec
import io.github.danielbunting.clickhouse.types.codec.NullableColumnCodec
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.Decimal256Vector
import org.apache.arrow.vector.DecimalVector
import org.apache.arrow.vector.DurationVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.IntervalYearVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TimeStampMicroTZVector
import org.apache.arrow.vector.TimeStampMilliTZVector
import org.apache.arrow.vector.TimeStampNanoTZVector
import org.apache.arrow.vector.TimeStampSecTZVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.UInt1Vector
import org.apache.arrow.vector.UInt2Vector
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.UInt8Vector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.TimeUnit
import java.math.BigDecimal
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.Period
import java.util.UUID

/**
 * Read-path bridge: fills a reused [VectorSchemaRoot] from one ClickHouse [Block].
 * One block becomes exactly one Arrow batch, preserving the streaming model.
 *
 * The column's resolved [io.github.danielbunting.clickhouse.types.ColumnCodec] and its
 * parallel null-map ([Column.nulls]) drive validity and value extraction; the matching
 * Arrow [FieldVector] (already shaped by [ClickHouseArrowTypes]) receives the values.
 */
public object BlockToArrow {

    /**
     * Resets each vector in [root], copies the [block]'s columns into them, and sets the
     * row count. Vectors are reused across calls (the caller owns/clears them at close).
     */
    @JvmStatic
    public fun fill(root: VectorSchemaRoot, block: Block) {
        val rows = block.rowCount()
        val vectors = root.fieldVectors
        for (c in vectors.indices) {
            val vector = vectors[c]
            vector.reset()
            writeColumn(vector, block.column(c), rows)
            vector.setValueCount(rows)
        }
        root.setRowCount(rows)
    }

    private fun writeColumn(vector: FieldVector, column: Column, rows: Int) {
        val nulls = column.nulls()
        // Most nullable columns expose validity via the parallel null-map. LowCardinality(Nullable(T))
        // is the exception: its null-ness lives inside the dictionary codec, so derive it from the
        // boxed value instead.
        val nullAt: (Int) -> Boolean = when {
            nulls != null -> { r -> nulls[r] }
            isLowCardinalityNullable(column) -> { r -> column.value(r) == null }
            else -> { _ -> false }
        }
        when (vector) {
            is TinyIntVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is SmallIntVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is IntVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is BigIntVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r)) }
            is UInt1Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is UInt2Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is UInt4Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is UInt8Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r)) }
            is Float4Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.doubleAt(r).toFloat()) }
            is Float8Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.doubleAt(r)) }
            is BitVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, if (column.value(r) as Boolean) 1 else 0) }
            is VarCharVector -> forEach(rows, vector, nullAt) { r ->
                vector.setSafe(r, column.stringAt(r)!!.toByteArray(StandardCharsets.UTF_8))
            }
            is FixedSizeBinaryVector -> forEach(rows, vector, nullAt) { r ->
                vector.setSafe(r, fixedBytesOf(column.value(r)!!, vector.byteWidth))
            }
            is DateDayVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r).toInt()) }
            is TimeStampSecTZVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.longAt(r)) }
            is TimeStampMilliTZVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, scaledTicks(column, r, 3)) }
            is TimeStampMicroTZVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, scaledTicks(column, r, 6)) }
            is TimeStampNanoTZVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, scaledTicks(column, r, 9)) }
            is DecimalVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.value(r) as BigDecimal) }
            is Decimal256Vector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, column.value(r) as BigDecimal) }
            // Time/Time64 and non-calendar Intervals box to Duration; calendar Intervals box to Period.
            is DurationVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, durationToTicks(column.value(r) as Duration, vector.unit)) }
            is IntervalYearVector -> forEach(rows, vector, nullAt) { r -> vector.setSafe(r, Math.toIntExact((column.value(r) as Period).toTotalMonths())) }
            // `Nothing`: the vector carries only nulls, so there is nothing to write.
            is NullVector -> Unit
            // Nested types use the boxed value (List/Map) and recurse. MapVector extends
            // ListVector, so it must be matched first.
            is MapVector -> forEach(rows, vector, nullAt) { r -> writeBoxed(vector, r, column.value(r)) }
            is ListVector -> forEach(rows, vector, nullAt) { r -> writeBoxed(vector, r, column.value(r)) }
            is StructVector -> forEach(rows, vector, nullAt) { r -> writeBoxed(vector, r, column.value(r)) }
            else -> throw UnsupportedOperationException(
                "No read-path conversion for Arrow vector ${vector.javaClass.simpleName} " +
                    "(column '${column.name()}' : ${column.type()})"
            )
        }
    }

    /**
     * Writes one boxed value (as produced by `ColumnCodec.get`) into [vector] at [index],
     * recursing through `List`/`Map` for `Array`/`Tuple`/`Map` children. Scalars are converted
     * from their boxed Java form (e.g. `Instant`, `LocalDate`, `BigDecimal`, `UUID`).
     */
    private fun writeBoxed(vector: FieldVector, index: Int, value: Any?) {
        if (value == null) {
            vector.setNull(index)
            return
        }
        when (vector) {
            is TinyIntVector -> vector.setSafe(index, (value as Number).toInt())
            is SmallIntVector -> vector.setSafe(index, (value as Number).toInt())
            is IntVector -> vector.setSafe(index, (value as Number).toInt())
            is BigIntVector -> vector.setSafe(index, (value as Number).toLong())
            is UInt1Vector -> vector.setSafe(index, (value as Number).toInt())
            is UInt2Vector -> vector.setSafe(index, (value as Number).toInt())
            is UInt4Vector -> vector.setSafe(index, (value as Number).toInt())
            is UInt8Vector -> vector.setSafe(index, (value as Number).toLong())
            is Float4Vector -> vector.setSafe(index, (value as Number).toFloat())
            is Float8Vector -> vector.setSafe(index, (value as Number).toDouble())
            is BitVector -> vector.setSafe(index, if (value as Boolean) 1 else 0)
            is VarCharVector -> vector.setSafe(index, value.toString().toByteArray(StandardCharsets.UTF_8))
            is FixedSizeBinaryVector -> vector.setSafe(index, fixedBytesOf(value, vector.byteWidth))
            is DateDayVector -> vector.setSafe(index, if (value is LocalDate) value.toEpochDay().toInt() else (value as Number).toInt())
            is TimeStampSecTZVector -> vector.setSafe(index, (value as Instant).epochSecond)
            is TimeStampMilliTZVector -> vector.setSafe(index, (value as Instant).toEpochMilli())
            is TimeStampMicroTZVector -> vector.setSafe(index, instantToTicks(value as Instant, 6))
            is TimeStampNanoTZVector -> vector.setSafe(index, instantToTicks(value as Instant, 9))
            is DecimalVector -> vector.setSafe(index, value as BigDecimal)
            is Decimal256Vector -> vector.setSafe(index, value as BigDecimal)
            is DurationVector -> vector.setSafe(index, durationToTicks(value as Duration, vector.unit))
            is IntervalYearVector -> vector.setSafe(index, Math.toIntExact((value as Period).toTotalMonths()))
            is NullVector -> vector.setNull(index)
            is MapVector -> writeMap(vector, index, value as Map<*, *>)
            is ListVector -> writeList(vector, index, value as List<*>)
            is StructVector -> writeStruct(vector, index, value as List<*>)
            else -> throw UnsupportedOperationException(
                "No nested read-path conversion for Arrow vector ${vector.javaClass.simpleName}"
            )
        }
    }

    private fun writeList(vector: ListVector, index: Int, list: List<*>) {
        val offset = vector.startNewValue(index)
        val data = vector.dataVector
        for (j in list.indices) {
            writeBoxed(data, offset + j, list[j])
        }
        vector.endValue(index, list.size)
    }

    private fun writeStruct(vector: StructVector, index: Int, elements: List<*>) {
        vector.setIndexDefined(index)
        for (i in elements.indices) {
            writeBoxed(vector.getChildByOrdinal(i) as FieldVector, index, elements[i])
        }
    }

    private fun writeMap(vector: MapVector, index: Int, map: Map<*, *>) {
        val offset = vector.startNewValue(index)
        val entries = vector.dataVector as StructVector
        val keys = entries.getChildByOrdinal(0) as FieldVector
        val values = entries.getChildByOrdinal(1) as FieldVector
        var j = offset
        for ((k, v) in map) {
            entries.setIndexDefined(j)
            writeBoxed(keys, j, k)
            writeBoxed(values, j, v)
            j++
        }
        vector.endValue(index, map.size)
    }

    private fun fixedBytesOf(value: Any, width: Int): ByteArray = when (value) {
        is UUID -> ByteBuffer.allocate(16).putLong(value.mostSignificantBits).putLong(value.leastSignificantBits).array()
        // An IPv6 column can hold an IPv4-mapped address (e.g. toIPv6('1.2.3.4') = ::ffff:1.2.3.4);
        // the JDK decodes such 16-byte values to an Inet4Address. Re-widen to the 16-byte form.
        is InetAddress -> normalizeAddress(value.address, width)
        is String -> padTo(value.toByteArray(StandardCharsets.UTF_8), width)
        is ByteArray -> padTo(value, width)
        else -> throw UnsupportedOperationException("Cannot convert ${value.javaClass} to FixedSizeBinary($width)")
    }

    /** Widens a 4-byte IPv4 address to its IPv4-mapped IPv6 form when [width] is 16. */
    private fun normalizeAddress(address: ByteArray, width: Int): ByteArray {
        if (address.size == width) {
            return address
        }
        if (address.size == 4 && width == 16) {
            val mapped = ByteArray(16)
            mapped[10] = 0xFF.toByte()
            mapped[11] = 0xFF.toByte()
            System.arraycopy(address, 0, mapped, 12, 4)
            return mapped
        }
        return padTo(address, width)
    }

    /** Converts a [Duration] (a span, possibly &gt;24h) to ticks at the Arrow vector's [TimeUnit]. */
    private fun durationToTicks(d: Duration, unit: TimeUnit): Long = when (unit) {
        TimeUnit.SECOND -> d.seconds
        TimeUnit.MILLISECOND -> d.toMillis()
        TimeUnit.MICROSECOND -> Math.addExact(Math.multiplyExact(d.seconds, POW10[6]), d.nano.toLong() / POW10[3])
        TimeUnit.NANOSECOND -> Math.addExact(Math.multiplyExact(d.seconds, POW10[9]), d.nano.toLong())
    }

    /** Converts an [Instant] to ticks at exponent `unitExponent` (6=micro, 9=nano). */
    private fun instantToTicks(instant: Instant, unitExponent: Int): Long {
        val factor = POW10[unitExponent]
        return Math.addExact(
            Math.multiplyExact(instant.epochSecond, factor),
            instant.nano.toLong() / POW10[9 - unitExponent]
        )
    }

    /** Runs [set] for each non-null row (per [nullAt]), marking null rows on [vector]. */
    private inline fun forEach(rows: Int, vector: FieldVector, nullAt: (Int) -> Boolean, set: (Int) -> Unit) {
        for (r in 0 until rows) {
            if (nullAt(r)) {
                vector.setNull(r)
            } else {
                set(r)
            }
        }
    }

    private fun isLowCardinalityNullable(column: Column): Boolean {
        val codec = column.codec()
        return codec is LowCardinalityColumnCodec && codec.inner() is NullableColumnCodec
    }

    /** UUID/IPv6/FixedString → fixed-width bytes, right-padded with NULs to `width`. */
    private fun padTo(bytes: ByteArray, width: Int): ByteArray =
        if (bytes.size == width) bytes else bytes.copyOf(width)

    /**
     * Scales a DateTime64 tick (`10^-precision` s) to the Arrow vector's exponent
     * (`3`=milli, `6`=micro, `9`=nano). Precision is read from the column codec; the
     * mapping in [ClickHouseArrowTypes] guarantees `unitExponent >= precision`.
     */
    private fun scaledTicks(column: Column, row: Int, unitExponent: Int): Long {
        val raw = column.longAt(row)
        val precision = (column.codec() as? DateTime64Codec)?.precision() ?: unitExponent
        val diff = unitExponent - precision
        return if (diff <= 0) raw else raw * POW10[diff]
    }

    private val POW10 = LongArray(19) { var p = 1L; repeat(it) { p *= 10 }; p }
}
