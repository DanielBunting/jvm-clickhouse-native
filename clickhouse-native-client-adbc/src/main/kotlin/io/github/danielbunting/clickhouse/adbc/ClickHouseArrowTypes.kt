package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.types.ColumnCodec
import io.github.danielbunting.clickhouse.types.DefaultTypeParser
import io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec
import io.github.danielbunting.clickhouse.types.codec.BFloat16Codec
import io.github.danielbunting.clickhouse.types.codec.BoolCodec
import io.github.danielbunting.clickhouse.types.codec.Date32Codec
import io.github.danielbunting.clickhouse.types.codec.DateCodec
import io.github.danielbunting.clickhouse.types.codec.DateTime64Codec
import io.github.danielbunting.clickhouse.types.codec.DateTimeCodec
import io.github.danielbunting.clickhouse.types.codec.DecimalCodec
import io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec
import io.github.danielbunting.clickhouse.types.codec.Enum16Codec
import io.github.danielbunting.clickhouse.types.codec.Enum8Codec
import io.github.danielbunting.clickhouse.types.codec.FixedStringCodec
import io.github.danielbunting.clickhouse.types.codec.Float32Codec
import io.github.danielbunting.clickhouse.types.codec.Float64Codec
import io.github.danielbunting.clickhouse.types.codec.Int128Codec
import io.github.danielbunting.clickhouse.types.codec.Int16Codec
import io.github.danielbunting.clickhouse.types.codec.Int256Codec
import io.github.danielbunting.clickhouse.types.codec.Int32Codec
import io.github.danielbunting.clickhouse.types.codec.Int64Codec
import io.github.danielbunting.clickhouse.types.codec.Int8Codec
import io.github.danielbunting.clickhouse.types.codec.IntervalCodec
import io.github.danielbunting.clickhouse.types.codec.Ipv4Codec
import io.github.danielbunting.clickhouse.types.codec.Ipv6Codec
import io.github.danielbunting.clickhouse.types.codec.JsonColumnCodec
import io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec
import io.github.danielbunting.clickhouse.types.codec.MapColumnCodec
import io.github.danielbunting.clickhouse.types.codec.NothingCodec
import io.github.danielbunting.clickhouse.types.codec.NullableColumnCodec
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec
import io.github.danielbunting.clickhouse.types.codec.Time64Codec
import io.github.danielbunting.clickhouse.types.codec.TimeCodec
import io.github.danielbunting.clickhouse.types.codec.TupleColumnCodec
import io.github.danielbunting.clickhouse.types.codec.UInt128Codec
import io.github.danielbunting.clickhouse.types.codec.UInt16Codec
import io.github.danielbunting.clickhouse.types.codec.UInt256Codec
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec
import io.github.danielbunting.clickhouse.types.codec.UInt8Codec
import io.github.danielbunting.clickhouse.types.codec.UuidCodec
import io.github.danielbunting.clickhouse.types.codec.VariantColumnCodec
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

/**
 * Maps ClickHouse type strings to Arrow [Field]/[Schema], reusing the core
 * [DefaultTypeParser] / [ColumnCodec] tree as the single source of type structure
 * rather than re-parsing type strings here.
 *
 * Representation choices (locked and asserted by `BlockToArrowTest`):
 * - `UInt8/16/32/64` → Arrow unsigned `Int` of the matching width; `UInt64` carries
 *   raw bits (an Arrow unsigned 64-bit value).
 * - `UUID` → `FixedSizeBinary(16)` (big-endian: most-significant then least-significant long).
 * - `IPv4` → unsigned `Int(32)`; `IPv6` → `FixedSizeBinary(16)`.
 * - `Enum8/16` → signed `Int(8/16)` (the underlying numeric value, not the label).
 * - `LowCardinality(T)` → the unwrapped `T` (dictionary encoding is a later optimisation).
 * - `FixedString(N)` → `FixedSizeBinary(N)`; the bytes are right-padded with NULs to `N`.
 * - `DateTime`/`DateTime64(p)` → `Timestamp` carrying the column timezone (UTC when absent);
 *   the time unit is the nearest Arrow unit at or finer than `p`
 *   (`0`→SECOND, `1..3`→MILLI, `4..6`→MICRO, `7..9`→NANO).
 * - `Int128/256`, `UInt128/256` → `Utf8` (base-10 decimal string; the value is lossless but not numeric).
 * - `BFloat16` → `Float32` (a bf16 widens exactly; a round trip back to bf16 truncates the mantissa).
 * - `JSON` → `Utf8` (the JSON document); `Dynamic`/`Variant` → `Utf8` (read-path `toString` rendering,
 *   ingest of arbitrary values is best-effort).
 * - `Time`/`Time64(p)` → `Duration` (ClickHouse spans may exceed 24h, so Arrow's time-of-day types do
 *   not fit); non-calendar `Interval*` → `Duration`; calendar `Interval*` (Month/Quarter/Year) →
 *   `Interval(YEAR_MONTH)`.
 * - `Nothing` → Arrow `Null`.
 *
 * For all of the above the exact source type is preserved in [CH_TYPE_META], so a read→ingest round trip
 * through this library recreates the original column type even where the structural reverse map is lossy.
 */
public object ClickHouseArrowTypes {

    private val parser = DefaultTypeParser()

    /** Builds an Arrow [Schema] aligned with `QueryResult.columnNames()`/`columnTypes()`. */
    @JvmStatic
    public fun schema(names: List<String>, types: List<String>): Schema {
        require(names.size == types.size) {
            "names (${names.size}) and types (${types.size}) must be the same length"
        }
        return Schema(names.indices.map { arrowField(names[it], types[it]) })
    }

    /**
     * Maps a single column `name`/ClickHouse type string to an Arrow [Field]. The exact source
     * type string is preserved in the field metadata under [CH_TYPE_META] so a read→CREATE-ingest
     * round trip can recreate the original column type — the structural reverse map alone is lossy
     * (e.g. UUID/IPv6 both read as `FixedSizeBinary`).
     */
    @JvmStatic
    public fun arrowField(name: String, clickHouseType: String): Field {
        val nullable = DefaultTypeParser.isNullable(clickHouseType)
        val inner = if (nullable) DefaultTypeParser.unwrapNullable(clickHouseType)!! else clickHouseType
        val field = fieldFor(name, parser.parse(inner), nullable)
        val fieldType = field.fieldType
        return Field(
            field.name,
            FieldType(fieldType.isNullable, fieldType.type, fieldType.dictionary, mapOf(CH_TYPE_META to clickHouseType)),
            field.children,
        )
    }

    private fun fieldFor(name: String, codec: ColumnCodec<*>, nullable: Boolean): Field {
        when (codec) {
            // Nested Nullable(T) (e.g. inside Array/Map/Tuple): unwrap and mark the field nullable.
            is NullableColumnCodec -> return fieldFor(name, codec.inner(), true)
            // LowCardinality(T): unwrap to T; T may itself be Nullable.
            is LowCardinalityColumnCodec -> return fieldFor(name, codec.inner(), nullable)
            is ArrayColumnCodec -> {
                val child = fieldFor(LIST_CHILD, codec.inner(), false)
                return Field(name, FieldType(nullable, ArrowType.List(), null), listOf(child))
            }
            is MapColumnCodec -> {
                val key = fieldFor(MAP_KEY, codec.keyCodec(), false)
                val value = fieldFor(MAP_VALUE, codec.valueCodec(), false)
                val entries = Field(MAP_ENTRIES, FieldType(false, ArrowType.Struct(), null), listOf(key, value))
                // keysSorted = false: ClickHouse does not guarantee map-key ordering.
                return Field(name, FieldType(nullable, ArrowType.Map(false), null), listOf(entries))
            }
            is TupleColumnCodec -> {
                val elements = codec.elements()
                val elementNames = codec.names()
                val children = elements.indices.map { i ->
                    fieldFor(elementNames[i] ?: "f$i", elements[i], false)
                }
                return Field(name, FieldType(nullable, ArrowType.Struct(), null), children)
            }
            else -> return Field(name, FieldType(nullable, leafArrowType(codec), null), null)
        }
    }

    private fun leafArrowType(codec: ColumnCodec<*>): ArrowType = when (codec) {
        is Int8Codec -> ArrowType.Int(8, true)
        is Int16Codec -> ArrowType.Int(16, true)
        is Int32Codec -> ArrowType.Int(32, true)
        is Int64Codec -> ArrowType.Int(64, true)
        is UInt8Codec -> ArrowType.Int(8, false)
        is UInt16Codec -> ArrowType.Int(16, false)
        is UInt32Codec -> ArrowType.Int(32, false)
        is UInt64Codec -> ArrowType.Int(64, false)
        is Float32Codec -> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
        is Float64Codec -> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        is BoolCodec -> ArrowType.Bool()
        is StringColumnCodec -> ArrowType.Utf8()
        is FixedStringCodec -> ArrowType.FixedSizeBinary(codec.fixedLength())
        is DateCodec -> ArrowType.Date(DateUnit.DAY)
        is Date32Codec -> ArrowType.Date(DateUnit.DAY)
        is DateTimeCodec -> ArrowType.Timestamp(TimeUnit.SECOND, codec.zoneId().toString())
        is DateTime64Codec -> ArrowType.Timestamp(timeUnitForPrecision(codec.precision()), codec.zoneId().toString())
        is DecimalCodec -> ArrowType.Decimal(codec.precision(), codec.scale(), if (codec.precision() <= 38) 128 else 256)
        is UuidCodec -> ArrowType.FixedSizeBinary(16)
        is Ipv4Codec -> ArrowType.Int(32, false)
        is Ipv6Codec -> ArrowType.FixedSizeBinary(16)
        is Enum8Codec -> ArrowType.Int(8, true)
        is Enum16Codec -> ArrowType.Int(16, true)
        // Wide integers have no native Arrow width: carry the exact value as a base-10 string.
        is Int128Codec, is UInt128Codec, is Int256Codec, is UInt256Codec -> ArrowType.Utf8()
        // BFloat16 widens exactly to Float32 (a bf16 is a truncated f32).
        is BFloat16Codec -> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
        // Semi-structured/open types are rendered as their string form (read path).
        is JsonColumnCodec, is DynamicColumnCodec, is VariantColumnCodec -> ArrowType.Utf8()
        // `Nothing` holds no value; only ever materialised as nulls (e.g. the child of an empty Array).
        is NothingCodec -> ArrowType.Null()
        // Time/Time64 and the non-calendar Intervals are spans that may exceed 24h → Arrow Duration
        // (Arrow's Time32/Time64 are time-of-day only). Calendar Intervals → Interval(YEAR_MONTH).
        is TimeCodec -> ArrowType.Duration(TimeUnit.SECOND)
        is Time64Codec -> ArrowType.Duration(timeUnitForPrecision(codec.precision()))
        is IntervalCodec ->
            if (codec.unit().isCalendar()) {
                ArrowType.Interval(IntervalUnit.YEAR_MONTH)
            } else {
                ArrowType.Duration(durationUnitForInterval(codec.unit()))
            }
        else -> throw UnsupportedOperationException(
            "No Arrow type mapping for ClickHouse type '${codec.typeName()}' (${codec.javaClass.simpleName})"
        )
    }

    /** Arrow [TimeUnit] for a non-calendar [IntervalCodec.Unit] (sub-second units keep their resolution). */
    private fun durationUnitForInterval(unit: IntervalCodec.Unit): TimeUnit = when (unit) {
        IntervalCodec.Unit.NANOSECOND -> TimeUnit.NANOSECOND
        IntervalCodec.Unit.MICROSECOND -> TimeUnit.MICROSECOND
        IntervalCodec.Unit.MILLISECOND -> TimeUnit.MILLISECOND
        // Second/Minute/Hour/Day/Week are whole-second counts: SECOND is exact.
        else -> TimeUnit.SECOND
    }

    /**
     * Inverse mapping: an Arrow [Field] → the ClickHouse type string used to declare a column when
     * a bulk-ingest target table is created from a bound schema. A nullable scalar field becomes
     * `Nullable(T)`; nullability on `Array`/`Map`/`Tuple` is dropped (ClickHouse forbids
     * `Nullable` around those). `FixedSizeBinary(n)` maps to `FixedString(n)` (the UUID/IPv6
     * read-path choices are not recoverable from Arrow alone).
     */
    @JvmStatic
    public fun clickHouseType(field: Field): String {
        // Prefer the exact source type captured on read; fall back to the structural mapping for
        // fields built outside this class (e.g. an externally-constructed ingest root).
        field.metadata[CH_TYPE_META]?.let { return it }
        val base = baseClickHouseType(field)
        val scalar = field.type.typeID != ArrowType.ArrowTypeID.List &&
            field.type.typeID != ArrowType.ArrowTypeID.Struct &&
            field.type.typeID != ArrowType.ArrowTypeID.Map
        return if (field.isNullable && scalar) "Nullable($base)" else base
    }

    private fun baseClickHouseType(field: Field): String = when (val type = field.type) {
        is ArrowType.Int -> when (type.bitWidth) {
            8 -> if (type.isSigned) "Int8" else "UInt8"
            16 -> if (type.isSigned) "Int16" else "UInt16"
            32 -> if (type.isSigned) "Int32" else "UInt32"
            64 -> if (type.isSigned) "Int64" else "UInt64"
            else -> throw UnsupportedOperationException("Unsupported Int width ${type.bitWidth}")
        }
        is ArrowType.FloatingPoint -> when (type.precision) {
            FloatingPointPrecision.SINGLE -> "Float32"
            FloatingPointPrecision.DOUBLE -> "Float64"
            else -> throw UnsupportedOperationException("Unsupported float precision ${type.precision}")
        }
        is ArrowType.Bool -> "Bool"
        is ArrowType.Utf8 -> "String"
        is ArrowType.FixedSizeBinary -> "FixedString(${type.byteWidth})"
        is ArrowType.Date -> "Date32"
        is ArrowType.Timestamp -> {
            val tz = type.timezone?.let { ", '$it'" } ?: ""
            when (type.unit) {
                TimeUnit.SECOND -> if (type.timezone != null) "DateTime('${type.timezone}')" else "DateTime"
                TimeUnit.MILLISECOND -> "DateTime64(3$tz)"
                TimeUnit.MICROSECOND -> "DateTime64(6$tz)"
                TimeUnit.NANOSECOND -> "DateTime64(9$tz)"
            }
        }
        is ArrowType.Decimal -> "Decimal(${type.precision}, ${type.scale})"
        is ArrowType.List -> "Array(${clickHouseType(field.children[0])})"
        is ArrowType.Struct -> field.children.joinToString(", ", "Tuple(", ")") { child ->
            "`${child.name}` ${clickHouseType(child)}"
        }
        is ArrowType.Map -> {
            val entries = field.children[0] // struct<key, value>
            "Map(${clickHouseType(entries.children[0])}, ${clickHouseType(entries.children[1])})"
        }
        // Best-effort inverse for span types; the exact source type (Time vs Interval, precision) is only
        // recoverable from [CH_TYPE_META], which is preferred in [clickHouseType] for library round trips.
        is ArrowType.Duration -> when (type.unit) {
            TimeUnit.SECOND -> "Time"
            TimeUnit.MILLISECOND -> "Time64(3)"
            TimeUnit.MICROSECOND -> "Time64(6)"
            TimeUnit.NANOSECOND -> "Time64(9)"
        }
        is ArrowType.Interval -> when (type.unit) {
            IntervalUnit.YEAR_MONTH -> "IntervalMonth"
            else -> throw UnsupportedOperationException("Unsupported Arrow interval unit ${type.unit}")
        }
        else -> throw UnsupportedOperationException(
            "No ClickHouse type mapping for Arrow type ${field.type} (field '${field.name}')"
        )
    }

    /** Nearest Arrow [TimeUnit] at or finer than a DateTime64 decimal precision `p`. */
    internal fun timeUnitForPrecision(precision: Int): TimeUnit = when {
        precision <= 0 -> TimeUnit.SECOND
        precision <= 3 -> TimeUnit.MILLISECOND
        precision <= 6 -> TimeUnit.MICROSECOND
        else -> TimeUnit.NANOSECOND
    }

    /** Arrow field-metadata key holding the exact source ClickHouse type string. */
    internal const val CH_TYPE_META = "clickhouse.type"

    internal const val LIST_CHILD = "item"
    internal const val MAP_ENTRIES = "entries"
    internal const val MAP_KEY = "key"
    internal const val MAP_VALUE = "value"
}
