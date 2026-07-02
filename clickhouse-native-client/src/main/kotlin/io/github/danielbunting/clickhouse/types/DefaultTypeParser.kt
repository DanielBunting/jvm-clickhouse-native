package io.github.danielbunting.clickhouse.types

import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.UnsupportedTypeException
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
import java.time.ZoneId
import java.util.LinkedHashMap

/**
 * Default [TypeParser]: recursively tokenizes a ClickHouse type string into a
 * composed [ColumnCodec] tree.
 *
 * Supported types:
 *  - Numerics: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`.
 *  - `String`, `FixedString(N)`, `UUID`.
 *  - `Date`, `DateTime`, `DateTime('tz')`,
 *    `DateTime64(p)`, `DateTime64(p, 'tz')`.
 *  - `Decimal(P, S)`.
 *  - `Enum8('a'=1, 'b'=2)`, `Enum16(...)`.
 *  - `Array(T)` (recursive).
 *  - `Nullable(T)` — the inner `T` codec is returned; null-map handling
 *    lives in the block layer. Use [isNullable] /
 *    [unwrapNullable] to detect and strip the wrapper.
 *  - `LowCardinality(T)` — a [io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec]
 *    wrapping `T`'s codec, decoding the native dictionary + index-key wire format.
 *  - `Variant(T1, ...)`, `Dynamic`, `JSON` — the semi-structured types,
 *    DECODE-only via the FLATTENED native serialization (see the respective
 *    `VariantColumnCodec` / `DynamicColumnCodec` / `JsonColumnCodec`).
 *    Requires `output_format_native_use_flattened_dynamic_and_json_serialization=1`,
 *    which the connection auto-enables per session.
 *
 * Where a parametric type carries a timezone (e.g. `DateTime`,
 * `DateTime64`) the per-type timezone is honoured; otherwise the parser's
 * default zone (constructor argument, default [ZoneId.of] UTC) is used.
 */
public class DefaultTypeParser
/**
 * Creates a parser using [defaultZone] for `DateTime` / `DateTime64`
 * columns that do not carry an explicit timezone.
 *
 * @param defaultZone fallback zone, never `null`
 */
public constructor(defaultZone: ZoneId?) : TypeParser {

    private val defaultZone: ZoneId

    init {
        if (defaultZone == null) {
            throw IllegalArgumentException("defaultZone must not be null")
        }
        this.defaultZone = defaultZone
    }

    /** Creates a parser whose default timezone for date/time types is UTC. */
    public constructor() : this(ZoneId.of("UTC"))

    override fun parse(chType: String?): ColumnCodec<*> {
        if (chType == null) {
            throw ClickHouseException("Cannot parse null ClickHouse type")
        }
        val type = chType.trim()
        if (type.isEmpty()) {
            throw ClickHouseException("Cannot parse empty ClickHouse type")
        }

        // Nullable(T): returns a NullableColumnCodec wrapping T so the inner null-map
        // is decoded when Nullable is NESTED (e.g. Array(Nullable(T))). For a TOP-LEVEL
        // Nullable column the block layer strips the wrapper string (unwrapNullable) and
        // resolves the inner type directly, keeping the primitive-array fast path — so it
        // never reaches this branch.
        if (startsWithIgnoreCase(type, "Nullable(")) {
            return NullableColumnCodec(parse(innerArgs(type, "Nullable")))
        }

        // LowCardinality(T): a custom dictionary + index-key wire format. The codec wraps
        // the inner type's codec and decodes/encodes the dictionary stream itself
        // (KeysSerializationVersion + index_type + dictionary + keys). Unwrapping to the
        // plain inner codec — the previous behaviour — silently corrupted values.
        if (startsWithIgnoreCase(type, "LowCardinality(")) {
            return LowCardinalityColumnCodec(parse(innerArgs(type, "LowCardinality")))
        }

        // Variant(T1, T2, ...): a tagged union. The server reports the member types in a
        // canonical sorted order; the parser preserves that order (discriminators index it).
        // FLATTENED native serialization (see VariantColumnCodec).
        if (startsWithIgnoreCase(type, "Variant(")) {
            val args = splitTopLevel(innerArgs(type, "Variant"))
            val variants = Array<ColumnCodec<*>>(args.size) { i -> parse(args[i].trim()) }
            return VariantColumnCodec(variants)
        }

        // Dynamic / Dynamic(max_types=N): a self-describing column whose concrete member
        // types are discovered at read time from the wire (FLATTENED serialization). The
        // codec parses each member type name with this parser.
        if (type.equals("Dynamic", ignoreCase = true) || startsWithIgnoreCase(type, "Dynamic(")) {
            return DynamicColumnCodec(this)
        }

        // JSON / JSON(...) (a.k.a. the named-tuple-of-paths "Object" type): typed paths,
        // each a Dynamic sub-column (FLATTENED serialization). Decoded to a JSON String.
        if (type.equals("JSON", ignoreCase = true) || startsWithIgnoreCase(type, "JSON(")
            || startsWithIgnoreCase(type, "Object(")
        ) {
            // The declaration carries the TYPED paths (e.g. JSON(a.b Int64)), which the
            // codec must know up front — they are serialized as their declared type, not
            // as Dynamic sub-columns, and are not discoverable from the wire.
            return JsonColumnCodec(this, type)
        }

        if (startsWithIgnoreCase(type, "Array(")) {
            val inner = parse(innerArgs(type, "Array"))
            return ArrayColumnCodec(inner)
        }

        // Geo types. On the Native protocol (verified on 25.6) geo columns are sent under
        // their ALIAS NAME ("Point", "Ring", ...), NOT as a structural Tuple/Array, and do
        // NOT carry the custom-serialization flag — so we map each alias to the equivalent
        // composed Tuple/Array codec. Decoded values are the existing List structures:
        //   Point           -> List[x, y]                  (Tuple(Float64, Float64))
        //   Ring/LineString -> List<Point>                 (Array(Point))
        //   Polygon         -> List<Ring>                  (Array(Array(Point)))
        //   MultiLineString -> List<LineString>            (Array(Array(Point)))
        //   MultiPolygon    -> List<Polygon>               (Array(Array(Array(Point))))
        when (type) {
            "Point" ->
                return geoPoint()
            "Ring", "LineString" ->
                return ArrayColumnCodec(geoPoint())
            "Polygon", "MultiLineString" ->
                return ArrayColumnCodec(ArrayColumnCodec(geoPoint()))
            "MultiPolygon" ->
                return ArrayColumnCodec(
                    ArrayColumnCodec(ArrayColumnCodec(geoPoint())))
            else -> {
                // not a geo type; fall through to the remaining parametric/leaf branches
            }
        }

        // Nested(name1 T1, name2 T2, ...): on the Native protocol a Nested column is
        // FLATTENED into one Array(T) sub-column per field (e.g. "n.a" Array(UInt32),
        // "n.b" Array(String)) — those reach the parser as ordinary Array(...) and already
        // round-trip. A whole-column "Nested(...)" type string is structurally identical to
        // Array(Tuple(...)): map it so completeness is covered if the server ever reports it.
        if (startsWithIgnoreCase(type, "Nested(")) {
            val args = splitTopLevel(innerArgs(type, "Nested"))
            val elements = arrayOfNulls<ColumnCodec<*>>(args.size)
            val names = arrayOfNulls<String>(args.size)
            for (i in args.indices) {
                val nameAndType = splitFieldName(args[i].trim())
                names[i] = nameAndType[0]
                elements[i] = parse(nameAndType[1])
            }
            return ArrayColumnCodec(TupleColumnCodec(elements, names))
        }

        // Map(K, V): on the wire IDENTICAL to Array(Tuple(K, V)) — cumulative UInt64
        // offsets then a flattened Tuple(K, V) entries column.
        if (startsWithIgnoreCase(type, "Map(")) {
            val kv = splitTopLevel(innerArgs(type, "Map"))
            if (kv.size != 2) {
                throw ClickHouseException("Map requires (Key, Value): " + chType)
            }
            return MapColumnCodec(parse(kv[0].trim()), parse(kv[1].trim()))
        }

        // Tuple(T1, T2, ...) or named Tuple(a T1, b T2, ...): a fixed heterogeneous
        // product. Each element is serialized as its own full sub-column (no offsets).
        if (startsWithIgnoreCase(type, "Tuple(")) {
            val args = splitTopLevel(innerArgs(type, "Tuple"))
            val elements = arrayOfNulls<ColumnCodec<*>>(args.size)
            val names = arrayOfNulls<String>(args.size)
            for (i in args.indices) {
                val nameAndType = splitFieldName(args[i].trim())
                names[i] = nameAndType[0]
                elements[i] = parse(nameAndType[1])
            }
            return TupleColumnCodec(elements, names)
        }

        // SimpleAggregateFunction(func, T [, ...]): on the wire this is IDENTICAL to the
        // underlying value type T (no aggregate state). args[0] is the aggregate function
        // name (ignored); the value type is the remaining arg. The single-type case is
        // handled here (the multi-type-arg form is rare and not supported).
        if (startsWithIgnoreCase(type, "SimpleAggregateFunction(")) {
            val args = splitTopLevel(innerArgs(type, "SimpleAggregateFunction"))
            if (args.size < 2) {
                throw ClickHouseException(
                    "SimpleAggregateFunction requires (func, Type): " + chType)
            }
            return parse(args[args.size - 1].trim())
        }

        // AggregateFunction(func, T...): an opaque, function-specific intermediate
        // aggregation STATE — fundamentally different from SimpleAggregateFunction
        // (which is the plain value type on the wire). The Native column data is the
        // raw output of the aggregate function's serializeBatch: there is NO per-row
        // length prefix and the per-row width is function-specific (e.g. sum(UInt64) is
        // 8 bytes, but uniqState/quantileState/groupArrayState are variable-length).
        // Because the framing is not self-delimiting, a generic decode (even to an opaque
        // byte[]) cannot find value boundaries without re-implementing every aggregate
        // function's state layout. We therefore recognise the type and fail with a
        // specific, actionable message rather than the generic "Unsupported type" or,
        // worse, silently mis-reading the stream. (Note: -State columns are not emitted
        // sparse and carry has_custom=0, so this is reached via normal column resolution.)
        if (startsWithIgnoreCase(type, "AggregateFunction(")) {
            throw UnsupportedTypeException(
                "AggregateFunction decode is not supported: " + chType
                    + ". Its Native column data is opaque, function-specific "
                    + "intermediate state with no length framing, so values cannot "
                    + "be delimited generically. Use -Merge in SQL to finalize the "
                    + "aggregate (e.g. sumMerge(v)) and read the resulting value type, "
                    + "or SimpleAggregateFunction where the underlying type is sent.")
        }

        if (startsWithIgnoreCase(type, "FixedString(")) {
            val len = Integer.parseInt(innerArgs(type, "FixedString").trim())
            return FixedStringCodec(len)
        }

        if (startsWithIgnoreCase(type, "Decimal(")) {
            val args = splitTopLevel(innerArgs(type, "Decimal"))
            if (args.size != 2) {
                throw ClickHouseException("Decimal requires (precision, scale): " + chType)
            }
            val precision = Integer.parseInt(args[0].trim())
            val scale = Integer.parseInt(args[1].trim())
            return DecimalCodec(precision, scale)
        }

        if (startsWithIgnoreCase(type, "DateTime64(")) {
            val args = splitTopLevel(innerArgs(type, "DateTime64"))
            val precision = Integer.parseInt(args[0].trim())
            val zone = if (args.size > 1) parseZone(args[1]) else defaultZone
            return DateTime64Codec(precision, zone)
        }

        // Time64(p): like DateTime64 but a time-of-day (Int64 ticks within a day).
        if (startsWithIgnoreCase(type, "Time64(")) {
            val args = splitTopLevel(innerArgs(type, "Time64"))
            val precision = Integer.parseInt(args[0].trim())
            return Time64Codec(precision)
        }

        if (startsWithIgnoreCase(type, "DateTime(")) {
            val args = splitTopLevel(innerArgs(type, "DateTime"))
            val zone = if (args.isNotEmpty() && args[0].trim().isNotEmpty()) {
                parseZone(args[0])
            } else {
                defaultZone
            }
            return DateTimeCodec(zone)
        }

        if (startsWithIgnoreCase(type, "Enum8(")) {
            return Enum8Codec(parseEnumMap(innerArgs(type, "Enum8")))
        }

        if (startsWithIgnoreCase(type, "Enum16(")) {
            return Enum16Codec(parseEnumMap(innerArgs(type, "Enum16")))
        }

        return parseLeaf(type, chType)
    }

    private fun parseLeaf(type: String, original: String): ColumnCodec<*> {
        return when (type) {
            "Int8" -> Int8Codec()
            "Int16" -> Int16Codec()
            "Int32" -> Int32Codec()
            "Int64" -> Int64Codec()
            "UInt8" -> UInt8Codec()
            "UInt16" -> UInt16Codec()
            "UInt32" -> UInt32Codec()
            "UInt64" -> UInt64Codec()
            "Int128" -> Int128Codec()
            "Int256" -> Int256Codec()
            "UInt128" -> UInt128Codec()
            "UInt256" -> UInt256Codec()
            "Float32" -> Float32Codec()
            "BFloat16" -> BFloat16Codec()
            "Float64" -> Float64Codec()
            "String" -> StringColumnCodec()
            "UUID" -> UuidCodec()
            "Bool" -> BoolCodec()
            "IPv4" -> Ipv4Codec()
            "IPv6" -> Ipv6Codec()
            "Date" -> DateCodec()
            "Date32" -> Date32Codec()
            "DateTime" -> DateTimeCodec(defaultZone)
            "Time" -> TimeCodec()
            "Nothing" ->
                // Bottom type: appears only as Array(Nothing) (empty / NULL-only arrays).
                NothingCodec()
            "IntervalNanosecond" -> IntervalCodec(IntervalCodec.Unit.NANOSECOND)
            "IntervalMicrosecond" -> IntervalCodec(IntervalCodec.Unit.MICROSECOND)
            "IntervalMillisecond" -> IntervalCodec(IntervalCodec.Unit.MILLISECOND)
            "IntervalSecond" -> IntervalCodec(IntervalCodec.Unit.SECOND)
            "IntervalMinute" -> IntervalCodec(IntervalCodec.Unit.MINUTE)
            "IntervalHour" -> IntervalCodec(IntervalCodec.Unit.HOUR)
            "IntervalDay" -> IntervalCodec(IntervalCodec.Unit.DAY)
            "IntervalWeek" -> IntervalCodec(IntervalCodec.Unit.WEEK)
            "IntervalMonth" -> IntervalCodec(IntervalCodec.Unit.MONTH)
            "IntervalQuarter" -> IntervalCodec(IntervalCodec.Unit.QUARTER)
            "IntervalYear" -> IntervalCodec(IntervalCodec.Unit.YEAR)
            else -> throw UnsupportedTypeException("Unsupported ClickHouse type: " + original)
        }
    }

    /** Parses a `'Timezone/Name'` argument into a [ZoneId]. */
    private fun parseZone(arg: String): ZoneId {
        var t = arg.trim()
        if (t.startsWith("'") && t.endsWith("'") && t.length >= 2) {
            t = t.substring(1, t.length - 1)
        }
        if (t.isEmpty()) {
            return defaultZone
        }
        return ZoneId.of(t)
    }

    public companion object {

        /** Returns `true` if [type] is a `Nullable(...)` wrapper. */
        @JvmStatic
        public fun isNullable(type: String?): Boolean {
            if (type == null) {
                return false
            }
            return startsWithIgnoreCase(type.trim(), "Nullable(")
        }

        /**
         * Strips a single `Nullable(...)` wrapper, returning the inner type string.
         * If [type] is not nullable it is returned unchanged (trimmed).
         */
        @JvmStatic
        public fun unwrapNullable(type: String?): String? {
            if (type == null) {
                return null
            }
            val trimmed = type.trim()
            if (!isNullable(trimmed)) {
                return trimmed
            }
            return innerArgs(trimmed, "Nullable").trim()
        }

        /** Builds the `Point` codec: `Tuple(Float64, Float64)` (x, y). */
        private fun geoPoint(): ColumnCodec<*> {
            return TupleColumnCodec(
                arrayOf<ColumnCodec<*>?>(Float64Codec(), Float64Codec()),
                arrayOf<String?>(null, null))
        }

        // --- parsing helpers -------------------------------------------------

        private fun startsWithIgnoreCase(s: String, prefix: String): Boolean {
            return s.regionMatches(0, prefix, 0, prefix.length, ignoreCase = true)
        }

        /**
         * Extracts the argument substring inside the parentheses of `name(...)`,
         * validating the matching close paren is the final character.
         */
        private fun innerArgs(type: String, name: String): String {
            val open = type.indexOf('(')
            if (open < 0 || !type.endsWith(")")) {
                throw ClickHouseException("Malformed parametric type: " + type)
            }
            return type.substring(open + 1, type.length - 1)
        }

        /**
         * Splits a comma-separated argument list at the top level only — commas nested
         * inside parentheses or single-quoted strings are ignored.
         */
        private fun splitTopLevel(args: String): Array<String> {
            val parts = ArrayList<String>()
            var depth = 0
            var inQuote = false
            val cur = StringBuilder()
            var i = 0
            while (i < args.length) {
                val c = args[i]
                if (inQuote) {
                    cur.append(c)
                    if (c == '\\' && i + 1 < args.length) {
                        cur.append(args[++i])
                    } else if (c == '\'') {
                        inQuote = false
                    }
                    i++
                    continue
                }
                when (c) {
                    '\'' -> {
                        inQuote = true
                        cur.append(c)
                    }
                    '(' -> {
                        depth++
                        cur.append(c)
                    }
                    ')' -> {
                        depth--
                        cur.append(c)
                    }
                    ',' ->
                        if (depth == 0) {
                            parts.add(cur.toString())
                            cur.setLength(0)
                        } else {
                            cur.append(c)
                        }
                    else -> cur.append(c)
                }
                i++
            }
            parts.add(cur.toString())
            return parts.toTypedArray()
        }

        /**
         * Splits a Tuple element argument into an optional field name and the element type.
         * Returns a two-element array `[name|null, type]`.
         *
         * An element may be `"Type"` (unnamed) or `"name Type"` (named). The
         * leading token is treated as a field name only when: (1) there is a top-level space
         * (outside parens/quotes), and (2) the part before the first such space is a plain
         * identifier (letters/digits/underscore, starting non-digit). Otherwise the whole arg
         * is the type (covers types whose own parentheses contain spaces).
         */
        private fun splitFieldName(arg: String): Array<String?> {
            var depth = 0
            var inQuote = false
            var i = 0
            while (i < arg.length) {
                val c = arg[i]
                if (inQuote) {
                    if (c == '\\') {
                        i++
                    } else if (c == '\'') {
                        inQuote = false
                    }
                    i++
                    continue
                }
                when (c) {
                    '\'' -> inQuote = true
                    '(' -> depth++
                    ')' -> depth--
                    ' ' ->
                        if (depth == 0) {
                            val first = arg.substring(0, i)
                            if (isIdentifier(first)) {
                                return arrayOf<String?>(first, arg.substring(i + 1).trim())
                            }
                            // First token is not a plain identifier -> no field name.
                            return arrayOf<String?>(null, arg)
                        }
                    else -> {
                        // keep scanning
                    }
                }
                i++
            }
            return arrayOf<String?>(null, arg)
        }

        /** True if [s] is a plain identifier (non-digit start, then word chars). */
        private fun isIdentifier(s: String): Boolean {
            if (s.isEmpty()) {
                return false
            }
            val c0 = s[0]
            if (!(Character.isLetter(c0) || c0 == '_')) {
                return false
            }
            for (i in 1 until s.length) {
                val c = s[i]
                if (!(Character.isLetterOrDigit(c) || c == '_')) {
                    return false
                }
            }
            return true
        }

        /**
         * Parses an enum body like `'a' = 1, 'b' = 2` into an ordered
         * `value -> name` map.
         */
        private fun parseEnumMap(body: String): Map<Int, String> {
            val map = LinkedHashMap<Int, String>()
            for (entry in splitTopLevel(body)) {
                val e = entry.trim()
                if (e.isEmpty()) {
                    continue
                }
                val eq = e.lastIndexOf('=')
                if (eq < 0) {
                    throw ClickHouseException("Malformed enum entry: " + entry)
                }
                val namePart = e.substring(0, eq).trim()
                val valuePart = e.substring(eq + 1).trim()
                map.put(Integer.parseInt(valuePart), unquote(namePart))
            }
            return map
        }

        /**
         * Strips surrounding single quotes and unescapes `\'`/`\\` and the doubled-quote
         * form `''` that ClickHouse uses to embed an apostrophe in an enum member name
         * (e.g. `'Query''Start'` -> `Query'Start`).
         */
        private fun unquote(s: String): String {
            var t = s.trim()
            if (t.length >= 2 && t[0] == '\'' && t[t.length - 1] == '\'') {
                t = t.substring(1, t.length - 1)
            }
            val sb = StringBuilder(t.length)
            var i = 0
            while (i < t.length) {
                val c = t[i]
                if (c == '\\' && i + 1 < t.length) {
                    sb.append(t[++i])
                } else if (c == '\'' && i + 1 < t.length && t[i + 1] == '\'') {
                    // Doubled '' collapses to a single quote; skip the second one.
                    sb.append('\'')
                    i++
                } else {
                    sb.append(c)
                }
                i++
            }
            return sb.toString()
        }
    }
}
