package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import io.github.danielbunting.clickhouse.types.TypeParser
import java.io.IOException

/**
 * Self-framing column codec for ClickHouse `Dynamic` columns using the **FLATTENED**
 * native serialization
 * (`output_format_native_use_flattened_dynamic_and_json_serialization = 1`).
 *
 * Unlike [VariantColumnCodec], a `Dynamic` column's concrete member types are
 * not known from the type string — they are discovered at read time from the wire and parsed
 * via the supplied [TypeParser].
 *
 * ## Wire format (per column data stream), reverse-engineered on ClickHouse 25.6
 *
 * ```
 * UInt64   version = 3
 * VarUInt  num_types
 * String   type_name[num_types]            (sorted)
 * UInt8    discriminator[rowCount]         index into type_name, or num_types = NULL
 * for each type i, in order:
 *   sub-column with count_i rows           (count_i = #discriminators == i)
 * ```
 *
 * ## Decoded value
 *
 * [get] returns the active member's boxed value for the row, or `null` for a
 * NULL row (discriminator == num_types). With `num_types == 0` every row is NULL.
 *
 * ## ENCODE ([write])
 *
 * The inverse of [read]. The backing `Object[]` holds one boxed Java value per
 * row (set via [set]). [write] infers a ClickHouse type per non-null value,
 * collects the distinct types, sorts them by name, then emits `UInt64 version = 3`,
 * `VarUInt num_types`, the sorted type-name strings, one `UInt8` discriminator per
 * row (`num_types` = NULL), then each type's sub-column. The server accepts this
 * flattened layout on binary Native INPUT with no input setting.
 *
 * ### Java → ClickHouse type inference (lossy)
 *
 *  - `Long` → `Int64`
 *  - `Integer` → `Int32`
 *  - `Short` → `Int16`, `Byte` → `Int8`
 *  - `Double` → `Float64`, `Float` → `Float32`
 *  - `Boolean` → `Bool`
 *  - `CharSequence` → `String`
 *
 * Inference is by the value's Java class, so the round-trip is exact only for the types
 * above; e.g. an `Integer` written into a `Dynamic` comes back as `Int32`
 * (read as `Integer`), and a `Long` as `Int64` (read as `Long`).
 * A value whose class is none of the above triggers an [IllegalArgumentException] at
 * write time. ClickHouse may itself coalesce/promote the on-disk Dynamic types after the
 * INSERT (a server concern, independent of the wire encoding here).
 */
public class DynamicColumnCodec
/**
 * @param parser used to resolve member type names discovered on the wire; never `null`
 */
public constructor(parser: TypeParser?) : ColumnCodec<Array<Any?>> {

    private val parser: TypeParser

    init {
        if (parser == null) {
            throw NullPointerException("parser")
        }
        this.parser = parser
    }

    override fun typeName(): String {
        return "Dynamic"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun read(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        if (rowCount == 0) {
            return
        }
        val version = `in`.readUInt64()
        if (version != DYNAMIC_VERSION) {
            throw ProtocolException(
                "Unsupported Dynamic serialization version: " + version
                    + " (expected flattened version 3; ensure "
                    + "output_format_native_use_flattened_dynamic_and_json_serialization=1)"
            )
        }
        readFlattened(`in`, rowCount, dest, parser)
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        if (rowCount == 0) {
            return // empty/terminating block: no payload (mirrors read())
        }
        out.writeUInt64(DYNAMIC_VERSION)
        writeFlattened(out, src, rowCount, parser)
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        if (value != null) {
            // Validate inferability NOW, so an unsupported value fails at add() time —
            // before any block bytes exist — with the row still identifiable, instead of
            // surfacing at flush time. The inference itself is a short instanceof chain
            // (no allocation); write() re-runs it to build the type set.
            inferClickHouseType(value)
        }
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return Any::class.java
    }

    public companion object {

        /** The flattened Dynamic structure version emitted by ClickHouse 25.6. */
        private const val DYNAMIC_VERSION: Long = 3L

        /**
         * Reads the flattened Dynamic *body* (type list + discriminators + sub-columns)
         * after the `UInt64 version` has already been consumed. Shared with
         * [JsonColumnCodec], which embeds a Dynamic per path.
         */
        @JvmStatic
        @Throws(IOException::class)
        @Suppress("UNCHECKED_CAST")
        internal fun readFlattened(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>, parser: TypeParser) {
            val numTypes = `in`.readVarUInt().toInt()
            val codecs = arrayOfNulls<ColumnCodec<*>>(numTypes)
            for (i in 0 until numTypes) {
                codecs[i] = parser.parse(`in`.readString())
            }

            // Member types with their own serialization PREFIX (e.g. JSON: version +
            // path list + per-path Dynamic type lists) emit that prefix HERE — after the
            // member-type names, BEFORE the discriminators (verified against 25.8 wire
            // bytes). readStatePrefix recurses through container codecs
            // (Array/Map/Tuple/Nullable/Variant), so a member like Array(JSON)
            // contributes its element's prefix at this position too; JsonColumnCodec
            // stashes what it reads here and consumes it in read(). Scalar members
            // contribute nothing.
            for (i in 0 until numTypes) {
                codecs[i]!!.readStatePrefix(`in`)
            }

            val discriminators = IntArray(rowCount)
            val counts = IntArray(numTypes)
            for (row in 0 until rowCount) {
                val d = `in`.readUInt8()
                discriminators[row] = d
                if (d != numTypes) {
                    if (d > numTypes) {
                        throw ProtocolException(
                            "Dynamic discriminator " + d + " out of range [0," + numTypes + "]"
                        )
                    }
                    counts[d]++
                }
            }

            val subArrays = arrayOfNulls<Any>(numTypes)
            for (i in 0 until numTypes) {
                val codec = codecs[i] as ColumnCodec<Any>
                val arr = codec.allocate(counts[i])
                codec.read(`in`, counts[i], arr)
                subArrays[i] = arr
            }

            val cursor = IntArray(numTypes)
            for (row in 0 until rowCount) {
                val d = discriminators[row]
                if (d == numTypes) {
                    dest[row] = null
                } else {
                    val codec = codecs[d] as ColumnCodec<Any?>
                    dest[row] = codec.get(subArrays[d], cursor[d]++)
                }
            }
        }

        /**
         * Writes the flattened Dynamic *body* (type list + discriminators + sub-columns)
         * after the `UInt64 version` has already been emitted. The inverse of
         * [readFlattened]; shared with [JsonColumnCodec], which embeds a Dynamic per
         * path.
         */
        @JvmStatic
        @Throws(IOException::class)
        @Suppress("UNCHECKED_CAST")
        internal fun writeFlattened(out: BinaryWriter, src: Array<Any?>, rowCount: Int, parser: TypeParser) {
            // Collect the distinct inferred CH type names (sorted) and a codec per type.
            val typeSet = java.util.TreeSet<String>()
            for (row in 0 until rowCount) {
                val v = src[row]
                if (v != null) {
                    typeSet.add(inferClickHouseType(v))
                }
            }
            val typeNames = typeSet.toTypedArray()
            val numTypes = typeNames.size
            val typeIndex = java.util.HashMap<String, Int>()
            val codecs = arrayOfNulls<ColumnCodec<*>>(numTypes)
            for (i in 0 until numTypes) {
                typeIndex[typeNames[i]] = i
                codecs[i] = parser.parse(typeNames[i])
            }

            // num_types + sorted type names.
            out.writeVarUInt(numTypes.toLong())
            for (i in 0 until numTypes) {
                out.writeString(typeNames[i])
            }

            // Member state prefixes, mirroring the pass in [readFlattened]. Every type
            // this writer can infer has an empty state prefix, so no bytes are emitted
            // today; the hook keeps the write layout aligned with the read layout.
            for (i in 0 until numTypes) {
                codecs[i]!!.writeStatePrefix(out)
            }

            // Resolve a discriminator per row (NULL == numTypes) and count occupancy.
            val discriminators = IntArray(rowCount)
            val counts = IntArray(numTypes)
            for (row in 0 until rowCount) {
                val v = src[row]
                if (v == null) {
                    discriminators[row] = numTypes
                } else {
                    val d = typeIndex[inferClickHouseType(v)]!!
                    discriminators[row] = d
                    counts[d]++
                }
            }
            for (row in 0 until rowCount) {
                out.writeUInt8(discriminators[row])
            }

            // Per-type sub-columns (values in row order).
            for (i in 0 until numTypes) {
                val codec = codecs[i] as ColumnCodec<Any>
                val arr = codec.allocate(counts[i])
                var cursor = 0
                for (row in 0 until rowCount) {
                    if (discriminators[row] == i) {
                        codec.set(arr, cursor++, src[row])
                    }
                }
                codec.write(out, arr, counts[i])
            }
        }

        /**
         * Infers the ClickHouse type name to encode [value] as, by its Java class.
         * See the class KDoc for the (lossy) mapping.
         *
         * @throws IllegalArgumentException if the value's Java type has no Dynamic mapping
         */
        @JvmStatic
        internal fun inferClickHouseType(value: Any): String {
            if (value is Long) {
                return "Int64"
            }
            if (value is Int) {
                return "Int32"
            }
            if (value is Short) {
                return "Int16"
            }
            if (value is Byte) {
                return "Int8"
            }
            if (value is Double) {
                return "Float64"
            }
            if (value is Float) {
                return "Float32"
            }
            if (value is Boolean) {
                return "Bool"
            }
            if (value is CharSequence) {
                return "String"
            }
            if (value is java.time.LocalDate) {
                return "Date"
            }
            if (value is java.time.Instant) {
                return "DateTime64(9)"
            }
            if (value is java.util.UUID) {
                return "UUID"
            }
            if (value is java.math.BigInteger) {
                // Pick the narrowest wide-int family that holds the value; the codecs
                // range-check again at set() time.
                return if (value.bitLength() < 127) "Int128" else "Int256"
            }
            if (value is List<*>) {
                // Element type from the first NON-NULL element; a null element makes the
                // array Nullable. An empty array infers Array(Nothing) — the server's own
                // type for an untyped empty array literal.
                var elemType: String? = null
                var hasNull = false
                for (e in value) {
                    if (e == null) {
                        hasNull = true
                    } else if (elemType == null) {
                        elemType = inferClickHouseType(e)
                    }
                }
                return when {
                    elemType == null && hasNull -> "Array(Nullable(Nothing))"
                    elemType == null -> "Array(Nothing)"
                    hasNull -> "Array(Nullable($elemType))"
                    else -> "Array($elemType)"
                }
            }
            if (value is Map<*, *>) {
                val first = value.entries.firstOrNull()
                    ?: return "Map(String, String)" // empty map: any concrete K/V works
                val k = first.key ?: throw IllegalArgumentException(
                    "Map keys must be non-null for Dynamic inference")
                val v = first.value ?: throw IllegalArgumentException(
                    "Map values must be non-null for Dynamic inference (wrap in a typed column instead)")
                return "Map(" + inferClickHouseType(k) + ", " + inferClickHouseType(v) + ")"
            }
            throw IllegalArgumentException(
                "No Dynamic type inference for Java type " + value.javaClass.name
                    + " (supported: Long, Integer, Short, Byte, Double, Float, Boolean, String,"
                    + " LocalDate, Instant, UUID, BigInteger, List, Map)"
            )
        }
    }
}
