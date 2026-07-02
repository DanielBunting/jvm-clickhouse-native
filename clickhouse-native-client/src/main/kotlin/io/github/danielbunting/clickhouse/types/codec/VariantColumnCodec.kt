package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Self-framing column codec for ClickHouse `Variant(T1, T2, ...)` columns using the
 * **FLATTENED** native serialization
 * (`output_format_native_use_flattened_dynamic_and_json_serialization = 1`).
 *
 * ## Member type order
 *
 * The server reports the member types in a canonical *sorted* order
 * (e.g. `Variant(UInt32, String)` is reported as `Variant(String, UInt32)`).
 * The parser preserves the reported order; the per-row discriminator indexes into it.
 *
 * ## Wire format (per column data stream), reverse-engineered on ClickHouse 25.6
 *
 * ```
 * UInt64  discriminators_version = 0   (BASIC: one discriminator byte per row)
 * UInt8   discriminator[rowCount]      index into the sorted member types, or 0xFF = NULL
 * for each member type i, in sorted order:
 *   sub-column with count_i rows       (count_i = #discriminators == i), decoded by the
 *                                       member's plain codec; values appear in row order
 * ```
 *
 * ## Decoded value
 *
 * [get] returns the active member's boxed value for the row, or `null`
 * for the NULL discriminator.
 *
 * ## ENCODE ([write])
 *
 * The inverse of [read]: the backing `Object[]` holds one boxed Java value
 * per row (set via [set]); [write] emits `UInt64 version = 0` (BASIC),
 * then one `UInt8` discriminator per row, then each member's sub-column (values whose
 * discriminator equals that member, in row order) via the member codec's own
 * [ColumnCodec.write]. The server accepts this flattened layout on binary Native INPUT
 * with no input setting (its deserializer reads the version prefix).
 *
 * ### Member disambiguation rule
 *
 * A `null` row maps to the NULL discriminator (`0xFF`). Otherwise the row's
 * value is assigned to the **first** member (in the server's canonical sorted order, which
 * the parser preserves) whose codec accepts the value's Java type, where "accepts" means:
 *
 *  - a `String` member accepts a [CharSequence];
 *  - an integer member (`javaType` [Long]/[Int]/[Short]/[Byte])
 *    accepts any integral [Number] (`Long`/`Integer`/`Short`/`Byte`/`BigInteger`);
 *  - a floating member (`javaType` [Double]/[Float]) accepts any [Number];
 *  - otherwise the member accepts a value whose runtime class is assignable to the member's
 *    `javaType()`.
 *
 * Because members are tried in sorted order, ambiguous numerics resolve to the first
 * matching numeric member. A value matching no member triggers an
 * [IllegalArgumentException] at write time.
 */
public class VariantColumnCodec
/**
 * @param variants the member codecs in the server's reported (sorted) order; non-empty
 */
public constructor(variants: Array<ColumnCodec<*>>?) : ColumnCodec<Array<Any?>> {

    private val variants: Array<ColumnCodec<*>>

    init {
        if (variants == null || variants.size == 0) {
            throw IllegalArgumentException("Variant requires at least one member type")
        }
        if (variants.size > 254) {
            throw IllegalArgumentException("Variant supports at most 254 member types")
        }
        this.variants = variants.copyOf()
    }

    override fun typeName(): String {
        val sb = StringBuilder("Variant(")
        for (i in variants.indices) {
            if (i > 0) {
                sb.append(", ")
            }
            sb.append(variants[i].typeName())
        }
        return sb.append(')').toString()
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        // The Variant itself carries no state prefix (its discriminators version is
        // inline in the body), but each member's prefix appears here, recursively —
        // e.g. a LowCardinality or JSON member contributes its own prefix bytes.
        for (i in variants.indices) {
            variants[i].readStatePrefix(`in`)
        }
    }

    @Throws(IOException::class)
    override fun writeStatePrefix(out: BinaryWriter) {
        // Mirror of readStatePrefix: recurse the member prefixes in member order.
        for (i in variants.indices) {
            variants[i].writeStatePrefix(out)
        }
    }

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    override fun read(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        if (rowCount == 0) {
            return // empty/header block: no payload
        }

        val version = `in`.readUInt64()
        if (version != DISCRIMINATORS_VERSION_BASIC) {
            throw ProtocolException(
                "Unsupported Variant discriminators serialization version: " + version
                    + " (only flattened BASIC=0 is supported; ensure "
                    + "output_format_native_use_flattened_dynamic_and_json_serialization=1)"
            )
        }

        val discriminators = IntArray(rowCount)
        val counts = IntArray(variants.size)
        for (row in 0 until rowCount) {
            val d = `in`.readUInt8()
            discriminators[row] = d
            if (d != NULL_DISCRIMINATOR) {
                if (d >= variants.size) {
                    throw ProtocolException(
                        "Variant discriminator " + d + " out of range [0,"
                            + variants.size + ")"
                    )
                }
                counts[d]++
            }
        }

        // Decode each member sub-column (count_i values, in row order).
        val subArrays = arrayOfNulls<Any>(variants.size)
        for (i in variants.indices) {
            val codec = variants[i] as ColumnCodec<Any>
            val arr = codec.allocate(counts[i])
            codec.read(`in`, counts[i], arr)
            subArrays[i] = arr
        }

        // Scatter sub-column values back to rows by walking discriminators.
        val cursor = IntArray(variants.size)
        for (row in 0 until rowCount) {
            val d = discriminators[row]
            if (d == NULL_DISCRIMINATOR) {
                dest[row] = null
            } else {
                val codec = variants[d] as ColumnCodec<Any?>
                dest[row] = codec.get(subArrays[d], cursor[d]++)
            }
        }
    }

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        if (rowCount == 0) {
            return // empty/terminating block: no payload (mirrors read())
        }

        // version = 0 (BASIC: one discriminator byte per row)
        out.writeUInt64(DISCRIMINATORS_VERSION_BASIC)

        // Resolve a discriminator per row and count per-member occupancy.
        val discriminators = IntArray(rowCount)
        val counts = IntArray(variants.size)
        for (row in 0 until rowCount) {
            val v = src[row]
            if (v == null) {
                discriminators[row] = NULL_DISCRIMINATOR
                continue
            }
            val d = memberFor(v)
            discriminators[row] = d
            counts[d]++
        }

        // Emit discriminators (UInt8) in row order.
        for (row in 0 until rowCount) {
            out.writeUInt8(discriminators[row])
        }

        // Build and emit each member's sub-column (values in row order).
        for (i in variants.indices) {
            val codec = variants[i] as ColumnCodec<Any>
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
     * Returns the index of the first member (in sorted order) whose codec accepts
     * [value]'s Java type, per the disambiguation rule documented on the class.
     *
     * @throws IllegalArgumentException if no member accepts the value
     */
    private fun memberFor(value: Any): Int {
        for (i in variants.indices) {
            if (accepts(variants[i], value)) {
                return i
            }
        }
        throw IllegalArgumentException(
            "No Variant member of " + typeName() + " accepts a value of type "
                + value.javaClass.name + ": " + value
        )
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return Any::class.java
    }

    public companion object {

        /** Discriminator value (UInt8) marking a SQL NULL row. */
        private const val NULL_DISCRIMINATOR: Int = 0xFF

        /** The only discriminators-serialization version we speak (BASIC: one byte per row). */
        private const val DISCRIMINATORS_VERSION_BASIC: Long = 0L

        /** Whether [codec] can encode [value] per the documented acceptance rule. */
        @JvmStatic
        internal fun accepts(codec: ColumnCodec<*>, value: Any): Boolean {
            val jt: Class<*> = codec.javaType()
            if (jt == String::class.java) {
                return value is CharSequence
            }
            if (jt == Long::class.javaObjectType || jt == Int::class.javaObjectType
                || jt == Short::class.javaObjectType || jt == Byte::class.javaObjectType
            ) {
                return value is Long || value is Int
                    || value is Short || value is Byte
                    || value is java.math.BigInteger
            }
            if (jt == Double::class.javaObjectType || jt == Float::class.javaObjectType) {
                return value is Number
            }
            return jt.isInstance(value)
        }
    }
}
