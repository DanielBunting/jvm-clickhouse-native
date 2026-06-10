package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Codec for the ClickHouse `Tuple(...)` type — a fixed-arity, heterogeneous
 * product of element types, e.g. `Tuple(UInt32, String)` or the named form
 * `Tuple(a UInt32, b String)`.
 *
 * **Native wire layout (column-major).** A Tuple column of `rowCount`
 * rows is serialized as *each element's full sub-column, in order*: all
 * `rowCount` values of element 0 (through `inner[0]`), then all
 * `rowCount` values of element 1, and so on. Unlike `Array(T)` there are
 * **no offsets** — every element column has exactly `rowCount` entries.
 * Write is the mirror image.
 *
 * The backing object is a [TupleColumn]: a holder of one inner backing array
 * per element (each `inner[i].allocate(rowCount)`). [get] materialises a
 * row as a `List<Object>` of the per-element boxed values; [set] accepts a
 * [List] or `Object[]` and scatters its components into the element arrays.
 *
 * Null handling is left to wrapper codecs (`Nullable(T)` elements are handled
 * by [NullableColumnCodec]); this codec is null-agnostic at the tuple level.
 */
public class TupleColumnCodec(elements: Array<out ColumnCodec<*>?>?, names: Array<String?>?) :
    ColumnCodec<TupleColumn> {

    private val elements: Array<ColumnCodec<*>>
    private val names: Array<String?> // element field names, or null entries when unnamed

    /**
     * @param elements the per-element codecs, in declaration order; must be non-empty
     * @param names    parallel element field names (`null` entry = unnamed); may be
     *                 `null` for a fully unnamed tuple. Used only for [typeName].
     */
    init {
        if (elements == null || elements.isEmpty()) {
            throw IllegalArgumentException("Tuple must have at least one element")
        }
        for (i in elements.indices) {
            if (elements[i] == null) {
                throw NullPointerException("Tuple element codec must not be null")
            }
        }
        @Suppress("UNCHECKED_CAST")
        this.elements = elements.copyOf() as Array<ColumnCodec<*>>
        this.names = names?.copyOf() ?: arrayOfNulls(elements.size)
    }

    /** Returns the per-element codecs in declaration order. */
    public fun elements(): Array<ColumnCodec<*>> {
        return elements.copyOf()
    }

    override fun typeName(): String {
        val sb = StringBuilder("Tuple(")
        for (i in elements.indices) {
            if (i > 0) {
                sb.append(", ")
            }
            if (names[i] != null) {
                sb.append(names[i]).append(' ')
            }
            sb.append(elements[i].typeName())
        }
        return sb.append(')').toString()
    }

    override fun allocate(rowCount: Int): TupleColumn {
        @Suppress("UNCHECKED_CAST")
        val sub = arrayOfNulls<Any>(elements.size) as Array<Any>
        for (i in elements.indices) {
            @Suppress("UNCHECKED_CAST")
            val c = elements[i] as ColumnCodec<Any>
            sub[i] = c.allocate(rowCount)
        }
        return TupleColumn(sub, rowCount)
    }

    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        // The tuple's state prefix is the concatenation of each element's state prefix,
        // in declaration order (ClickHouse SerializationTuple recurses into every element).
        for (i in elements.indices) {
            elements[i].readStatePrefix(`in`)
        }
    }

    @Throws(IOException::class)
    override fun writeStatePrefix(out: BinaryWriter) {
        for (i in elements.indices) {
            elements[i].writeStatePrefix(out)
        }
    }

    @Throws(IOException::class)
    override fun read(`in`: BinaryReader, rowCount: Int, dest: TupleColumn) {
        for (i in elements.indices) {
            @Suppress("UNCHECKED_CAST")
            val c = elements[i] as ColumnCodec<Any>
            c.read(`in`, rowCount, dest.sub[i])
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: TupleColumn, rowCount: Int) {
        for (i in elements.indices) {
            @Suppress("UNCHECKED_CAST")
            val c = elements[i] as ColumnCodec<Any>
            c.write(out, src.sub[i], rowCount)
        }
    }

    override fun get(array: TupleColumn, row: Int): Any {
        val tuple = ArrayList<Any?>(elements.size)
        for (i in elements.indices) {
            @Suppress("UNCHECKED_CAST")
            val c = elements[i] as ColumnCodec<Any>
            tuple.add(c.get(array.sub[i], row))
        }
        return tuple
    }

    override fun set(array: TupleColumn, row: Int, value: Any?) {
        val vals = asComponents(value)
        if (vals.size != elements.size) {
            throw IllegalArgumentException(
                    "Tuple expects " + elements.size + " components but got " + vals.size)
        }
        for (i in elements.indices) {
            @Suppress("UNCHECKED_CAST")
            val c = elements[i] as ColumnCodec<Any>
            c.set(array.sub[i], row, vals[i])
        }
    }

    override fun javaType(): Class<*> {
        return List::class.java
    }

    private companion object {

        private fun asComponents(value: Any?): Array<Any?> {
            if (value == null) {
                throw IllegalArgumentException("Tuple value must not be null")
            }
            if (value is List<*>) {
                return value.toTypedArray()
            }
            if (value is Array<*>) {
                @Suppress("UNCHECKED_CAST")
                return value as Array<Any?>
            }
            throw IllegalArgumentException(
                    "Tuple value must be a java.util.List or Object[], got "
                            + value.javaClass.name)
        }
    }
}
