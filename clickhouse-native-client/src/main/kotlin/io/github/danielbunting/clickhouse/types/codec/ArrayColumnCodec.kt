package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.protocol.WireLimits
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Codec for the ClickHouse `Array(T)` type.
 *
 * **Native wire layout.** For a column of `rowCount` array rows the
 * data is split into two consecutive sections:
 * 1. An *offsets* section: `rowCount` `UInt64` values, each the
 *    **cumulative end-offset** into the flattened value section. Row `i`
 *    therefore spans the half-open range `[offsets[i-1], offsets[i])` (with
 *    `offsets[-1]` taken as `0`); the last offset equals the total
 *    number of flattened inner values.
 * 2. A *values* section: that total number of inner `T` values, read
 *    and written contiguously through the supplied inner [ColumnCodec].
 *
 * The backing array is an `Object[]` whose element `i` is a
 * [List] holding the inner Java values for row `i`. The codec is fully
 * recursive: the inner codec may itself be an [ArrayColumnCodec], in which case
 * each list element is itself a `List` (i.e. `Array(Array(UInt32))`
 * materialises as `List<List<Long>>` per row).
 *
 * Null handling is left to the block layer; this codec is null-agnostic.
 *
 * Implements task W1.B5.
 */
public class ArrayColumnCodec(inner: ColumnCodec<*>?) : ColumnCodec<Array<Any?>> {

    private val inner: ColumnCodec<*>

    /**
     * Creates an `Array(T)` codec wrapping the codec for the element type `T`.
     *
     * @param inner the codec for the inner element type; must not be `null`
     */
    init {
        if (inner == null) {
            throw NullPointerException("inner codec must not be null")
        }
        this.inner = inner
    }

    /** Returns the inner element codec. */
    public fun inner(): ColumnCodec<*> {
        return inner
    }

    override fun typeName(): String {
        return "Array(" + inner.typeName() + ")"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        // The array's serialization state prefix is its element's state prefix
        // (ClickHouse SerializationArray delegates to the nested serialization),
        // emitted once before the offsets + flattened values.
        inner.readStatePrefix(`in`)
    }

    @Throws(IOException::class)
    override fun writeStatePrefix(out: BinaryWriter) {
        inner.writeStatePrefix(out)
    }

    @Throws(IOException::class)
    override fun read(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        // Section 1: cumulative end-offsets (one UInt64 per row). The offsets are untrusted:
        // require them non-decreasing (a row spans [prev, offsets[i])) and bound the total to
        // a non-negative int. Monotonic + last-in-int-range guarantees every slice index is a
        // valid int, so a hostile offset can neither crash the decode (NegativeArraySize /
        // ArithmeticException) nor drive an unbounded allocation.
        val offsets = LongArray(rowCount)
        var prev = 0L
        for (i in 0 until rowCount) {
            val off = `in`.readUInt64()
            if (off < prev) {
                throw ProtocolException("Array offsets not monotonic at row " + i + ": "
                        + java.lang.Long.toUnsignedString(off) + " < " + prev)
            }
            offsets[i] = off
            prev = off
        }
        val total = if (rowCount == 0) 0 else WireLimits.checkCount(offsets[rowCount - 1], "Array length")

        // Section 2: the flattened inner values, read column-major into one array.
        val innerArray = readInner(`in`, total)

        // Slice the flat values back into one List per row.
        var start = 0L
        for (i in 0 until rowCount) {
            val end = offsets[i]
            val len = Math.toIntExact(end - start)
            val row = ArrayList<Any?>(len)
            val base = Math.toIntExact(start)
            for (j in 0 until len) {
                row.add(getInner(innerArray, base + j))
            }
            dest[i] = row
            start = end
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        // First pass: compute cumulative offsets and the total flattened length.
        val offsets = LongArray(rowCount)
        var running = 0L
        for (i in 0 until rowCount) {
            running += sizeOf(src[i])
            offsets[i] = running
        }
        val total = Math.toIntExact(running)

        // Flatten every row's list into one contiguous inner backing array.
        val innerArray = allocateInner(total)
        var pos = 0
        for (i in 0 until rowCount) {
            val row = asList(src[i])
            for (j in 0 until row.size) {
                setInner(innerArray, pos++, row[j])
            }
        }

        // Section 1: offsets.
        for (i in 0 until rowCount) {
            out.writeUInt64(offsets[i])
        }
        // Section 2: flattened values.
        writeInner(out, innerArray, total)
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return List::class.java
    }

    // --- Type-bridging helpers around the wildcard inner codec ----------------
    // The inner codec is ColumnCodec<?>; these helpers localise the unavoidable
    // unchecked casts so the rest of the logic stays clean.

    @Throws(IOException::class)
    private fun readInner(`in`: BinaryReader, count: Int): Any {
        @Suppress("UNCHECKED_CAST")
        val c = inner as ColumnCodec<Any>
        val arr = c.allocate(count)
        c.read(`in`, count, arr)
        return arr
    }

    @Throws(IOException::class)
    private fun writeInner(out: BinaryWriter, array: Any, count: Int) {
        @Suppress("UNCHECKED_CAST")
        val c = inner as ColumnCodec<Any>
        c.write(out, array, count)
    }

    private fun allocateInner(count: Int): Any {
        @Suppress("UNCHECKED_CAST")
        return (inner as ColumnCodec<Any>).allocate(count)
    }

    private fun getInner(array: Any, row: Int): Any? {
        @Suppress("UNCHECKED_CAST")
        return (inner as ColumnCodec<Any>).get(array, row)
    }

    private fun setInner(array: Any, row: Int, value: Any?) {
        @Suppress("UNCHECKED_CAST")
        (inner as ColumnCodec<Any>).set(array, row, value)
    }

    private companion object {

        private fun asList(value: Any?): List<*> {
            if (value == null) {
                return java.util.List.of<Any>()
            }
            if (value is List<*>) {
                return value
            }
            throw IllegalArgumentException(
                    "Array element must be a java.util.List, got " + value.javaClass.name)
        }

        private fun sizeOf(value: Any?): Int {
            return asList(value).size
        }
    }
}
