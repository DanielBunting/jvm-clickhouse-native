package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.protocol.WireLimits
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.util.LinkedHashMap

/**
 * Codec for the ClickHouse `Map(K, V)` type.
 *
 * **Native wire layout.** A `Map(K, V)` is serialized on the wire
 * *exactly* like `Array(Tuple(K, V))`:
 * 1. An *offsets* section: `rowCount` `UInt64` values, each the
 *    **cumulative end-offset** into the flattened entries section. Row `i`
 *    spans the half-open range `[offsets[i-1], offsets[i])` (with
 *    `offsets[-1]` taken as `0`); the last offset equals the total
 *    number of flattened key/value entries. This matches
 *    [ArrayColumnCodec] exactly.
 * 2. An *entries* section: a `Tuple(K, V)` column of `totalEntries`
 *    rows, i.e. **all keys** as a K sub-column followed by **all values** as a
 *    V sub-column (the column-major [TupleColumnCodec] layout).
 *
 * The implementation composes the existing primitives: the offset logic mirrors
 * [ArrayColumnCodec], and the entries are read/written through an internal
 * [TupleColumnCodec] over `[K, V]`.
 *
 * [get] materialises a row as a [LinkedHashMap] preserving insertion
 * (wire) order; [set] accepts any [java.util.Map] and writes its entries
 * in iteration order. Null handling is left to the block layer; this codec is
 * null-agnostic.
 */
public class MapColumnCodec(keyCodec: ColumnCodec<*>?, valueCodec: ColumnCodec<*>?) :
    ColumnCodec<MapColumn> {

    private val keyCodec: ColumnCodec<*>
    private val valueCodec: ColumnCodec<*>
    private val entries: TupleColumnCodec

    /**
     * Creates a `Map(K, V)` codec from the key and value element codecs.
     *
     * @param keyCodec   the codec for the key type `K`; must not be `null`
     * @param valueCodec the codec for the value type `V`; must not be `null`
     */
    init {
        if (keyCodec == null || valueCodec == null) {
            throw NullPointerException("Map key/value codecs must not be null")
        }
        this.keyCodec = keyCodec
        this.valueCodec = valueCodec
        this.entries = TupleColumnCodec(arrayOf<ColumnCodec<*>?>(keyCodec, valueCodec), null)
    }

    /** Returns the key element codec. */
    public fun keyCodec(): ColumnCodec<*> {
        return keyCodec
    }

    /** Returns the value element codec. */
    public fun valueCodec(): ColumnCodec<*> {
        return valueCodec
    }

    override fun typeName(): String {
        return "Map(" + keyCodec.typeName() + ", " + valueCodec.typeName() + ")"
    }

    override fun allocate(rowCount: Int): MapColumn {
        return MapColumn(LongArray(rowCount))
    }

    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        // A Map is serialized as Array(Tuple(K, V)); its state prefix is the inner
        // Tuple(K, V)'s state prefix (i.e. key's then value's), emitted once before
        // the offsets + entries.
        entries.readStatePrefix(`in`)
    }

    @Throws(IOException::class)
    override fun writeStatePrefix(out: BinaryWriter) {
        entries.writeStatePrefix(out)
    }

    @Throws(IOException::class)
    override fun read(`in`: BinaryReader, rowCount: Int, dest: MapColumn) {
        // Section 1: cumulative end-offsets (one UInt64 per row), matching ArrayColumnCodec.
        // Untrusted: require non-decreasing and bound the total to a non-negative int so a
        // hostile offset cannot crash the decode or force an unbounded allocation.
        val offsets = dest.offsets
        var prev = 0L
        for (i in 0 until rowCount) {
            val off = `in`.readUInt64()
            if (off < prev) {
                throw ProtocolException("Map offsets not monotonic at row " + i + ": "
                        + java.lang.Long.toUnsignedString(off) + " < " + prev)
            }
            offsets[i] = off
            prev = off
        }
        val total = if (rowCount == 0) 0 else WireLimits.checkCount(offsets[rowCount - 1], "Map length")

        // Section 2: the flattened (key, value) entries as a Tuple(K, V) column.
        val entryCol = entries.allocate(total)
        entries.read(`in`, total, entryCol)
        dest.entries = entryCol
        dest.entryCount = total
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: MapColumn, rowCount: Int) {
        // First pass: cumulative offsets and total entry count.
        val offsets = LongArray(rowCount)
        var running = 0L
        for (i in 0 until rowCount) {
            running += mapOf(src.pending!![i]).size
            offsets[i] = running
        }
        val total = Math.toIntExact(running)

        // Flatten every row's map into one contiguous Tuple(K, V) entries column.
        val entryCol = entries.allocate(total)
        var pos = 0
        for (i in 0 until rowCount) {
            for (e in mapOf(src.pending!![i]).entries) {
                entries.set(entryCol, pos++, java.util.List.of<Any?>(e.key, e.value))
            }
        }

        // Section 1: offsets.
        for (i in 0 until rowCount) {
            out.writeUInt64(offsets[i])
        }
        // Section 2: flattened entries.
        entries.write(out, entryCol, total)
    }

    override fun get(array: MapColumn, row: Int): Any {
        val start = if (row == 0) 0L else array.offsets[row - 1]
        val end = array.offsets[row]
        val len = Math.toIntExact(end - start)
        val base = Math.toIntExact(start)
        val map = LinkedHashMap<Any?, Any?>(Math.max(4, len * 2))
        for (j in 0 until len) {
            @Suppress("UNCHECKED_CAST")
            val entry = entries.get(array.entries!!, base + j) as List<Any?>
            map.put(entry[0], entry[1])
        }
        return map
    }

    override fun set(array: MapColumn, row: Int, value: Any?) {
        array.ensurePending(row)
        array.pending!![row] = mapOf(value)
    }

    override fun javaType(): Class<*> {
        return Map::class.java
    }

    private companion object {

        private fun mapOf(value: Any?): Map<*, *> {
            if (value == null) {
                return java.util.Map.of<Any, Any>()
            }
            if (value is Map<*, *>) {
                return value
            }
            throw IllegalArgumentException(
                    "Map value must be a java.util.Map, got " + value.javaClass.name)
        }
    }
}
