package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Codec for the ClickHouse `Enum16` type.
 *
 * Wire format: one signed little-endian 16-bit integer per row (same encoding
 * as `Int16`). The backing array is `int[]` holding the raw Int16
 * values (widened to `int`). [get] maps the integer key to its
 * [String] name via the provided enum map; [set] accepts either
 * a [String] name or a boxed [Integer][Int] ordinal.
 *
 * The enum map is supplied at construction time and allows values in the full
 * signed Int16 range `[-32768, 32767]`.
 */
public class Enum16Codec(enumMap: Map<Int, String>) : ColumnCodec<IntArray> {

    private val enumMap: Map<Int, String>

    /** Reverse map from name -> ordinal for [set]. */
    private val reverseMap: Map<String, Int>

    /**
     * Constructs an `Enum16Codec` with the given ordinal-to-name mapping.
     *
     * @param enumMap a map from Int16 ordinal values to their string names
     */
    init {
        this.enumMap = java.util.Map.copyOf(enumMap)
        val rev = java.util.HashMap<String, Int>(enumMap.size * 2)
        for (e in enumMap.entries) {
            rev[e.value] = e.key
        }
        this.reverseMap = java.util.Map.copyOf(rev)
    }

    override fun typeName(): String {
        return "Enum16"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        for (i in 0 until rowCount) {
            // readInt16() returns a signed short; widen to int
            dest[i] = input.readInt16().toInt()
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeInt16(src[i].toShort())
        }
    }

    override fun get(array: IntArray, row: Int): Any? {
        return enumMap[array[row]]
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        if (value is String) {
            val ordinal = reverseMap[value]
                ?: throw IllegalArgumentException("Unknown Enum16 name: " + value)
            array[row] = ordinal
        } else if (value is Number) {
            array[row] = value.toInt()
        } else {
            throw IllegalArgumentException("Cannot set Enum16 from: " + value)
        }
    }

    /** Returns the raw Int16 ordinal stored in the backing array (not the name). */
    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    /** Stores the raw Int16 ordinal directly (no name lookup). */
    override fun setLong(array: IntArray, row: Int, v: Long) {
        array[row] = v.toInt()
    }

    override fun javaType(): Class<*> {
        return String::class.java
    }

    /**
     * Returns the ordinal-to-name mapping for this enum type.
     *
     * @return an unmodifiable view of the enum map
     */
    public fun enumMap(): Map<Int, String> {
        return enumMap
    }
}
