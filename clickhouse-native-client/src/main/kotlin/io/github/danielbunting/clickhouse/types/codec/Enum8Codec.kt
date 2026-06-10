package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Codec for the ClickHouse `Enum8` type.
 *
 * Wire format: one signed byte per row (same wire encoding as `Int8`).
 * The backing array is `int[]` holding the raw Int8 values (widened to
 * `int`). [get] maps the integer key to its [String] name
 * via the provided enum map; [set] accepts either a [String] name
 * (reverse-looked-up) or a boxed [Integer][Int] ordinal directly.
 *
 * The enum map is supplied at construction time, e.g.:
 * `Map.of(1, "active", 2, "inactive")`.
 */
public class Enum8Codec(enumMap: Map<Int, String>) : ColumnCodec<IntArray> {

    private val enumMap: Map<Int, String>

    /** Reverse map from name -> ordinal for [set]. */
    private val reverseMap: Map<String, Int>

    /**
     * Constructs an `Enum8Codec` with the given ordinal-to-name mapping.
     *
     * @param enumMap a map from Int8 ordinal values to their string names
     */
    init {
        this.enumMap = java.util.Map.copyOf(enumMap)
        // Build the reverse map for set()
        val rev = java.util.HashMap<String, Int>(enumMap.size * 2)
        for (e in enumMap.entries) {
            rev[e.value] = e.key
        }
        this.reverseMap = java.util.Map.copyOf(rev)
    }

    override fun typeName(): String {
        return "Enum8"
    }

    override fun allocate(rowCount: Int): IntArray {
        return IntArray(rowCount)
    }

    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: IntArray) {
        for (i in 0 until rowCount) {
            // readInt8() returns a signed byte; widen to int preserving sign
            dest[i] = input.readInt8().toInt()
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: IntArray, rowCount: Int) {
        for (i in 0 until rowCount) {
            out.writeInt8(src[i].toByte())
        }
    }

    override fun get(array: IntArray, row: Int): Any? {
        return enumMap[array[row]]
    }

    override fun set(array: IntArray, row: Int, value: Any?) {
        if (value is String) {
            val ordinal = reverseMap[value]
                ?: throw IllegalArgumentException("Unknown Enum8 name: " + value)
            array[row] = ordinal
        } else if (value is Number) {
            array[row] = value.toInt()
        } else {
            throw IllegalArgumentException("Cannot set Enum8 from: " + value)
        }
    }

    /** Returns the raw Int8 ordinal stored in the backing array (not the name). */
    override fun getLong(array: IntArray, row: Int): Long {
        return array[row].toLong()
    }

    /** Stores the raw Int8 ordinal directly (no name lookup). */
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
