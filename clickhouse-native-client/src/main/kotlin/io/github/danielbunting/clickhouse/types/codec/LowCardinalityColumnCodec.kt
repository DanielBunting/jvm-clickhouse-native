package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException
import java.util.LinkedHashMap

/**
 * Column-major codec for ClickHouse `LowCardinality(T)` columns, implementing the
 * native protocol's dictionary + index-key wire format (a faithful port of the
 * `SerializationLowCardinality` layout).
 *
 * Prior to this codec the [io.github.danielbunting.clickhouse.types.DefaultTypeParser]
 * transparently *unwrapped* `LowCardinality(T)` to `T` and decoded the
 * dictionary stream with `T`'s plain codec, which **silently corrupted** values
 * without throwing. This codec decodes (and encodes) the real format.
 *
 * ## Wire format (per column data stream)
 * ```
 * KeysSerializationVersion : UInt64  = 1 (SharedDictionariesWithAdditionalKeys), once at stream start
 * index_type               : UInt64  low byte = key width (0=UInt8,1=UInt16,2=UInt32,3=UInt64);
 *                                     flag 0x100 = HasAdditionalKeys, 0x200 = NeedGlobalDictionary,
 *                                     0x400 = NeedUpdateDictionary
 * [if HasAdditionalKeys]
 *   dictionary_size        : UInt64
 *   dictionary             : dictionary_size values of the INNER type T (plain T codec)
 * num_keys                 : UInt64  (== row count)
 * keys                     : num_keys unsigned integers of the index_type width, indexing the dictionary
 * ```
 * Reconstruction: `value[row] = dictionary[key[row]]`.
 *
 * ## `LowCardinality(Nullable(T))`
 * The dictionary is serialized as the non-null inner type `T` (NOT `Nullable(T)`),
 * and slot `0` of the dictionary conventionally represents SQL `NULL`. This codec
 * detects a [NullableColumnCodec] inner, decodes the dictionary with the unwrapped
 * `T` codec, and maps key `0` to `null`.
 *
 * ## Backing array
 * `Object[]` of fully-resolved per-row values (the inner codec's [get] output,
 * or `null` for a null row). [get]/[set] are direct array access, so the
 * codec works uniformly for String and numeric inner types and integrates with the bulk-insert
 * binder (which falls back to the boxed `set` path).
 *
 * ## Limitations
 * WRITE always emits a self-contained per-block dictionary with `HasAdditionalKeys`
 * set and no global dictionary (matching what ClickHouse accepts for client INSERTs). A
 * `NeedGlobalDictionary` or `NeedUpdateDictionary` flag on READ is rejected; in
 * practice single-block Native results never set them.
 */
public class LowCardinalityColumnCodec(inner: ColumnCodec<*>?) : ColumnCodec<Array<Any?>> {

    /** The inner codec as declared by the parser (possibly a [NullableColumnCodec]). */
    private val inner: ColumnCodec<*>

    /**
     * The codec used to (de)serialize the dictionary column: the non-null base type. Equal to
     * [inner] for `LowCardinality(T)`; the unwrapped `T` for
     * `LowCardinality(Nullable(T))`.
     */
    private val dictCodec: ColumnCodec<*>

    /** Whether the inner type is `Nullable(...)` (dictionary slot 0 == NULL). */
    private val nullable: Boolean

    /**
     * @param inner the codec for the inner type `T` (or `Nullable(T)`); never `null`
     */
    init {
        if (inner == null) {
            throw NullPointerException("inner codec must not be null")
        }
        this.inner = inner
        if (inner is NullableColumnCodec) {
            this.dictCodec = inner.inner()
            this.nullable = true
        } else {
            this.dictCodec = inner
            this.nullable = false
        }
    }

    /** The inner element codec (possibly `Nullable(...)`). */
    public fun inner(): ColumnCodec<*> {
        return inner
    }

    override fun typeName(): String {
        return "LowCardinality(" + inner.typeName() + ")"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    // -------------------------------------------------------------------------
    // READ
    // -------------------------------------------------------------------------

    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        // KeysSerializationVersion lives in the column's serialization state prefix —
        // written ONCE before the column's bulk data, recursively at every nesting level
        // a LowCardinality appears. The block layer calls this before read(); the version
        // is therefore NOT inline before the dictionary, which is what makes nested
        // LowCardinality (Array/Map/Tuple) decode correctly.
        val version = `in`.readUInt64()
        if (version != KEYS_VERSION_SHARED_DICTIONARY) {
            throw ProtocolException(
                    "Unsupported LowCardinality keys serialization version: " + version)
        }
    }

    @Throws(IOException::class)
    override fun writeStatePrefix(out: BinaryWriter) {
        // Mirror of readStatePrefix: emit the KeysSerializationVersion once, in the
        // column's state prefix, before any (nested) dictionary data.
        out.writeUInt64(KEYS_VERSION_SHARED_DICTIONARY)
    }

    @Throws(IOException::class)
    override fun read(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        if (rowCount == 0) {
            // Empty block (sample/header or terminating block): ClickHouse writes no
            // LowCardinality payload at all. Read nothing.
            return
        }

        // (2a) index_type. Low byte = key width; flags in the high bits.
        val indexType = `in`.readUInt64()
        val keyWidth = indexType and 0xFFL
        val hasAdditionalKeys = (indexType and FLAG_HAS_ADDITIONAL_KEYS) != 0L
        // NeedUpdateDictionary is informational here (the per-block dictionary is always
        // freshly read). A separate GLOBAL dictionary block is not supported.
        if ((indexType and FLAG_NEED_GLOBAL_DICTIONARY) != 0L) {
            throw ProtocolException(
                    "LowCardinality global dictionary is not supported (index_type="
                            + indexType + ")")
        }
        if (!hasAdditionalKeys) {
            throw ProtocolException(
                    "LowCardinality block without an additional dictionary is not supported "
                            + "(index_type=" + indexType + ")")
        }

        // (2b) dictionary: dictionary_size values of the inner (non-null) type T.
        val dictSize = `in`.readUInt64().toInt()
        @Suppress("UNCHECKED_CAST")
        val dc = dictCodec as ColumnCodec<Any>
        val dictArray = dc.allocate(dictSize)
        dc.read(`in`, dictSize, dictArray)
        val dictionary = arrayOfNulls<Any>(dictSize)
        for (i in 0 until dictSize) {
            // For Nullable(T) slot 0 is the NULL placeholder (default value on the wire).
            dictionary[i] = if (nullable && i == 0) null else dc.get(dictArray, i)
        }

        // (2c) num_keys + keys.
        val numKeys = `in`.readUInt64().toInt()
        if (numKeys != rowCount) {
            throw ProtocolException(
                    "LowCardinality key count " + numKeys + " != row count " + rowCount)
        }
        for (row in 0 until numKeys) {
            val key = readKey(`in`, keyWidth)
            dest[row] = dictionary[key]
        }
    }

    // -------------------------------------------------------------------------
    // WRITE
    // -------------------------------------------------------------------------

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        if (rowCount == 0) {
            // Empty/terminating block: emit no LowCardinality payload (mirrors read()).
            return
        }

        // Build the dictionary, mirroring ClickHouse's serialization. The nested
        // dictionary always reserves index 0 for the type's DEFAULT value (empty
        // String / 0). For a Nullable inner type an extra leading slot is prepended
        // as the NULL placeholder, so null rows map to key 0 and the nested default
        // sits at index 1; non-null distinct values follow.
        @Suppress("UNCHECKED_CAST")
        val dc = dictCodec as ColumnCodec<Any>
        val defaultValue = defaultDictValue()

        val dictValues = java.util.ArrayList<Any>()
        if (nullable) {
            dictValues.add(defaultValue) // slot 0 — NULL placeholder (default on the wire)
        }
        dictValues.add(defaultValue)     // nested default slot
        val reserved = dictValues.size   // 1 (non-nullable) or 2 (nullable)

        val indexOf = LinkedHashMap<Any, Int>()
        val keys = IntArray(rowCount)
        for (row in 0 until rowCount) {
            val v = src[row]
            if (v == null) {
                keys[row] = 0 // NULL placeholder (only reachable when nullable)
                continue
            }
            val cached = indexOf.get(v)
            val idx: Int
            if (cached == null) {
                idx = dictValues.size
                indexOf.put(v, idx)
                dictValues.add(v)
            } else {
                idx = cached
            }
            keys[row] = idx
        }

        val dictSize = dictValues.size
        val keyWidth = chooseKeyWidth(dictSize)

        // (2a) index_type: a freshly built per-block dictionary (HasAdditionalKeys),
        // marked NeedUpdateDictionary, with the chosen key width in the low byte.
        out.writeUInt64(FLAG_HAS_ADDITIONAL_KEYS or FLAG_NEED_UPDATE_DICTIONARY or keyWidth)

        // (2b) dictionary serialized as the inner (non-null) type T. Reserved slots
        // hold the nested default; the remainder hold the distinct values.
        val dictArray = dc.allocate(dictSize)
        for (i in 0 until reserved) {
            dc.set(dictArray, i, defaultValue)
        }
        for (i in reserved until dictSize) {
            dc.set(dictArray, i, dictValues.get(i))
        }
        out.writeUInt64(dictSize.toLong())
        dc.write(out, dictArray, dictSize)

        // (2c) num_keys + keys.
        out.writeUInt64(rowCount.toLong())
        writeKeys(out, keys, rowCount, keyWidth)
    }

    /**
     * The default value for the nested dictionary type `T`, used to fill the
     * reserved leading dictionary slot(s): an empty [String] for String columns,
     * otherwise numeric zero (the boxed type `dictCodec.set` accepts).
     */
    private fun defaultDictValue(): Any {
        val jt = dictCodec.javaType()
        if (jt == String::class.java) {
            return ""
        }
        return 0L
    }

    // -------------------------------------------------------------------------
    // Boxed accessors
    // -------------------------------------------------------------------------

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return inner.javaType()
    }

    private companion object {

        /** `SharedDictionariesWithAdditionalKeys` — the only key-serialization version we speak. */
        private const val KEYS_VERSION_SHARED_DICTIONARY = 1L

        // index_type flag bits (ClickHouse IndexesSerializationType):
        /** A global dictionary precedes the per-block data (unsupported here). */
        private const val FLAG_NEED_GLOBAL_DICTIONARY = 0x100L // 1u << 8

        /** An additional (per-block) dictionary follows the index_type. */
        private const val FLAG_HAS_ADDITIONAL_KEYS = 0x200L    // 1u << 9

        /** The dictionary is freshly (re)built for this block. */
        private const val FLAG_NEED_UPDATE_DICTIONARY = 0x400L // 1u << 10

        /** Low-byte index-width selectors. */
        private const val KEY_WIDTH_UINT8 = 0L
        private const val KEY_WIDTH_UINT16 = 1L
        private const val KEY_WIDTH_UINT32 = 2L
        private const val KEY_WIDTH_UINT64 = 3L

        @Throws(IOException::class)
        private fun readKey(`in`: BinaryReader, keyWidth: Long): Int {
            if (keyWidth == KEY_WIDTH_UINT8) {
                return `in`.readUInt8()
            } else if (keyWidth == KEY_WIDTH_UINT16) {
                return `in`.readUInt16()
            } else if (keyWidth == KEY_WIDTH_UINT32) {
                return `in`.readUInt32().toInt()
            } else if (keyWidth == KEY_WIDTH_UINT64) {
                return `in`.readUInt64().toInt()
            }
            throw ProtocolException("Unknown LowCardinality key width: " + keyWidth)
        }

        /** Picks the narrowest unsigned key width that can index a dictionary of `dictSize`. */
        private fun chooseKeyWidth(dictSize: Int): Long {
            val maxIndex = dictSize.toLong() - 1 // largest key value
            if (maxIndex <= 0xFFL) {
                return KEY_WIDTH_UINT8
            } else if (maxIndex <= 0xFFFFL) {
                return KEY_WIDTH_UINT16
            } else if (maxIndex <= 0xFFFF_FFFFL) {
                return KEY_WIDTH_UINT32
            }
            return KEY_WIDTH_UINT64
        }

        @Throws(IOException::class)
        private fun writeKeys(out: BinaryWriter, keys: IntArray, n: Int, keyWidth: Long) {
            if (keyWidth == KEY_WIDTH_UINT8) {
                for (i in 0 until n) {
                    out.writeUInt8(keys[i] and 0xFF)
                }
            } else if (keyWidth == KEY_WIDTH_UINT16) {
                for (i in 0 until n) {
                    out.writeUInt16(keys[i] and 0xFFFF)
                }
            } else if (keyWidth == KEY_WIDTH_UINT32) {
                for (i in 0 until n) {
                    out.writeUInt32(keys[i].toLong() and 0xFFFF_FFFFL)
                }
            } else {
                for (i in 0 until n) {
                    out.writeUInt64(keys[i].toLong())
                }
            }
        }
    }
}
