package io.github.danielbunting.clickhouse.protocol

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.types.CodecRegistry
import io.github.danielbunting.clickhouse.types.Column
import io.github.danielbunting.clickhouse.types.ColumnCodec
import io.github.danielbunting.clickhouse.types.DefaultTypeParser
import io.github.danielbunting.clickhouse.types.NullMaps
import java.io.IOException

/**
 * Serializes and deserializes a [Block] in the ClickHouse native-protocol
 * **column-major** wire format.
 *
 * Wire layout of a single data block:
 * ```
 * Block:
 *   BlockInfo                       (see below)
 *   VarUInt  num_columns
 *   VarUInt  num_rows
 *   Column[num_columns]:
 *     String   column name
 *     String   column type
 *     bytes    column data (column-major; for Nullable(T) the null-map of
 *              num_rows bytes precedes the inner T value array)
 *
 * BlockInfo:                        // VERIFY against CH.Native
 *   VarUInt  field_num = 1
 *   UInt8    is_overflows
 *   VarUInt  field_num = 2
 *   Int32    bucket_num
 *   VarUInt  field_num = 0          (terminator)
 * ```
 *
 * The block info is a sequence of `(field_num, value)` pairs terminated by a
 * `field_num` of `0`. ClickHouse historically emits two fields:
 * `is_overflows` (a `UInt8`) under field 1 and `bucket_num` (an
 * `Int32`) under field 2. This codec writes both with their default values
 * (`is_overflows = 0`, `bucket_num = -1`) and skips over any field it
 * does not recognise on read so that future fields do not break parsing.
 *
 * For each column the codec resolves a [ColumnCodec] from the
 * [CodecRegistry] keyed by the raw type string. When the type is
 * `Nullable(T)` (detected via [DefaultTypeParser.isNullable]) the
 * null-map of `num_rows` bytes is read/written first via [NullMaps] into
 * [Column.nulls], and the value array is handled by the codec resolved for the
 * unwrapped inner type `T`. [ColumnCodec] implementations therefore stay
 * null-agnostic.
 *
 * **Task W1.D3.** Wire-format authority is the CH.Native .NET source; the block
 * info layout is the standard ClickHouse-native behavior and is marked for verification.
 */
public object BlockCodec {

    /** Block-info field number that terminates the info field list. */
    private const val BLOCK_INFO_FIELD_END = 0

    /** Block-info field number for the `is_overflows` `UInt8` flag. */
    private const val BLOCK_INFO_FIELD_OVERFLOWS = 1

    /** Block-info field number for the `bucket_num` `Int32`. */
    private const val BLOCK_INFO_FIELD_BUCKET = 2

    /** Default `bucket_num` for a non-aggregated block. */
    private const val DEFAULT_BUCKET_NUM = -1

    /**
     * Minimum protocol revision at which each column carries a `UInt8`
     * "has custom serialization" flag between its type string and its data.
     * Modern servers (and this client) negotiate well above this, so the flag is
     * normally present; the no-revision overloads pass `0` to skip it
     * (used by unit tests that round-trip without a server).
     */
    // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION.
    private const val MIN_REVISION_WITH_CUSTOM_SERIALIZATION = 54_454

    /**
     * Serialization-kind preamble written after `has_custom = 1`
     * (ClickHouse `SerializationInfo::serialializeKindStackBinary` /
     * `KindStackBinarySerializationType`). A single `UInt8` selects a
     * predefined kind stack; `SPARSE` is stack {Default, Sparse}.
     */
    private const val KIND_STACK_DEFAULT = 0

    /** Predefined kind stack {Default, Sparse}: the column data is sparse-serialized. */
    private const val KIND_STACK_SPARSE = 1

    /**
     * Reads one data block from [in], resolving each column's codec from
     * [registry].
     *
     * The leading block info is consumed and discarded (its fields are not modelled
     * on [Block]); unknown info fields are skipped. The returned block has its
     * [Block.rowCount] and one populated [Column] per wire column, with
     * the codec, value array and (for `Nullable`) null-map set.
     *
     * @param in       the reader positioned at the start of a block (after the packet code)
     * @param registry resolves raw type strings to codecs
     * @return the decoded block
     * @throws IOException if the underlying source fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun read(`in`: BinaryReader, registry: CodecRegistry): Block {
        return read(`in`, registry, 0)
    }

    /**
     * Revision-aware variant of [read]. When
     * `revision >= 54454` each column carries a `UInt8` custom-serialization
     * flag after its type string, which is consumed here.
     *
     * @param in       the reader positioned at the start of a block (after the packet code)
     * @param registry resolves raw type strings to codecs
     * @param revision the negotiated protocol revision
     * @return the decoded block
     * @throws IOException if the underlying source fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun read(`in`: BinaryReader, registry: CodecRegistry, revision: Int): Block {
        readBlockInfo(`in`)

        // numColumns/numRows are untrusted VarUInts: bound them to a non-negative int so a
        // hostile or corrupt value cannot silently wrap negative (NegativeArraySizeException
        // on allocate) or drive a >2 GiB allocation. Rejected as a ProtocolException.
        val numColumns = WireLimits.checkCount(`in`.readVarUInt(), "block column count")
        val numRows = WireLimits.checkCount(`in`.readVarUInt(), "block row count")

        val block = Block()
        block.rowCount(numRows)

        for (c in 0 until numColumns) {
            val name = `in`.readString()
            val type = `in`.readString()

            var sparse = false
            if (revision >= MIN_REVISION_WITH_CUSTOM_SERIALIZATION) {
                val hasCustom = `in`.readUInt8()
                if (hasCustom != 0) {
                    sparse = readCustomKindIsSparse(`in`, name, type)
                }
            }

            val column = Column(name, type)
            column.rowCount(numRows)

            var valueType = type
            val nullable = DefaultTypeParser.isNullable(type)
            if (nullable) {
                // unwrapNullable is null only for a null input; type is non-null here.
                valueType = DefaultTypeParser.unwrapNullable(type)!!
            }

            @Suppress("UNCHECKED_CAST")
            val codec = registry.resolve(valueType) as ColumnCodec<Any>
            column.codec(codec)

            // Serialization state prefix: written ONCE per column before any of the
            // column's bulk data (and before a top-level Nullable's null-map), recursively.
            // For LowCardinality this carries the KeysSerializationVersion. Empty
            // (header/terminating) blocks carry no column payload at all, so the prefix is
            // only present when the block has rows.
            if (numRows > 0) {
                codec.readStatePrefix(`in`)
            }

            if (nullable) {
                val nulls = NullMaps.read(`in`, numRows)
                column.nulls(nulls)
            }

            val values: Any =
                if (sparse) {
                    SparseDecoder.decode(`in`, numRows, codec)
                } else {
                    readValues(codec, `in`, numRows)
                }
            column.values(values)

            block.addColumn(column)
        }

        return block
    }

    /**
     * Writes [block] to [out] in the native column-major format.
     *
     * Emits the block info, the column and row counts, then each column's name,
     * type and data. For a `Nullable(T)` column the null-map is written before the
     * inner values; a `null` [Column.nulls] on a nullable type is treated as
     * "no nulls".
     *
     * @param out   the writer to emit to (caller owns flushing)
     * @param block the block to serialize
     * @throws IOException if the underlying sink fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun write(out: BinaryWriter, block: Block) {
        write(out, block, 0)
    }

    /**
     * Revision-aware variant of [write]. When
     * `revision >= 54454` a `UInt8` custom-serialization flag of `0`
     * (default serialization) is written after each column's type string.
     *
     * @param out      the writer to emit to (caller owns flushing)
     * @param block    the block to serialize
     * @param revision the negotiated protocol revision
     * @throws IOException if the underlying sink fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun write(out: BinaryWriter, block: Block, revision: Int) {
        writeBlockInfo(out)

        val numColumns = block.columnCount()
        val numRows = block.rowCount()

        out.writeVarUInt(numColumns.toLong())
        out.writeVarUInt(numRows.toLong())

        for (c in 0 until numColumns) {
            val column = block.column(c)

            out.writeString(column.name())
            out.writeString(column.type())

            if (revision >= MIN_REVISION_WITH_CUSTOM_SERIALIZATION) {
                out.writeUInt8(0) // default serialization
            }

            @Suppress("UNCHECKED_CAST")
            val codec = column.codec() as ColumnCodec<Any?>

            // Serialization state prefix (mirror of read): written once per column before
            // any bulk data / top-level null-map, only for blocks that carry rows.
            if (numRows > 0) {
                codec.writeStatePrefix(out)
            }

            if (DefaultTypeParser.isNullable(column.type())) {
                var nulls = column.nulls()
                if (nulls == null) {
                    nulls = BooleanArray(numRows)
                }
                NullMaps.write(out, nulls, numRows)
            }

            codec.write(out, column.values(), numRows)
        }
    }

    /**
     * Reads the serialization-kind preamble that follows a `has_custom = 1` flag and
     * returns whether the column is `Sparse`-serialized.
     *
     * ClickHouse writes the kind via `SerializationInfo::serialializeKindStackBinary`:
     * a single `UInt8` `KindStackBinarySerializationType` selecting a predefined
     * stack. `DEFAULT` (0) means the data is, despite the flag, default-serialized;
     * `SPARSE` (1) is the stack {Default, Sparse}. Any other value (the `DETACHED`
     * variants, `REPLICATED`, or the `COMBINATION` escape that prefixes a varint
     * count + per-kind bytes) is a custom serialization this client does not decode, so it is
     * rejected with a specific [ProtocolException]
     * rather than silently mis-reading the column data.
     *
     * @return `true` if the column data that follows is sparse-serialized; `false`
     *         if the kind stack is `DEFAULT` (read normally)
     */
    @Throws(IOException::class)
    private fun readCustomKindIsSparse(`in`: BinaryReader, name: String, type: String): Boolean {
        val kind = `in`.readUInt8()
        return when (kind) {
            KIND_STACK_DEFAULT -> false
            KIND_STACK_SPARSE -> true
            else -> throw ProtocolException(
                "Unsupported custom serialization kind $kind for column '$name' of type " +
                    "$type (only Default and Sparse are decoded)"
            )
        }
    }

    // --- block info -----------------------------------------------------

    /**
     * Reads and discards the leading block info: a sequence of `(field_num, value)`
     * pairs terminated by `field_num == 0`. Known fields are consumed by their type;
     * any unknown field number terminates parsing defensively (it cannot be skipped without
     * knowing its width).
     */
    @Throws(IOException::class)
    private fun readBlockInfo(`in`: BinaryReader) {
        while (true) {
            val fieldNum = `in`.readVarUInt().toInt()
            if (fieldNum == BLOCK_INFO_FIELD_END) {
                break
            }
            when (fieldNum) {
                BLOCK_INFO_FIELD_OVERFLOWS -> `in`.readUInt8()
                BLOCK_INFO_FIELD_BUCKET -> `in`.readInt32()
                else ->
                    // VERIFY against CH.Native: unknown block-info fields. ClickHouse only
                    // defines fields 1 and 2; an unknown non-terminator field has no known
                    // width so we stop rather than risk desynchronising the stream.
                    return
            }
        }
    }

    /** Writes the default block info: is_overflows=0, bucket_num=-1, terminator. */
    @Throws(IOException::class)
    private fun writeBlockInfo(out: BinaryWriter) {
        out.writeVarUInt(BLOCK_INFO_FIELD_OVERFLOWS.toLong())
        out.writeUInt8(0)
        out.writeVarUInt(BLOCK_INFO_FIELD_BUCKET.toLong())
        out.writeInt32(DEFAULT_BUCKET_NUM)
        out.writeVarUInt(BLOCK_INFO_FIELD_END.toLong())
    }

    // --- value delegation (bridges ColumnCodec<?> erasure) --------------

    /**
     * Allocates the codec's backing array and reads `rowCount` values into it.
     * The unchecked casts are safe: `allocate` produces the array type the
     * codec's `read` expects.
     */
    @Throws(IOException::class)
    private fun <A> readValues(codec: ColumnCodec<A>, `in`: BinaryReader, rowCount: Int): A {
        val dest = codec.allocate(rowCount)
        codec.read(`in`, rowCount, dest)
        return dest
    }
}
