package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.io.IOException

/**
 * Column-major codec for ClickHouse `String` columns with **lazy UTF-8
 * decoding**.
 *
 * Wire format: each value is a `VarUInt` byte-length prefix followed by
 * that many UTF-8 bytes (identical to the length-prefixed string used elsewhere in
 * the native protocol). There is no trailing NUL; the length field is the authority.
 *
 * ## Lazy materialisation
 *
 * The backing type is [StringColumn], not `String[]`. On [read]
 * the codec copies the block's entire string payload into one column-owned
 * `byte[]` plus parallel `int[]` offsets/lengths — *no* [String]
 * objects are allocated. A [String] is materialised only when the caller actually
 * reads a row via [getString] / [get], with the decoded value cached. Cells
 * the caller never touches cost nothing beyond the bulk byte copy, which is the key
 * low-allocation lever for string-heavy and column-projected reads.
 *
 * ## Shared-buffer lifetime
 *
 * The column-owned `byte[]` is allocated and filled by [read] via
 * [BinaryReader.readFully]/[BinaryReader.readBytes]; it never aliases a
 * reader scratch buffer that the next block could overwrite. Each block read targets a
 * fresh [StringColumn] (from [allocate]), so a column decoded after a later
 * block was read is unaffected. See `StringColumnCodecTest.lazyDecodeSurvivesSubsequentBlockRead`.
 *
 * Null handling is the responsibility of the block layer (via `NullMaps`); this
 * codec is null-agnostic and will throw a [NullPointerException] if a null element
 * is written.
 *
 * **Thread-safety:** the codec is stateless and shareable; the per-column
 * [StringColumn] backing is single-threaded (one per block) by design.
 */
public class StringColumnCodec : ColumnCodec<StringColumn> {

    // -------------------------------------------------------------------------
    // ColumnCodec contract
    // -------------------------------------------------------------------------

    override fun typeName(): String {
        return TYPE_NAME
    }

    /**
     * Allocates a fresh [StringColumn] holder for [rowCount] rows. The
     * holder owns its own backing buffer; nothing is aliased across blocks.
     *
     * @param rowCount number of rows
     * @return a fresh [StringColumn] of capacity [rowCount]
     */
    override fun allocate(rowCount: Int): StringColumn {
        return StringColumn(rowCount)
    }

    /**
     * Reads [rowCount] string values from the wire into [dest] *without*
     * decoding them. Each value on the wire is a `VarUInt` byte-length then that many
     * UTF-8 bytes (length may be zero). The bytes are copied into one column-owned buffer
     * and indexed by parallel offset/length arrays; decoding is deferred to
     * [getString].
     *
     * @param input    source of bytes
     * @param rowCount number of values to read
     * @param dest     destination holder (capacity >= [rowCount])
     * @throws IOException if the underlying stream throws
     */
    @Throws(IOException::class)
    override fun read(input: BinaryReader, rowCount: Int, dest: StringColumn) {
        val offsets = IntArray(rowCount)
        val lengths = IntArray(rowCount)

        // First pass would require seeking; instead grow a single owned buffer as we read
        // each value, copying wire bytes straight into column-owned storage (never an
        // aliased reader buffer). The per-row estimate (8 bytes/row) is deliberately
        // modest: over-sizing it inflates both the first allocation and every grow-loop
        // intermediate, and — because the grow formula overshoots — pushes the final
        // buffer past the 25% slack threshold below so it gets trimmed anyway, paying for
        // the larger buffer with no copy saved. 8 bytes/row keeps the common case landing
        // within the keep-without-trim band (verified by benchmark: rowCount*16 regressed
        // StringSelectBenchmark allocation ~7%).
        var buf = ByteArray(Math.max(16, rowCount * 8))
        var pos = 0
        for (i in 0 until rowCount) {
            val byteLen = input.readVarUInt().toInt()
            offsets[i] = pos
            lengths[i] = byteLen
            if (byteLen > 0) {
                if (pos + byteLen > buf.size) {
                    var newCap = buf.size
                    while (newCap < pos + byteLen) {
                        newCap = newCap + (newCap shr 1) + byteLen
                    }
                    val grown = ByteArray(newCap)
                    System.arraycopy(buf, 0, grown, 0, pos)
                    buf = grown
                }
                input.readFully(buf, pos, byteLen)
                pos += byteLen
            }
        }
        // Hand the holder its backing buffer. [StringColumn] addresses bytes purely
        // via the parallel offsets/lengths arrays and never inspects `buf.length`, so a
        // buffer with trailing slack is functionally identical to a tight one. We therefore
        // trim only when the slack is large: the grow formula above almost never lands
        // `pos` exactly on `buf.length`, so an unconditional trim copied the whole
        // payload on essentially every column of every block. Skipping it whenever waste is
        // small (≤25% of the buffer) eliminates that copy on the common streaming path while
        // still reclaiming memory for columns that over-allocated badly (e.g. a few long rows
        // tripping the grow loop). `buf` remains a freshly-allocated, holder-private
        // array regardless of branch — it never aliases a reader scratch buffer.
        val owned: ByteArray
        val slack = buf.size - pos
        if (slack <= buf.size / 4) {
            // Exact fit or tolerable slack (≤25%): keep the buffer as-is, no copy.
            owned = buf
        } else {
            // Excessive slack: trim to the exact payload so the retained column is tight.
            owned = ByteArray(pos)
            System.arraycopy(buf, 0, owned, 0, pos)
        }
        dest.initFromWire(owned, offsets, lengths)
    }

    /**
     * Writes the first [rowCount] values of [src] to the wire as a
     * `VarUInt` byte-length followed by UTF-8 bytes. Values that were read from
     * the wire are re-emitted from the original bytes (no re-encoding); explicitly set
     * values are encoded on demand.
     *
     * @param out      destination byte sink
     * @param src      source holder; values `0..rowCount-1` must not be `null`
     * @param rowCount number of values to write
     * @throws IOException if the underlying stream throws
     */
    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: StringColumn, rowCount: Int) {
        for (i in 0 until rowCount) {
            val bytes = src.encodedBytes(i)
            out.writeVarUInt(bytes.size.toLong())
            if (bytes.size > 0) {
                out.writeBytes(bytes, 0, bytes.size)
            }
        }
    }

    /**
     * Returns the element at [row] as a lazily-decoded [String].
     *
     * @param holder the backing [StringColumn]
     * @param row    zero-based row index
     * @return the string value (decoded on demand; `null` only if set so explicitly)
     */
    override fun get(holder: StringColumn, row: Int): Any? {
        return holder.getString(row)
    }

    /**
     * Returns the element at [row] as a [String], decoding lazily from the
     * column-owned byte buffer. Equivalent to [get] but typed.
     *
     * @param holder the backing [StringColumn]
     * @param row    zero-based row index
     * @return the decoded string
     */
    public fun getString(holder: StringColumn, row: Int): String? {
        return holder.getString(row)
    }

    /**
     * Sets the element at [row] from a boxed value.
     *
     * @param holder the backing [StringColumn]
     * @param row    zero-based row index
     * @param value  must be a [String]/[CharSequence] (or `null` — see block-layer note)
     */
    override fun set(holder: StringColumn, row: Int, value: Any?) {
        holder.set(row, value?.toString())
    }

    /**
     * Returns [String]`.class` as the Java element type.
     *
     * @return `String.class`
     */
    override fun javaType(): Class<*> {
        return String::class.java
    }

    private companion object {
        /** Canonical ClickHouse type name. */
        private const val TYPE_NAME = "String"
    }
}
