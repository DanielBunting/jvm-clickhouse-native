package io.github.danielbunting.clickhouse.types.codec

import java.nio.charset.StandardCharsets

/**
 * Column-owned backing store for a ClickHouse `String` column that defers
 * UTF-8 decoding to access time.
 *
 * Instead of materialising one [String] per row at decode time, the
 * [StringColumnCodec] reads a block's raw UTF-8 payload once into a single
 * column-owned `byte[]` and records parallel `int[]` offsets/lengths.
 * A [String] is only allocated when [getString] is actually
 * called, so cells the caller never reads cost nothing beyond the bulk byte copy.
 *
 * ## Lifetime / aliasing safety
 *
 * The [bytes] array is **owned by this holder**. [StringColumnCodec.read]
 * copies the wire bytes into a freshly-allocated, holder-private array; it never
 * aliases a reader's scratch buffer. Each block read produces a new `StringColumn`
 * (via [StringColumnCodec.allocate]), so decoding a column remains valid even
 * after a subsequent block has been read into a different holder.
 *
 * ## Dual representation
 *
 * To serve both the read path (lazy decode from [bytes]) and the write
 * path / object-mapper `set` (store a `String` directly), the holder
 * keeps a lazily-allocated `String[]` overlay:
 *
 *  - [set] writes into the overlay (write path, `Array`/`Nullable`
 *    flattening, object mapper).
 *  - [getString] returns an overlay entry if present (set value or a previously
 *    decoded+cached value), otherwise decodes lazily from [bytes] and caches
 *    the result in the overlay.
 *  - [encodedBytes] returns, for a row, the UTF-8 bytes to write: the raw slice
 *    when it came from the wire, or a freshly encoded slice when the value was set.
 *
 * Not thread-safe; a column is single-threaded and reused per block by design.
 */
public class StringColumn internal constructor(
    /** Number of rows this holder can address. */
    private val rowCount: Int
) {

    /**
     * Single column-owned buffer holding all rows' UTF-8 payloads back-to-back.
     * Never aliases an external/reader buffer. May be `null` before a read
     * (e.g. a write-only holder produced by `allocate`).
     */
    private var bytes: ByteArray? = null

    /** Start offset into [bytes] for each row (valid only for wire-read rows). */
    private var offsets: IntArray? = null

    /** Byte length within [bytes] for each row (valid only for wire-read rows). */
    private var lengths: IntArray? = null

    /**
     * Lazily-allocated overlay of decoded / explicitly-set [String] values.
     * A non-null entry at `row` takes precedence over the [bytes] slice
     * and doubles as the lazy-decode cache. `null` until first needed.
     */
    private var overlay: Array<String?>? = null

    /** Marks a row whose authoritative value is the overlay entry (even if that entry is `null`). */
    private var overlaySet: BooleanArray? = null

    /** @return the number of rows this holder addresses */
    public fun rowCount(): Int {
        return rowCount
    }

    /**
     * Installs the column-owned wire payload and its parallel offset/length arrays.
     * The `bytes` array must be owned by this holder (a fresh copy of the wire
     * bytes), never an aliased reader buffer.
     *
     * @param bytes   column-owned UTF-8 payload (all rows concatenated)
     * @param offsets per-row start offsets into `bytes`
     * @param lengths per-row byte lengths
     */
    public fun initFromWire(bytes: ByteArray, offsets: IntArray, lengths: IntArray) {
        this.bytes = bytes
        this.offsets = offsets
        this.lengths = lengths
    }

    /**
     * Returns the value at `row` as a [String], decoding lazily from the
     * column-owned byte buffer on first access and caching the result.
     *
     * @param row zero-based row index
     * @return the decoded string; `null` only if explicitly set to `null`
     */
    public fun getString(row: Int): String? {
        val overlaySet = this.overlaySet
        var overlay = this.overlay
        if (overlaySet != null && overlaySet[row]) {
            return overlay!![row]
        }
        if (overlay != null && overlay[row] != null) {
            return overlay[row]
        }
        val lengths = this.lengths
        val len = if (lengths == null) 0 else lengths[row]
        val decoded = if (len == 0) {
            ""
        } else {
            String(bytes!!, offsets!![row], len, StandardCharsets.UTF_8)
        }
        if (overlay == null) {
            overlay = arrayOfNulls(rowCount)
            this.overlay = overlay
        }
        overlay[row] = decoded // cache
        return decoded
    }

    /**
     * Stores an explicit [String] (or `null`) at `row`, used by the
     * write path, container flattening, and the object mapper.
     *
     * @param row   zero-based row index
     * @param value the value to store (may be `null`)
     */
    public fun set(row: Int, value: String?) {
        var overlay = this.overlay
        if (overlay == null) {
            overlay = arrayOfNulls(rowCount)
            this.overlay = overlay
        }
        var overlaySet = this.overlaySet
        if (overlaySet == null) {
            overlaySet = BooleanArray(rowCount)
            this.overlaySet = overlaySet
        }
        overlay[row] = value
        overlaySet[row] = true
    }

    /**
     * Returns the UTF-8 bytes to serialise for `row`. When the value was set
     * explicitly (overlay), it is encoded on demand; otherwise the original wire slice
     * is returned without re-encoding.
     *
     * @param row zero-based row index
     * @return a fresh `byte[]` of the value's UTF-8 encoding (length may be 0)
     * @throws NullPointerException if the row's authoritative value is `null`
     */
    public fun encodedBytes(row: Int): ByteArray {
        val overlaySet = this.overlaySet
        val overlay = this.overlay
        if (overlaySet != null && overlaySet[row]) {
            val v = overlay!![row]
                ?: throw NullPointerException(
                    "Cannot encode null String at row " + row
                        + "; null handling belongs to the block/null-map layer"
                )
            return v.toByteArray(StandardCharsets.UTF_8)
        }
        if (overlay != null && overlay[row] != null) {
            return overlay[row]!!.toByteArray(StandardCharsets.UTF_8)
        }
        val bytes = this.bytes
        val lengths = this.lengths
        if (bytes != null && lengths != null) {
            val len = lengths[row]
            val out = ByteArray(len)
            if (len > 0) {
                System.arraycopy(bytes, offsets!![row], out, 0, len)
            }
            return out
        }
        // Never-set slot: occurs at the null positions of a nested Nullable(String)
        // (NullableColumnCodec leaves those inner slots unset and masks them via the
        // null-map). ClickHouse stores the type default ("") at null positions, so emit an
        // empty string — the null-map, not this value, marks the row NULL. (For a non-nullable
        // String column every row is set before write, so this branch is only the Nullable path.)
        return EMPTY_BYTES
    }

    /**
     * Materialises this column into a plain `String[]` for callers that require a
     * fully-decoded array (backward-compatible `values()` consumers, the mapper).
     *
     * @return a new `String[]` of length [rowCount] with every row decoded
     */
    public fun toStringArray(): Array<String?> {
        val out = arrayOfNulls<String>(rowCount)
        for (i in 0 until rowCount) {
            out[i] = getString(i)
        }
        return out
    }

    private companion object {
        private val EMPTY_BYTES = ByteArray(0)
    }
}
