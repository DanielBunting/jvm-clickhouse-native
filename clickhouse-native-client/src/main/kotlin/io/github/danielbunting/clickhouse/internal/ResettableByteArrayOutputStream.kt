package io.github.danielbunting.clickhouse.internal

import java.io.ByteArrayOutputStream

/**
 * A [ByteArrayOutputStream] that exposes its internal backing array and byte
 * count so callers can read the staged bytes without the extra defensive copy made
 * by [ByteArrayOutputStream.toByteArray].
 *
 * Used on the compressed block-write path to stage one serialized block, then
 * compress directly from [buffer] / [length] and [reset] for the next block —
 * reusing one growable buffer across the many blocks of an insert instead of
 * allocating a fresh one per block.
 *
 * **Not thread-safe.** Instances are confined to a single [NativeClientImpl]
 * connection whose single-operation guard serializes all block writes, so
 * concurrent access cannot occur. The returned backing array is only valid until
 * the next write or [reset]; callers must consume it before mutating the stream
 * again.
 */
internal class ResettableByteArrayOutputStream(size: Int) : ByteArrayOutputStream(size) {

    /**
     * Returns the internal backing array (not a copy). Only the first
     * [length] bytes are valid staged data.
     */
    fun buffer(): ByteArray {
        return buf
    }

    /** Returns the number of valid bytes currently staged in [buffer]. */
    fun length(): Int {
        return count
    }
}
