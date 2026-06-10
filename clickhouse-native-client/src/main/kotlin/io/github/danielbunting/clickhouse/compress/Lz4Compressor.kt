package io.github.danielbunting.clickhouse.compress

import net.jpountz.lz4.LZ4Factory

/**
 * [Compressor] implementation that uses the LZ4 raw-block format as
 * produced by the ClickHouse native protocol.
 *
 * ClickHouse transmits LZ4-compressed data as a raw LZ4 block (not an LZ4
 * frame). The framing — 16-byte CityHash128 checksum, method marker byte, and
 * the two 4-byte size fields — is written by the block-compression layer
 * ([CompressedBlockCodec], task C4). This class performs only the codec
 * step: plain `LZ4Compressor.compress`.
 *
 * The factory prefers the native (JNI) implementation for performance and
 * falls back to the safe Java implementation automatically via
 * [LZ4Factory.fastestInstance].
 */
public class Lz4Compressor : Compressor {

    private val delegate: net.jpountz.lz4.LZ4Compressor = FACTORY.fastCompressor()

    /**
     * Reused worst-case destination buffer, grown on demand. Compressing into a
     * single per-instance scratch buffer avoids a fresh `byte[maxCompressed]`
     * allocation on every block.
     *
     * **Not thread-safe:** an `Lz4Compressor` carrying this mutable
     * buffer must be confined to one connection. `NativeClientImpl` holds one
     * compressor per connection and serializes all writes behind its
     * single-operation guard, so concurrent [compress] calls cannot occur.
     */
    private var scratch: ByteArray = ByteArray(0)

    override fun method(): CompressionMethod = CompressionMethod.LZ4

    /**
     * Compresses [length] bytes from [src] starting at [offset]
     * using the LZ4 raw-block format.
     *
     * The output array is sized using
     * `LZ4Compressor.maxCompressedLength(int)` to guarantee sufficient
     * capacity. The returned array is trimmed to the actual compressed length so
     * callers receive an exact-length result.
     *
     * @param src    source byte array
     * @param offset start offset within [src]
     * @param length number of bytes to compress
     * @return a new byte array containing the compressed data, sized exactly to
     *         the compressed output (may be larger than [length] for
     *         incompressible data — LZ4 does not expand output beyond the
     *         maximum compressed bound)
     */
    override fun compress(src: ByteArray, offset: Int, length: Int): ByteArray {
        if (length == 0) {
            return ByteArray(0)
        }
        val maxCompressed = delegate.maxCompressedLength(length)
        if (scratch.size < maxCompressed) {
            scratch = ByteArray(maxCompressed)
        }
        val compressedLen = delegate.compress(src, offset, length, scratch, 0, maxCompressed)
        // Return an exact-length copy: the scratch buffer is reused on the next call,
        // and the framing layer relies on payload.length being the true compressed size.
        val trimmed = ByteArray(compressedLen)
        System.arraycopy(scratch, 0, trimmed, 0, compressedLen)
        return trimmed
    }

    private companion object {
        private val FACTORY: LZ4Factory = LZ4Factory.fastestInstance()
    }
}
