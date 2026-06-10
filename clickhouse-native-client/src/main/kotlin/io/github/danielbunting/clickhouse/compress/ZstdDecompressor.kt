package io.github.danielbunting.clickhouse.compress

import com.github.luben.zstd.Zstd

/**
 * [Decompressor] implementation that expands a standard Zstd frame back to
 * the original block bytes, via `com.github.luben:zstd-jni`.
 *
 * The caller supplies the known uncompressed size (read from the block header by
 * the block-codec layer, task C4) so that the destination buffer can be sized
 * exactly without a streaming pass.
 */
public class ZstdDecompressor : Decompressor {

    override fun method(): CompressionMethod = CompressionMethod.ZSTD

    /**
     * Decompresses [length] bytes of [src] starting at [offset]
     * into exactly [uncompressedSize] bytes.
     *
     * @param src              source array containing the compressed Zstd frame
     * @param offset           start offset within [src]
     * @param length           number of compressed bytes to read
     * @param uncompressedSize expected number of bytes after decompression
     * @return decompressed bytes as a new array of length [uncompressedSize]
     * @throws IllegalArgumentException if Zstd reports a decompression error or
     *                                  the actual decompressed size does not match
     *                                  [uncompressedSize]
     */
    override fun decompress(src: ByteArray, offset: Int, length: Int, uncompressedSize: Int): ByteArray {
        val dst = ByteArray(uncompressedSize)
        val written = Zstd.decompressByteArray(dst, 0, uncompressedSize, src, offset, length)
        if (Zstd.isError(written)) {
            throw IllegalArgumentException("Zstd decompression failed: " + Zstd.getErrorName(written))
        }
        if (written.toInt() != uncompressedSize) {
            throw IllegalArgumentException(
                "Zstd decompressed $written bytes but expected $uncompressedSize"
            )
        }
        return dst
    }
}
