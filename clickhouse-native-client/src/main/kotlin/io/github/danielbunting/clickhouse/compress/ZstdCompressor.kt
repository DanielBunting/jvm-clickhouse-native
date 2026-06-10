package io.github.danielbunting.clickhouse.compress

import com.github.luben.zstd.Zstd

/**
 * [Compressor] implementation that uses the Zstandard algorithm via
 * `com.github.luben:zstd-jni`.
 *
 * Produces a standard Zstd frame (not seekable / skippable-frame extras).
 * The framing wrapper (CityHash128 checksum + method marker + size headers) is
 * applied by the block-codec layer (task C4, [CompressedBlockCodec]), not
 * here. This class is purely responsible for the codec step.
 *
 * The default Zstd compression level ([Zstd.defaultCompressionLevel])
 * is used, which balances speed and ratio in a manner consistent with the
 * CH.Native .NET defaults.
 */
public class ZstdCompressor : Compressor {

    /**
     * Reused worst-case destination buffer, grown on demand. Compressing into a
     * single per-instance scratch buffer avoids a fresh `byte[compressBound]`
     * allocation on every block.
     *
     * **Not thread-safe:** a `ZstdCompressor` carrying this mutable
     * buffer must be confined to one connection. `NativeClientImpl` holds one
     * compressor per connection and serializes all writes behind its
     * single-operation guard, so concurrent [compress] calls cannot occur.
     */
    private var scratch: ByteArray = ByteArray(0)

    override fun method(): CompressionMethod = CompressionMethod.ZSTD

    /**
     * Compresses [length] bytes of [src] starting at [offset]
     * into a standard Zstd frame.
     *
     * @param src    source byte array
     * @param offset start offset within [src]
     * @param length number of bytes to compress
     * @return compressed bytes as a new array
     * @throws IllegalArgumentException if the Zstd library reports a compression error
     */
    override fun compress(src: ByteArray, offset: Int, length: Int): ByteArray {
        // Size the reused worst-case output buffer using Zstd's compressBound.
        val maxCompressed = Zstd.compressBound(length.toLong()).toInt()
        if (scratch.size < maxCompressed) {
            scratch = ByteArray(maxCompressed)
        }
        val level = Zstd.defaultCompressionLevel()
        val written = Zstd.compressByteArray(scratch, 0, maxCompressed, src, offset, length, level)
        if (Zstd.isError(written)) {
            throw IllegalArgumentException("Zstd compression failed: " + Zstd.getErrorName(written))
        }
        // Return an exact-length copy: the scratch buffer is reused on the next call,
        // and the framing layer relies on payload.length being the true compressed size.
        val result = ByteArray(written.toInt())
        System.arraycopy(scratch, 0, result, 0, written.toInt())
        return result
    }
}
