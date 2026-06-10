package io.github.danielbunting.clickhouse.compress

import net.jpountz.lz4.LZ4Factory
import net.jpountz.lz4.LZ4FastDecompressor

/**
 * [Decompressor] implementation that uses the LZ4 raw-block format as
 * produced by the ClickHouse native protocol.
 *
 * ClickHouse compresses blocks using the raw LZ4 format and stores the
 * uncompressed size in the block header. The caller supplies that size to
 * [decompress] so the destination buffer can be pre-allocated exactly.
 *
 * Uses [LZ4FastDecompressor], which requires knowing the uncompressed
 * size in advance but is significantly faster than the safe decompressor because
 * it can stop as soon as the destination buffer is full rather than scanning to
 * the end of the compressed stream.
 *
 * The factory prefers the native (JNI) implementation for performance and
 * falls back to the safe Java implementation automatically via
 * [LZ4Factory.fastestInstance].
 */
public class Lz4Decompressor : Decompressor {

    private val delegate: LZ4FastDecompressor = FACTORY.fastDecompressor()

    override fun method(): CompressionMethod = CompressionMethod.LZ4

    /**
     * Decompresses [length] bytes from [src] starting at
     * [offset] into a new byte array of exactly [uncompressedSize]
     * bytes.
     *
     * Uses `LZ4FastDecompressor.decompress(byte[], int, byte[], int, int)`
     * which requires the exact uncompressed size. The uncompressed size is read
     * from the block header by the block-compression layer before this method is
     * called.
     *
     * @param src              source byte array containing the compressed data
     * @param offset           start offset within [src]
     * @param length           number of compressed bytes (not used directly — the
     *                         fast decompressor stops once [uncompressedSize]
     *                         bytes have been written)
     * @param uncompressedSize the exact size of the decompressed output, as read
     *                         from the compressed block header
     * @return a new byte array of length [uncompressedSize] containing the
     *         decompressed data
     */
    override fun decompress(src: ByteArray, offset: Int, length: Int, uncompressedSize: Int): ByteArray {
        if (uncompressedSize == 0) {
            return ByteArray(0)
        }
        val dest = ByteArray(uncompressedSize)
        // LZ4FastDecompressor ignores the compressed length; it stops when
        // uncompressedSize bytes have been written. The 'length' parameter is
        // retained in the signature for symmetry with the Decompressor contract
        // and for potential future validation.
        // VERIFY against CH.Native: confirm raw LZ4 block (not frame) is used.
        delegate.decompress(src, offset, dest, 0, uncompressedSize)
        return dest
    }

    private companion object {
        private val FACTORY: LZ4Factory = LZ4Factory.fastestInstance()
    }
}
