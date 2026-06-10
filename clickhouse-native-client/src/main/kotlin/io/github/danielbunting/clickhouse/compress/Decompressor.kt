package io.github.danielbunting.clickhouse.compress

/**
 * Inverse of [Compressor]: expands compressed payload bytes back to the
 * original block bytes. The caller supplies the known uncompressed size (read
 * from the block header) so implementations can size the destination exactly.
 *
 * **Contract frozen in W0.2.** Implementations: W1.C2 (LZ4), W1.C3 (ZSTD).
 */
public interface Decompressor {

    public fun method(): CompressionMethod

    /** Decompresses [length] bytes of [src] from [offset] into [uncompressedSize] bytes. */
    public fun decompress(src: ByteArray, offset: Int, length: Int, uncompressedSize: Int): ByteArray
}
