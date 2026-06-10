package io.github.danielbunting.clickhouse.compress

/**
 * Compresses raw block bytes for a single [CompressionMethod]. The framing
 * (CityHash128 checksum + method byte + size headers) is applied by the block
 * compression layer (W1.C4), not here — this interface only does the codec step.
 *
 * **Contract frozen in W0.2.** Implementations: W1.C2 (LZ4), W1.C3 (ZSTD).
 */
public interface Compressor {

    public fun method(): CompressionMethod

    /** Compresses [length] bytes of [src] starting at [offset]. */
    public fun compress(src: ByteArray, offset: Int, length: Int): ByteArray
}
