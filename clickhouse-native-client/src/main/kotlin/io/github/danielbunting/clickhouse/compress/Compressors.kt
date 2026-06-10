package io.github.danielbunting.clickhouse.compress

/**
 * Factory for [Compressor] / [Decompressor] implementations, selecting
 * the concrete codec by [CompressionMethod] or by on-wire method-marker byte.
 *
 * Markers (see [CompressionMethod]): `NONE=0x00`, `LZ4=0x82`,
 * `ZSTD=0x90`. The block framing (CityHash128 checksum + size headers) is
 * applied by [CompressedBlockCodec]; this class only resolves the codec step.
 *
 * **Task W1.C4.**
 */
public object Compressors {

    /**
     * Returns a [Compressor] for the given method.
     *
     * @param method the compression method
     * @return a compressor implementation
     * @throws IllegalArgumentException if the method is `NONE` or unsupported
     */
    @JvmStatic
    public fun compressor(method: CompressionMethod?): Compressor {
        requireNotNull(method) { "method must not be null" }
        return when (method) {
            CompressionMethod.LZ4 -> Lz4Compressor()
            CompressionMethod.ZSTD -> ZstdCompressor()
            else -> throw IllegalArgumentException("No compressor for method: $method")
        }
    }

    /**
     * Returns a [Decompressor] for the on-wire method-marker byte.
     *
     * @param marker the method-marker byte (low 8 bits significant)
     * @return a decompressor implementation
     * @throws IllegalArgumentException if the marker is unknown / unsupported
     */
    @JvmStatic
    public fun decompressorFor(marker: Int): Decompressor {
        val m = marker and 0xFF
        if (m == CompressionMethod.LZ4.marker) {
            return Lz4Decompressor()
        }
        if (m == CompressionMethod.ZSTD.marker) {
            return ZstdDecompressor()
        }
        throw IllegalArgumentException(String.format("Unknown compression method marker: 0x%02X", m))
    }
}
