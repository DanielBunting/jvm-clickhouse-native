package io.github.danielbunting.clickhouse.compress

/**
 * Block compression methods and their on-wire method-marker bytes. The marker is
 * the first byte of the compressed-block payload that follows the 16-byte
 * CityHash128 checksum.
 *
 * **Contract frozen in W0.2.** Verify marker bytes against the CH.Native
 * .NET source. Default method is [LZ4] (matches CH.Native).
 */
public enum class CompressionMethod(
    /** On-wire method-marker byte. */
    @JvmField public val marker: Int,
) {
    NONE(0x00),
    LZ4(0x82),
    ZSTD(0x90),
}
