package io.github.danielbunting.clickhouse.compress

import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import java.io.EOFException
import java.io.IOException

/**
 * Reads and writes a single ClickHouse compressed block (frame).
 *
 * On-wire layout of one compressed block:
 * ```
 *   +-----------------------------+----------+-----------------+-------------------+-----------+
 *   | CityHash128 checksum (16 B) | method   | compressed_size | uncompressed_size | payload   |
 *   |                             | (1 byte) | (u32, LE)       | (u32, LE)         | (N bytes) |
 *   +-----------------------------+----------+-----------------+-------------------+-----------+
 * ```
 *
 * - The 16-byte CityHash128 checksum covers the bytes
 *   `[method | compressed_size | uncompressed_size | payload]` — that is, the
 *   9-byte header plus the compressed payload (everything after the checksum).
 * - `compressed_size` is the size of `[method | compressed_size |
 *   uncompressed_size | payload]` taken together, i.e. `9 + payload.length`
 *   (it INCLUDES the 9-byte header, matching ClickHouse / CH.Native).
 * - `uncompressed_size` is the length of the original raw block.
 *
 * // VERIFY against CH.Native: (1) the checksum is computed over the 9-byte header +
 * // compressed payload (the `compressed_size` bytes following the checksum);
 * // (2) `compressed_size` on the wire INCLUDES the 9-byte header
 * // (`1 + 4 + 4`); (3) CityHash128 byte order is the little-endian layout that
 * // ClickHouse emits (delegated to [CityHash128.hash128]).
 *
 * **Task W1.C4.**
 */
public object CompressedBlockCodec {

    /** Size of the per-block header that precedes the payload: method (1) + two u32 sizes (8). */
    private const val HEADER_SIZE = 1 + 4 + 4

    /** Size of the CityHash128 checksum. */
    private const val CHECKSUM_SIZE = 16

    /**
     * Compresses [rawBlock] with [compressor] and writes the framed
     * compressed block (checksum + header + payload) to [out].
     *
     * @param out        sink for the framed block
     * @param rawBlock   the uncompressed block bytes
     * @param compressor codec used to produce the payload
     * @throws IOException if the underlying writer fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun write(out: BinaryWriter, rawBlock: ByteArray, compressor: Compressor) {
        write(out, rawBlock, 0, rawBlock.size, compressor)
    }

    /**
     * Compresses [length] bytes of [rawBlock] starting at [offset]
     * with [compressor] and writes the framed compressed block
     * (checksum + header + payload) to [out].
     *
     * This offset/length overload lets callers serialize a block into a reused
     * staging buffer and compress directly from that buffer's backing array without
     * an intermediate `toByteArray()` copy. The bytes placed on the wire are
     * identical to [write] called with a trimmed copy of the same region.
     *
     * @param out        sink for the framed block
     * @param rawBlock   the array holding the uncompressed block bytes
     * @param offset     start offset within [rawBlock]
     * @param length     number of uncompressed bytes to compress
     * @param compressor codec used to produce the payload
     * @throws IOException if the underlying writer fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun write(
        out: BinaryWriter,
        rawBlock: ByteArray,
        offset: Int,
        length: Int,
        compressor: Compressor,
    ) {
        if (offset < 0 || length < 0 || offset + length > rawBlock.size) {
            throw IndexOutOfBoundsException(
                "offset=$offset, length=$length, rawBlock.length=${rawBlock.size}"
            )
        }
        val payload = compressor.compress(rawBlock, offset, length)

        val compressedSize = HEADER_SIZE + payload.size
        val uncompressedSize = length

        // Build the checksummed region: [method | compressed_size | uncompressed_size | payload].
        val checksummed = ByteArray(compressedSize)
        var p = 0
        checksummed[p++] = compressor.method().marker.toByte()
        p = putLeU32(checksummed, p, compressedSize)
        p = putLeU32(checksummed, p, uncompressedSize)
        System.arraycopy(payload, 0, checksummed, p, payload.size)

        val checksum = CityHash128.hash128(checksummed, 0, checksummed.size)
        if (checksum.size != CHECKSUM_SIZE) {
            throw ClickHouseException("CityHash128 must return a 16-byte checksum")
        }

        out.writeBytes(checksum, 0, CHECKSUM_SIZE)
        out.writeBytes(checksummed, 0, checksummed.size)
    }

    /**
     * Reads one framed compressed block from [in], verifies its CityHash128
     * checksum, and returns the decompressed (raw) block bytes.
     *
     * **Note:** this reads exactly ONE compressed frame. A single native data
     * block may be split by the server into multiple consecutive frames once its
     * uncompressed serialization exceeds the per-frame limit (~1 MiB); to read a
     * whole block transparently across frame boundaries use
     * [CompressedFrameInputStream], which calls [readFrameOrNull] on
     * demand. This single-frame entry point is retained for unit tests and for
     * callers that genuinely want one frame.
     *
     * @param in source of the framed block
     * @return the decompressed block bytes (`uncompressed_size` long)
     * @throws IOException        if the underlying reader fails
     * @throws ClickHouseException if the checksum does not match or sizes are invalid
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun read(`in`: BinaryReader): ByteArray {
        return readFrameOrNull(`in`)
            ?: throw EOFException("Unexpected end of stream before a compressed frame header")
    }

    /**
     * Reads one framed compressed frame from [in], verifies its CityHash128
     * checksum, and returns the decompressed bytes — or `null` if the reader is
     * cleanly at end-of-stream before any frame byte (no partial header consumed).
     *
     * This is the per-frame primitive used by [CompressedFrameInputStream]
     * to reassemble multi-frame blocks. Once the first checksum byte has been read a
     * truncated frame is reported as an [java.io.EOFException] by the underlying
     * reader rather than as `null`.
     *
     * @param in source of the framed frame
     * @return the decompressed frame bytes (`uncompressed_size` long), or
     *         `null` at clean end-of-stream
     * @throws IOException        if the underlying reader fails
     * @throws ClickHouseException if the checksum does not match or sizes are invalid
     */
    @JvmStatic
    @Throws(IOException::class)
    internal fun readFrameOrNull(`in`: BinaryReader): ByteArray? {
        val first = `in`.readByteOrEof()
        if (first < 0) {
            return null
        }
        val expectedChecksum = ByteArray(CHECKSUM_SIZE)
        expectedChecksum[0] = first.toByte()
        `in`.readFully(expectedChecksum, 1, CHECKSUM_SIZE - 1)

        val method = `in`.readByteUnsigned()
        val compressedSizeU = `in`.readUInt32()
        val uncompressedSizeU = `in`.readUInt32()

        if (compressedSizeU < HEADER_SIZE || compressedSizeU > Integer.MAX_VALUE) {
            throw ClickHouseException("Invalid compressed_size: $compressedSizeU")
        }
        if (uncompressedSizeU < 0 || uncompressedSizeU > Integer.MAX_VALUE) {
            throw ClickHouseException("Invalid uncompressed_size: $uncompressedSizeU")
        }

        val compressedSize = compressedSizeU.toInt()
        val uncompressedSize = uncompressedSizeU.toInt()
        val payloadLen = compressedSize - HEADER_SIZE

        // Read the header + compressed payload into one buffer laid out as the
        // checksummed region [method | compressed_size | uncompressed_size | payload],
        // so we can hash and decompress in place without a second full-frame copy.
        val framed = ByteArray(compressedSize)
        framed[0] = method.toByte()
        putLeU32(framed, 1, compressedSize)
        putLeU32(framed, 5, uncompressedSize)
        `in`.readFully(framed, HEADER_SIZE, payloadLen)

        val actualChecksum = CityHash128.hash128(framed, 0, framed.size)
        if (!expectedChecksum.contentEquals(actualChecksum)) {
            throw ClickHouseException(
                "Compressed block checksum mismatch: expected " +
                    toHex(expectedChecksum) + " but computed " + toHex(actualChecksum)
            )
        }

        val decompressor = Compressors.decompressorFor(method)
        return decompressor.decompress(framed, HEADER_SIZE, payloadLen, uncompressedSize)
    }

    private fun putLeU32(dest: ByteArray, off: Int, value: Int): Int {
        dest[off] = (value and 0xFF).toByte()
        dest[off + 1] = ((value ushr 8) and 0xFF).toByte()
        dest[off + 2] = ((value ushr 16) and 0xFF).toByte()
        dest[off + 3] = ((value ushr 24) and 0xFF).toByte()
        return off + 4
    }

    private fun toHex(b: ByteArray): String {
        val sb = StringBuilder(b.size * 2)
        for (v in b) {
            sb.append(Character.forDigit((v.toInt() shr 4) and 0xF, 16))
            sb.append(Character.forDigit(v.toInt() and 0xF, 16))
        }
        return sb.toString()
    }
}
