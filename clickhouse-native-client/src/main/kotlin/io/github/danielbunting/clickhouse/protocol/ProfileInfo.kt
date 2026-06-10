package io.github.danielbunting.clickhouse.protocol

/**
 * Decoded body of a [ServerPacket.PROFILE_INFO] packet: aggregate
 * statistics about the result the server is streaming.
 *
 * Counters are read as `VarUInt` (returned as `long`, interpreted
 * unsigned); the two boolean flags are read as single `UInt8` bytes.
 *
 * @property rows                       total rows in the result
 * @property blocks                     number of blocks
 * @property bytes                      uncompressed bytes
 * @property appliedLimit               whether a LIMIT was applied
 * @property rowsBeforeLimit            rows before the LIMIT clause was applied
 * @property calculatedRowsBeforeLimit  whether `rowsBeforeLimit` is exact
 */
@JvmRecord
public data class ProfileInfo(
    val rows: Long,
    val blocks: Long,
    val bytes: Long,
    val appliedLimit: Boolean,
    val rowsBeforeLimit: Long,
    val calculatedRowsBeforeLimit: Boolean,
)
