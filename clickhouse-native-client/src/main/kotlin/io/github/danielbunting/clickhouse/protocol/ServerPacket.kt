package io.github.danielbunting.clickhouse.protocol

/**
 * Server → client packet type codes (native TCP protocol).
 *
 * **Contract frozen in W0.2.** Confirm codes against the CH.Native .NET source.
 */
public enum class ServerPacket(
    /** On-wire `VarUInt` code. */
    @JvmField public val code: Int,
) {
    HELLO(0),
    DATA(1),
    EXCEPTION(2),
    PROGRESS(3),
    PONG(4),
    END_OF_STREAM(5),
    PROFILE_INFO(6),
    TOTALS(7),
    EXTREMES(8),
    TABLES_STATUS_RESPONSE(9),
    LOG(10),
    TABLE_COLUMNS(11),
    PART_UUIDS(12),
    READ_TASK_REQUEST(13),
    PROFILE_EVENTS(14),
    ;

    public companion object {

        /** Resolves a wire code to its enum constant, or `null` if unknown. */
        @JvmStatic
        public fun fromCode(code: Int): ServerPacket? {
            for (p in entries) {
                if (p.code == code) {
                    return p
                }
            }
            return null
        }
    }
}
