package io.github.danielbunting.clickhouse.protocol

/**
 * Client → server packet type codes (native TCP protocol).
 *
 * **Contract frozen in W0.2.** Confirm codes against the CH.Native .NET source.
 */
public enum class ClientPacket(
    /** On-wire `VarUInt` code. */
    @JvmField public val code: Int,
) {
    HELLO(0),
    QUERY(1),
    DATA(2),
    CANCEL(3),
    PING(4),
    TABLES_STATUS_REQUEST(5),
}
