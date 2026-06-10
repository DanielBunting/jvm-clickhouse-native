package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.ServerException
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.protocol.ProfileInfo
import io.github.danielbunting.clickhouse.protocol.Progress
import io.github.danielbunting.clickhouse.protocol.ServerPacket

/**
 * A decoded server packet returned by [NativeClient.readMessage]. Exactly
 * one payload accessor is populated, determined by [type]:
 *
 *  - [ServerPacket.DATA]/`TOTALS`/`EXTREMES` -> [block]
 *  - [ServerPacket.EXCEPTION] -> [exception]
 *  - [ServerPacket.PROGRESS] -> [progress]
 *  - [ServerPacket.PROFILE_INFO] -> [profileInfo]
 *  - [ServerPacket.END_OF_STREAM]/`PONG` -> no payload
 *
 * **Wave 2.0 frozen internal contract.**
 */
public class ServerMessage private constructor(
    private val type: ServerPacket,
    private val block: Block?,
    private val exception: ServerException?,
    private val progress: Progress?,
    private val profileInfo: ProfileInfo?,
) {

    public fun type(): ServerPacket {
        return type
    }

    public fun block(): Block? {
        return block
    }

    public fun exception(): ServerException? {
        return exception
    }

    public fun progress(): Progress? {
        return progress
    }

    public fun profileInfo(): ProfileInfo? {
        return profileInfo
    }

    public companion object {

        @JvmStatic
        public fun data(block: Block?): ServerMessage {
            return ServerMessage(ServerPacket.DATA, block, null, null, null)
        }

        @JvmStatic
        public fun block(type: ServerPacket, block: Block?): ServerMessage {
            return ServerMessage(type, block, null, null, null)
        }

        @JvmStatic
        public fun exception(exception: ServerException?): ServerMessage {
            return ServerMessage(ServerPacket.EXCEPTION, null, exception, null, null)
        }

        @JvmStatic
        public fun progress(progress: Progress?): ServerMessage {
            return ServerMessage(ServerPacket.PROGRESS, null, null, progress, null)
        }

        @JvmStatic
        public fun profileInfo(profileInfo: ProfileInfo?): ServerMessage {
            return ServerMessage(ServerPacket.PROFILE_INFO, null, null, null, profileInfo)
        }

        /** For payload-less packets (END_OF_STREAM, PONG, ...). */
        @JvmStatic
        public fun of(type: ServerPacket): ServerMessage {
            return ServerMessage(type, null, null, null, null)
        }
    }
}
