package io.github.danielbunting.clickhouse.protocol

import io.github.danielbunting.clickhouse.AuthMethod
import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ProtocolException
import java.io.IOException

/**
 * Implements the ClickHouse native-protocol Hello exchange (the connection handshake).
 *
 * The client sends a [ClientPacket.HELLO] packet announcing its name, version,
 * protocol revision and the target database / credentials. The server replies with a
 * [ServerPacket.HELLO] packet describing its identity and capabilities, which is
 * captured into a [ServerHello].
 *
 * Wire layout (little-endian integers; `VarUInt` = unsigned LEB128;
 * `String` = `VarUInt` length prefix + UTF-8 payload):
 * ```
 * Client Hello:
 *   VarUInt  packet code = HELLO (0)
 *   String   client name
 *   VarUInt  client version major
 *   VarUInt  client version minor
 *   VarUInt  client protocol revision (TCP protocol version)
 *   String   database
 *   String   username
 *   String   password
 *
 * Server Hello:
 *   VarUInt  packet code = HELLO (0)
 *   String   server name
 *   VarUInt  server version major
 *   VarUInt  server version minor
 *   VarUInt  server protocol revision (TCP protocol version)
 *   String   timezone        (if revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
 *   String   display name     (if revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
 *   VarUInt  server version patch (if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
 * ```
 *
 * **Task W1.D1.** Wire-format authority is the CH.Native .NET source; the
 * field order and revision-gating constants below are the standard ClickHouse-native
 * behavior and are marked for verification.
 */
public object Handshake {

    /** Reported client name in the Hello packet. */
    // VERIFY against CH.Native: client name string.
    public const val CLIENT_NAME: String = "clickhouse-native-client"

    /** Client version major reported to the server. */
    public const val CLIENT_VERSION_MAJOR: Int = 24

    /** Client version minor reported to the server. */
    public const val CLIENT_VERSION_MINOR: Int = 3

    /**
     * Client TCP protocol revision advertised in the Hello.
     *
     * This is the headline revision constant; the server gates optional Hello
     * fields (timezone, display name, version patch) on its own revision.
     */
    // VERIFY against CH.Native: DBMS_TCP_PROTOCOL_VERSION value.
    public const val CLIENT_PROTOCOL_REVISION: Int = 54_460

    /** Minimum server revision that includes the timezone string in its Hello. */
    // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE.
    public const val MIN_REVISION_WITH_SERVER_TIMEZONE: Int = 54_058

    /** Minimum server revision that includes the display name in its Hello. */
    // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME.
    public const val MIN_REVISION_WITH_SERVER_DISPLAY_NAME: Int = 54_372

    /** Minimum server revision that includes the version patch in its Hello. */
    // VERIFY against CH.Native: DBMS_MIN_REVISION_WITH_VERSION_PATCH.
    public const val MIN_REVISION_WITH_VERSION_PATCH: Int = 54_401

    /**
     * Special username marker that signals access-token / JWT authentication to the
     * ClickHouse server over the native TCP protocol.
     *
     * The native Hello packet has no dedicated token field; the established
     * convention (used by ClickHouse's own clients for token / JWT auth) is to send
     * this sentinel as the username and carry the token in the password slot. The
     * server recognises the marker and treats the password payload as a bearer token
     * rather than a plaintext password.
     */
    // VERIFY against CH.Native: native-protocol token-auth convention. The Hello
    // packet (Core/Protocol.h) carries no dedicated JWT field on the headline
    // revision we target, so we use the "__api_token__" username + token-in-password
    // convention. If/when CH.Native gains a dedicated handshake-addendum field for
    // JWT, this slot must move there. Token MUST ride over TLS (see feat/tls).
    public const val ACCESS_TOKEN_USER_MARKER: String = "__api_token__"

    /**
     * Writes the client Hello packet to [w] using the credentials and database
     * from [config].
     *
     * Credential placement depends on [ClickHouseConfig.authMethod]:
     * - [AuthMethod.PASSWORD] (default) — the configured username and password are
     *   sent verbatim.
     * - [AuthMethod.ACCESS_TOKEN] — the username is replaced with
     *   [ACCESS_TOKEN_USER_MARKER] and the access token is sent in the password
     *   slot (see that constant's note).
     * - [AuthMethod.CERT] — the configured username is sent with an empty password;
     *   the certificate itself is presented by the TLS transport (feat/tls), not in
     *   this packet.
     *
     * @param w      the binary writer (caller owns flushing)
     * @param config the connection configuration supplying database / credentials
     * @throws IOException if the underlying sink fails
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun sendHello(w: BinaryWriter, config: ClickHouseConfig) {
        w.writeVarUInt(ClientPacket.HELLO.code.toLong())
        w.writeString(CLIENT_NAME)
        w.writeVarUInt(CLIENT_VERSION_MAJOR.toLong())
        w.writeVarUInt(CLIENT_VERSION_MINOR.toLong())
        w.writeVarUInt(CLIENT_PROTOCOL_REVISION.toLong())
        w.writeString(config.database()!!)

        when (config.authMethod()) {
            AuthMethod.ACCESS_TOKEN -> {
                // Token auth: sentinel username + token carried in the password slot.
                // VERIFY against CH.Native (see ACCESS_TOKEN_USER_MARKER).
                w.writeString(ACCESS_TOKEN_USER_MARKER)
                w.writeString(config.accessToken()!!)
            }
            AuthMethod.CERT -> {
                // mTLS identity is presented by the TLS layer (feat/tls); the Hello
                // carries only the username with no password.
                w.writeString(config.username()!!)
                w.writeString("")
            }
            AuthMethod.PASSWORD -> {
                w.writeString(config.username()!!)
                w.writeString(config.password()!!)
            }
        }
    }

    /**
     * Reads a server Hello packet from [r], including the leading packet-code
     * `VarUInt`, and returns the parsed [ServerHello].
     *
     * The optional trailing fields (timezone, display name, version patch) are gated
     * on the server's advertised protocol revision; when absent they default to the empty
     * string / zero so the returned record is always fully populated.
     *
     * @param r the binary reader positioned at the start of the server Hello packet
     * @return the parsed server Hello
     * @throws IOException        if the underlying source fails
     * @throws ProtocolException  if the packet code is not [ServerPacket.HELLO];
     *                            if a [ServerPacket.EXCEPTION] was received instead
     */
    @JvmStatic
    @Throws(IOException::class)
    public fun readHello(r: BinaryReader): ServerHello {
        val packetCode = r.readVarUInt().toInt()
        if (packetCode == ServerPacket.EXCEPTION.code) {
            // Decode and surface the server's actual error rather than a generic message.
            throw ServerPacketReader.readException(r)
        }
        if (packetCode != ServerPacket.HELLO.code) {
            val pkt = ServerPacket.fromCode(packetCode)
            val detail = pkt?.name ?: "unknown($packetCode)"
            throw ProtocolException(
                "Expected server HELLO packet but received $detail (code $packetCode)"
            )
        }

        val name = r.readString()
        val versionMajor = r.readVarUInt()
        val versionMinor = r.readVarUInt()
        val protocolRevision = r.readVarUInt()

        var timezone = ""
        if (protocolRevision >= MIN_REVISION_WITH_SERVER_TIMEZONE) {
            timezone = r.readString()
        }

        var displayName = ""
        if (protocolRevision >= MIN_REVISION_WITH_SERVER_DISPLAY_NAME) {
            displayName = r.readString()
        }

        var versionPatch = 0L
        if (protocolRevision >= MIN_REVISION_WITH_VERSION_PATCH) {
            versionPatch = r.readVarUInt()
        }

        return ServerHello(
            name, versionMajor, versionMinor, versionPatch,
            protocolRevision, timezone, displayName
        )
    }
}
