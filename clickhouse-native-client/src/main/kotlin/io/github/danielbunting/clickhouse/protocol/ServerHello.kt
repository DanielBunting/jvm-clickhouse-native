package io.github.danielbunting.clickhouse.protocol

/**
 * Server identity and capabilities returned by the handshake (server Hello).
 * The `timezone` is authoritative for decoding `DateTime` columns.
 *
 * **Contract frozen in W0.2.** Populated by task W1.D1.
 */
@JvmRecord
public data class ServerHello(
    val name: String?,
    val versionMajor: Long,
    val versionMinor: Long,
    val versionPatch: Long,
    val protocolRevision: Long,
    val timezone: String?,
    val displayName: String?,
)
