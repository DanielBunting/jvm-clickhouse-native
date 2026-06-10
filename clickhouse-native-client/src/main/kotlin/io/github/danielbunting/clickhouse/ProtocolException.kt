package io.github.danielbunting.clickhouse

/**
 * Thrown when the ClickHouse native binary protocol is violated: an unexpected
 * packet type, a malformed field, an unsupported protocol version, or any other
 * deviation from the expected wire format after a successful TCP connection has
 * been established.
 *
 * This exception is raised by the protocol layer (handshake, packet readers,
 * block codec) when the bytes received from the server do not match what the
 * client expects according to the ClickHouse native protocol specification.
 * It is distinct from [ConnectionException], which covers transport-level
 * failures, and from [ServerException], which carries a well-formed error
 * packet that the server itself sent.
 */
public open class ProtocolException : ClickHouseException {

    /**
     * Constructs a new `ProtocolException` with the given detail message.
     *
     * @param message human-readable description of the protocol violation
     */
    public constructor(message: String?) : super(message)

    /**
     * Constructs a new `ProtocolException` with the given detail message
     * and root cause.
     *
     * @param message human-readable description of the protocol violation
     * @param cause   the underlying exception that triggered the violation
     */
    public constructor(message: String?, cause: Throwable?) : super(message, cause)
}
