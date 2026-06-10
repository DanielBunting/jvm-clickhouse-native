package io.github.danielbunting.clickhouse

/**
 * Thrown when the client cannot establish or maintain a TCP connection to the
 * ClickHouse server. Typical causes include an unreachable host, a refused
 * connection, a socket timeout, or an unexpected connection reset during an
 * active query.
 *
 * This exception wraps the underlying [java.io.IOException] when the
 * root cause is an I/O failure at the transport layer, and is thrown before any
 * ClickHouse protocol bytes have been exchanged (or after the connection is lost
 * mid-conversation). For errors that occur after a successful handshake and
 * involve recognisable protocol violations, see [ProtocolException].
 */
public open class ConnectionException : ClickHouseException {

    /**
     * Constructs a new `ConnectionException` with the given detail message.
     *
     * @param message human-readable description of the connection failure
     */
    public constructor(message: String?) : super(message)

    /**
     * Constructs a new `ConnectionException` with the given detail message
     * and root cause.
     *
     * @param message human-readable description of the connection failure
     * @param cause   the underlying I/O or network exception
     */
    public constructor(message: String?, cause: Throwable?) : super(message, cause)
}
