package io.github.danielbunting.clickhouse

/**
 * An error reported by the ClickHouse server (decoded from an `Exception`
 * packet): a numeric error code, the server's exception class name, the message,
 * and the server-side stack trace.
 *
 * **Contract frozen in W0.2.** Decoding/mapping is task W1.E2 (and W1.D2).
 */
public open class ServerException(
    private val code: Int,
    private val serverExceptionName: String?,
    message: String?,
    private val serverStackTrace: String?,
) : ClickHouseException("[$code] $serverExceptionName: $message") {

    /** ClickHouse numeric error code. */
    public fun code(): Int = code

    public fun serverExceptionName(): String? = serverExceptionName

    public fun serverStackTrace(): String? = serverStackTrace
}
