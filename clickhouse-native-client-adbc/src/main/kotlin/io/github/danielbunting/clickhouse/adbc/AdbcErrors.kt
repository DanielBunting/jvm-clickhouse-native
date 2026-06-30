package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.UnsupportedTypeException
import org.apache.arrow.adbc.core.AdbcException
import org.apache.arrow.adbc.core.AdbcStatusCode

/** Small helpers for mapping core failures onto [AdbcException] status codes. */
internal object AdbcErrors {

    /** Wraps an I/O / protocol failure as an [AdbcStatusCode.IO] error. */
    fun io(message: String, cause: Throwable? = null): AdbcException =
        AdbcException.io(message).let { if (cause != null) it.withCause(cause) else it }

    /** A cancellation surfaced as [AdbcStatusCode.CANCELLED]. */
    fun cancelled(message: String, cause: Throwable? = null): AdbcException =
        AdbcException(message, cause, AdbcStatusCode.CANCELLED, null, 0)

    fun notImplemented(message: String): AdbcException = AdbcException.notImplemented(message)

    fun notFound(message: String): AdbcException =
        AdbcException(message, null, AdbcStatusCode.NOT_FOUND, null, 0)

    /**
     * Funnels an arbitrary failure into an [AdbcException]: an existing [AdbcException] passes
     * through; an unsupported ClickHouse type ([UnsupportedTypeException], e.g. an `AggregateFunction`
     * state) or an unsupported operation becomes [AdbcStatusCode.NOT_IMPLEMENTED]; anything else
     * becomes [AdbcStatusCode.IO]. Keeps the ADBC error contract at one boundary.
     */
    fun wrap(message: String, cause: Throwable): AdbcException = when (cause) {
        is AdbcException -> cause
        is UnsupportedTypeException -> AdbcException.notImplemented("$message: ${cause.message}").withCause(cause)
        is UnsupportedOperationException -> AdbcException.notImplemented("$message: ${cause.message}").withCause(cause)
        else -> io("$message: ${cause.message}", cause)
    }

    fun invalidState(message: String): AdbcException = AdbcException.invalidState(message)

    fun invalidArgument(message: String): AdbcException = AdbcException.invalidArgument(message)
}
