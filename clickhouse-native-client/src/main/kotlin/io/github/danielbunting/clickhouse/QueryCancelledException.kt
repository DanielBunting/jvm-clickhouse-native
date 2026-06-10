package io.github.danielbunting.clickhouse

/**
 * Thrown when a query is cancelled client-side — either explicitly via
 * [ClickHouseConnection.cancel], or by the client-side query-timeout watchdog
 * (`ClickHouseConfig.queryTimeout` / JDBC `Statement.setQueryTimeout`) firing
 * after the deadline.
 *
 * This signals a *client-initiated* cancel (a `Cancel` packet was sent and the
 * response stream drained). It is distinct from a server-side abort such as
 * `max_execution_time`, which would surface as a [ServerException].
 */
public open class QueryCancelledException : ClickHouseException {

    /** Whether the cancellation was caused by the client-side query-timeout deadline. */
    public val isTimedOut: Boolean

    public constructor(message: String?, timedOut: Boolean) : super(message) {
        this.isTimedOut = timedOut
    }

    public constructor(message: String?, timedOut: Boolean, cause: Throwable?) : super(message, cause) {
        this.isTimedOut = timedOut
    }
}
