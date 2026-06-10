package io.github.danielbunting.clickhouse

/**
 * Base unchecked exception for all client-side failures (connection, protocol,
 * codec, configuration). Server-reported errors use [ServerException].
 *
 * **Contract frozen in W0.2.** Hierarchy fleshed out in task W1.E2.
 */
public open class ClickHouseException : RuntimeException {

    public constructor(message: String?) : super(message)

    public constructor(message: String?, cause: Throwable?) : super(message, cause)
}
