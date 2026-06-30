package io.github.danielbunting.clickhouse

/**
 * Signals that the client recognises a ClickHouse type but cannot decode it (e.g. an opaque
 * `AggregateFunction(...)` aggregation state), or that the type name is unknown to this client.
 *
 * Distinct from a malformed-type-string parse error (which stays a plain [ClickHouseException]):
 * this is a *supported-type-boundary* signal, so callers — notably the ADBC bridge — can surface it
 * as "not implemented" rather than a generic I/O failure. Subclasses [ClickHouseException] so existing
 * `catch (ClickHouseException)` handlers keep working unchanged.
 */
public class UnsupportedTypeException : ClickHouseException {
    public constructor(message: String?) : super(message)
    public constructor(message: String?, cause: Throwable?) : super(message, cause)
}
