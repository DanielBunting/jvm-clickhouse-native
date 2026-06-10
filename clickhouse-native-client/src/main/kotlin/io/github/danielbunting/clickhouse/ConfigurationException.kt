package io.github.danielbunting.clickhouse

/**
 * Thrown when a [ClickHouseConfig] value is invalid or missing. Typical
 * causes include a malformed connection URL, an unrecognised compression method
 * name, a negative or out-of-range numeric setting, or a required field that
 * was not supplied.
 *
 * This exception is raised during configuration parsing (e.g.
 * `ClickHouseConfig.fromUrl`) and builder validation, before any network
 * activity takes place. It signals a programmer or operator error that must be
 * corrected at the call site rather than retried at runtime.
 */
public open class ConfigurationException : ClickHouseException {

    /**
     * Constructs a new `ConfigurationException` with the given detail
     * message.
     *
     * @param message human-readable description of the configuration problem
     */
    public constructor(message: String?) : super(message)

    /**
     * Constructs a new `ConfigurationException` with the given detail
     * message and root cause.
     *
     * @param message human-readable description of the configuration problem
     * @param cause   the underlying exception (e.g. a parse error)
     */
    public constructor(message: String?, cause: Throwable?) : super(message, cause)
}
