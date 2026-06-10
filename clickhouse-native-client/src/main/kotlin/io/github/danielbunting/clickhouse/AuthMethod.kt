package io.github.danielbunting.clickhouse

/**
 * The credential mechanism a [ClickHouseConfig] authenticates with.
 *
 * The method is *derived* from which credential fields are set on the
 * config (see [ClickHouseConfig.Builder.build]); it is never selected
 * directly. The fields are mutually exclusive: a config may carry a password, an
 * access token, or a client certificate, but not a combination.
 *
 * - [PASSWORD] — plaintext username + password (the default; an empty
 *   password is still `PASSWORD`).
 * - [ACCESS_TOKEN] — an access token / JWT / bearer credential. See
 *   [ClickHouseConfig.accessToken] and the handshake wiring in
 *   `protocol/Handshake` for how the token rides the wire.
 * - [CERT] — client-certificate (mTLS) identity. The credential side
 *   is configured here; the TLS transport that actually presents the
 *   certificate is the responsibility of the TLS feature (feat/tls). Until
 *   that transport exists, selecting `CERT` configures identity only.
 */
public enum class AuthMethod {

    /** Plaintext username + password (default). */
    PASSWORD,

    /** Access token / JWT / bearer credential. */
    ACCESS_TOKEN,

    /** Client-certificate (mTLS) identity. */
    CERT,
}
