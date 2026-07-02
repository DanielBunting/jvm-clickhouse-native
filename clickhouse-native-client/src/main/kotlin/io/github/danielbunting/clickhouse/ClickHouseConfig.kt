package io.github.danielbunting.clickhouse

import io.github.danielbunting.clickhouse.compress.CompressionMethod
import java.net.URI
import java.net.URISyntaxException
import java.nio.file.Path
import java.time.Duration
import java.util.Collections

/**
 * Immutable connection configuration. Build via [builder].
 *
 * The value-object shape and builder are part of the frozen W0.2 contract.
 * URL parsing ([fromUrl]) and full validation are task W1.E1.
 */
public class ClickHouseConfig private constructor(b: Builder) {

    private val host: String
    private val port: Int
    private val endpoints: List<Endpoint>
    private val loadBalancingPolicy: LoadBalancingPolicy
    private val database: String?
    private val username: String?
    private val password: String?
    private val accessToken: String?
    private val clientCertPath: String?
    private val clientKeyPath: String?
    private val authMethod: AuthMethod
    private val compression: CompressionMethod?
    private val insertBatchSize: Int
    private val connectTimeout: Duration?
    private val socketTimeout: Duration?
    private val tls: Boolean
    private val trustStorePath: Path?
    private val trustStorePassword: String?
    private val keyStorePath: Path?
    private val keyStorePassword: String?
    private val verifyHostname: Boolean
    private val insecureSkipVerify: Boolean
    private val settings: Map<String, String>
    private val queryTimeout: Duration?

    init {
        this.endpoints = resolveEndpoints(b)
        // host()/port() report the first (primary) endpoint for backward compatibility.
        val primary = endpoints[0]
        this.host = primary.host
        this.port = primary.port
        this.loadBalancingPolicy = b.loadBalancingPolicy
        this.database = b.database
        this.username = b.username
        this.password = b.password
        this.accessToken = b.accessToken
        this.clientCertPath = b.clientCertPath
        this.clientKeyPath = b.clientKeyPath
        this.authMethod = b.resolveAuthMethod()
        this.compression = b.compression
        this.insertBatchSize = b.insertBatchSize
        this.connectTimeout = b.connectTimeout
        this.socketTimeout = b.socketTimeout
        this.tls = b.tls
        this.trustStorePath = b.trustStorePath
        this.trustStorePassword = b.trustStorePassword
        this.keyStorePath = b.keyStorePath
        this.keyStorePassword = b.keyStorePassword
        this.verifyHostname = b.verifyHostname
        this.insecureSkipVerify = b.insecureSkipVerify
        // Defensive copy preserving insertion order; exposed read-only.
        this.settings = Collections.unmodifiableMap(LinkedHashMap(b.settings))
        this.queryTimeout = b.queryTimeout
    }

    /** The primary (first) endpoint host. */
    public fun host(): String = host

    /** The primary (first) endpoint port. */
    public fun port(): Int = port

    /**
     * The resolved, ordered list of candidate endpoints (always non-empty). A config built
     * with only `host`/`port` yields a single-element list.
     */
    public fun endpoints(): List<Endpoint> = endpoints

    /** The load-balancing / failover ordering policy across [endpoints]. */
    public fun loadBalancingPolicy(): LoadBalancingPolicy = loadBalancingPolicy

    public fun database(): String? = database

    public fun username(): String? = username

    public fun password(): String? = password

    /**
     * The access token / JWT / bearer credential, or `null` if none is set.
     *
     * When non-null this config authenticates with [AuthMethod.ACCESS_TOKEN].
     * The token is carried on the native-protocol handshake (see `protocol/Handshake`).
     *
     * **Security:** an access token is a bearer credential and MUST only be sent
     * over an encrypted transport (TLS). The TLS transport is provided by the TLS
     * feature (feat/tls); until then this credential travels in cleartext on a plain
     * socket, which is unsafe outside of trusted/local testing.
     */
    public fun accessToken(): String? = accessToken

    /**
     * Filesystem path to the client certificate (PEM) used for mTLS identity, or
     * `null` if none is set. Configuring this selects [AuthMethod.CERT].
     *
     * **Seam with feat/tls:** this is the credential/identity side only. The
     * TLS transport that actually presents the certificate during the handshake is
     * the responsibility of feat/tls; this field merely declares the identity.
     */
    public fun clientCertPath(): String? = clientCertPath

    /**
     * Filesystem path to the client private key (PEM) paired with
     * [clientCertPath], or `null` if none is set.
     */
    public fun clientKeyPath(): String? = clientKeyPath

    /**
     * The credential mechanism this config authenticates with, derived from which
     * credential fields are set. Never `null`; defaults to
     * [AuthMethod.PASSWORD].
     */
    public fun authMethod(): AuthMethod = authMethod

    public fun compression(): CompressionMethod? = compression

    public fun insertBatchSize(): Int = insertBatchSize

    public fun connectTimeout(): Duration? = connectTimeout

    public fun socketTimeout(): Duration? = socketTimeout

    /**
     * Whether to connect over TLS. The native protocol is unchanged; TLS is layered
     * beneath the existing socket. ClickHouse's default TLS-native port is `9440`
     * (set [Builder.port] accordingly).
     */
    public fun tls(): Boolean = tls

    /**
     * Optional path to a truststore (JKS or PKCS12) used to verify the server
     * certificate. When `null`, the platform default trust material is used.
     */
    public fun trustStorePath(): Path? = trustStorePath

    /** Optional password for [trustStorePath]; may be `null` or empty. */
    public fun trustStorePassword(): String? = trustStorePassword

    /**
     * Optional path to a keystore (JKS or PKCS12) holding the client certificate and
     * private key for mutual TLS (mTLS). When `null`, no client certificate is sent.
     */
    public fun keyStorePath(): Path? = keyStorePath

    /** Optional password for [keyStorePath]; may be `null` or empty. */
    public fun keyStorePassword(): String? = keyStorePassword

    /**
     * Whether the server certificate's hostname is checked against the connection host
     * (RFC 2818 / "HTTPS" endpoint identification). Defaults to `true`. Ignored
     * when [insecureSkipVerify] is set.
     */
    public fun verifyHostname(): Boolean = verifyHostname

    /**
     * **DEV/TEST ONLY — UNSAFE.** When `true`, the client trusts any server
     * certificate and skips hostname verification entirely (a trust-all
     * `X509TrustManager`). This disables all protection against
     * man-in-the-middle attacks and must never be used in production. Defaults to
     * `false`.
     */
    public fun insecureSkipVerify(): Boolean = insecureSkipVerify

    /**
     * Per-connection default ClickHouse server settings (e.g. `max_execution_time`,
     * `max_memory_usage`, `max_threads`, `async_insert`). Sent on the
     * settings slot of every `Query` packet on this connection; a per-query settings
     * map overrides these key-by-key. Insertion-ordered and immutable; never `null`.
     */
    public fun settings(): Map<String, String> = settings

    /**
     * The client-side query deadline applied per operation: if a query has not
     * completed within this duration, the client sends a `Cancel` packet and
     * the call fails with a
     * [io.github.danielbunting.clickhouse.QueryCancelledException]. `ZERO`
     * (the default) disables the watchdog. A per-call timeout (e.g. JDBC
     * `Statement.setQueryTimeout`) overrides this default.
     *
     * This is purely client-side. Server-side enforcement
     * (`max_execution_time`) is a separate seam owned by `feat/settings`.
     */
    public fun queryTimeout(): Duration? = queryTimeout

    /** Builder with ClickHouse-sensible defaults. */
    public class Builder {
        internal var host: String? = "localhost"
        internal var port: Int = 9000
        internal val endpoints: MutableList<Endpoint> = ArrayList()
        internal var loadBalancingPolicy: LoadBalancingPolicy = LoadBalancingPolicy.FIRST_ALIVE
        internal var database: String? = "default"
        internal var username: String? = "default"
        internal var password: String? = ""
        internal var accessToken: String? = null
        internal var clientCertPath: String? = null
        internal var clientKeyPath: String? = null
        internal var compression: CompressionMethod? = CompressionMethod.LZ4
        internal var insertBatchSize: Int = 65_536
        internal var connectTimeout: Duration? = Duration.ofSeconds(10)
        internal var socketTimeout: Duration? = Duration.ofSeconds(30)
        internal var tls: Boolean = false
        internal var trustStorePath: Path? = null
        internal var trustStorePassword: String? = null
        internal var keyStorePath: Path? = null
        internal var keyStorePassword: String? = null
        internal var verifyHostname: Boolean = true
        internal var insecureSkipVerify: Boolean = false
        internal val settings: MutableMap<String, String> = LinkedHashMap()
        internal var queryTimeout: Duration? = Duration.ZERO

        public fun host(host: String?): Builder {
            this.host = host
            return this
        }

        public fun port(port: Int): Builder {
            this.port = port
            return this
        }

        /**
         * Adds one candidate endpoint. Calling this (one or more times) makes the explicit
         * endpoint list authoritative, and `host`/`port` are then ignored. The
         * first endpoint added is the primary.
         *
         * @param host the endpoint host
         * @param port the endpoint port
         * @return this builder
         */
        public fun endpoint(host: String, port: Int): Builder {
            this.endpoints.add(Endpoint(host, port))
            return this
        }

        /**
         * Replaces the candidate endpoint list. When non-empty, this list is authoritative
         * and `host`/`port` are ignored.
         *
         * @param endpoints the ordered endpoint list (first is primary)
         * @return this builder
         */
        public fun endpoints(endpoints: List<Endpoint>?): Builder {
            this.endpoints.clear()
            if (endpoints != null) {
                this.endpoints.addAll(endpoints)
            }
            return this
        }

        /**
         * Sets the policy for ordering endpoints on connect/failover.
         *
         * @param policy the load-balancing policy (null resets to `FIRST_ALIVE`)
         * @return this builder
         */
        public fun loadBalancingPolicy(policy: LoadBalancingPolicy?): Builder {
            this.loadBalancingPolicy = policy ?: LoadBalancingPolicy.FIRST_ALIVE
            return this
        }

        public fun database(database: String?): Builder {
            this.database = database
            return this
        }

        public fun username(username: String?): Builder {
            this.username = username
            return this
        }

        public fun password(password: String?): Builder {
            this.password = password
            return this
        }

        /**
         * Sets an access token / JWT / bearer credential. Selects
         * [AuthMethod.ACCESS_TOKEN]; mutually exclusive with a non-empty
         * [password] and with the client-certificate fields.
         *
         * @param accessToken the token, or `null`/empty to clear it
         */
        public fun accessToken(accessToken: String?): Builder {
            this.accessToken = accessToken
            return this
        }

        /**
         * Sets the client-certificate (PEM) path for mTLS identity. Selects
         * [AuthMethod.CERT]; mutually exclusive with a non-empty
         * [password] and with [accessToken].
         */
        public fun clientCertPath(clientCertPath: String?): Builder {
            this.clientCertPath = clientCertPath
            return this
        }

        /** Sets the client private-key (PEM) path paired with the client certificate. */
        public fun clientKeyPath(clientKeyPath: String?): Builder {
            this.clientKeyPath = clientKeyPath
            return this
        }

        /**
         * Derives the [AuthMethod] from the credential fields set on this
         * builder and validates that they are mutually exclusive.
         *
         * Precedence is by uniqueness, not priority: at most one credential
         * family (password / access token / client certificate) may be configured.
         * The default empty password is the implicit baseline and does not conflict
         * with selecting a token or certificate.
         *
         * @throws ConfigurationException if more than one credential family is set,
         *         or the certificate is incomplete (cert without key, or vice versa)
         */
        internal fun resolveAuthMethod(): AuthMethod {
            val hasPassword = !password.isNullOrEmpty()
            val hasToken = !accessToken.isNullOrEmpty()
            val hasCert = !clientCertPath.isNullOrEmpty()
            val hasKey = !clientKeyPath.isNullOrEmpty()

            val families = (if (hasPassword) 1 else 0) + (if (hasToken) 1 else 0) + (if (hasCert) 1 else 0)
            if (families > 1) {
                throw ConfigurationException(
                    "Conflicting credentials: choose exactly one of password, " +
                        "accessToken, or clientCertPath/clientKeyPath " +
                        "(password set=$hasPassword, accessToken set=$hasToken" +
                        ", clientCert set=$hasCert)."
                )
            }

            if (hasToken) {
                return AuthMethod.ACCESS_TOKEN
            }
            if (hasCert || hasKey) {
                if (!hasCert || !hasKey) {
                    throw ConfigurationException(
                        "Client-certificate auth requires both clientCertPath and " +
                            "clientKeyPath to be set (clientCertPath set=$hasCert" +
                            ", clientKeyPath set=$hasKey)."
                    )
                }
                return AuthMethod.CERT
            }
            return AuthMethod.PASSWORD
        }

        public fun compression(compression: CompressionMethod?): Builder {
            this.compression = compression
            return this
        }

        public fun insertBatchSize(insertBatchSize: Int): Builder {
            this.insertBatchSize = insertBatchSize
            return this
        }

        public fun connectTimeout(connectTimeout: Duration?): Builder {
            this.connectTimeout = connectTimeout
            return this
        }

        public fun socketTimeout(socketTimeout: Duration?): Builder {
            this.socketTimeout = socketTimeout
            return this
        }

        /** Enable or disable TLS transport. See [ClickHouseConfig.tls]. */
        public fun tls(tls: Boolean): Builder {
            this.tls = tls
            return this
        }

        /** Set the truststore path. See [ClickHouseConfig.trustStorePath]. */
        public fun trustStorePath(trustStorePath: Path?): Builder {
            this.trustStorePath = trustStorePath
            return this
        }

        /** Set the truststore password. See [ClickHouseConfig.trustStorePassword]. */
        public fun trustStorePassword(trustStorePassword: String?): Builder {
            this.trustStorePassword = trustStorePassword
            return this
        }

        /** Set the client-certificate keystore path. See [ClickHouseConfig.keyStorePath]. */
        public fun keyStorePath(keyStorePath: Path?): Builder {
            this.keyStorePath = keyStorePath
            return this
        }

        /** Set the client-certificate keystore password. See [ClickHouseConfig.keyStorePassword]. */
        public fun keyStorePassword(keyStorePassword: String?): Builder {
            this.keyStorePassword = keyStorePassword
            return this
        }

        /** Enable or disable hostname verification. See [ClickHouseConfig.verifyHostname]. */
        public fun verifyHostname(verifyHostname: Boolean): Builder {
            this.verifyHostname = verifyHostname
            return this
        }

        /**
         * Adds (or replaces) a single per-connection default ClickHouse server setting,
         * sent on the settings slot of every `Query` packet. Insertion order is
         * preserved; a per-query settings map overrides these key-by-key.
         *
         * @param name  the setting name (must not be `null`/blank)
         * @param value the setting value as a string (must not be `null`)
         */
        public fun setting(name: String?, value: String?): Builder {
            if (name.isNullOrBlank()) {
                throw ClickHouseException("setting name must not be null or blank")
            }
            if (value == null) {
                throw ClickHouseException("setting value must not be null (setting '$name')")
            }
            this.settings[name] = value
            return this
        }

        /**
         * **DEV/TEST ONLY — UNSAFE.** Trust any server certificate and skip hostname
         * verification. See [ClickHouseConfig.insecureSkipVerify].
         */
        public fun insecureSkipVerify(insecureSkipVerify: Boolean): Builder {
            this.insecureSkipVerify = insecureSkipVerify
            return this
        }

        /**
         * Adds all entries of [settings] as per-connection default server settings,
         * preserving the map's iteration order. Existing keys are overwritten.
         *
         * @param settings the settings to add (may be empty; must not be `null`)
         */
        public fun settings(settings: Map<String, String>?): Builder {
            if (settings == null) {
                throw ClickHouseException("settings map must not be null")
            }
            for ((name, value) in settings) {
                setting(name, value)
            }
            return this
        }

        /**
         * Sets the client-side query deadline (see [ClickHouseConfig.queryTimeout]).
         * Pass `null` or [Duration.ZERO] to disable.
         */
        public fun queryTimeout(queryTimeout: Duration?): Builder {
            this.queryTimeout = queryTimeout ?: Duration.ZERO
            return this
        }

        public fun build(): ClickHouseConfig {
            return ClickHouseConfig(this)
        }
    }

    public companion object {

        @JvmStatic
        public fun builder(): Builder = Builder()

        /**
         * Resolves the effective endpoint list from a builder: the explicit `endpoints`
         * list if any was added, otherwise a single-element list from `host`/`port`
         * (backward compatibility).
         */
        private fun resolveEndpoints(b: Builder): List<Endpoint> {
            if (b.endpoints.isNotEmpty()) {
                return java.util.List.copyOf(b.endpoints)
            }
            val host = b.host
                ?: throw ConfigurationException("Endpoint host must not be null or blank")
            return java.util.List.of(Endpoint(host, b.port))
        }

        /**
         * Parses a connection URL of the form
         * `chnative://[user[:password]@]host[:port][,host2[:port2]...]/[database][?k=v&...]`.
         *
         * Multiple comma-separated `host:port` endpoints may be supplied; they become the
         * config's endpoint list (in order) and are tried with failover per the
         * `loadBalancingPolicy`. A single host behaves exactly as before.
         *
         * Defaults (matching builder defaults): port=9000, database="default",
         * username="default", password="".
         *
         * Supported query parameters:
         * - `compression` — `lz4`, `zstd`, or `none`
         * - `insertBatchSize` — positive integer
         * - `connectTimeout` — seconds, positive integer
         * - `socketTimeout` — seconds, positive integer
         * - `ssl` / `secure` — `true`/`false`: enable TLS
         * - `sslmode` — `none`/`disable` (no TLS), `strict`/`verify-full`/`require`/`true`
         *   (TLS), or `none-verify`/`insecure` (TLS with insecure-skip-verify, dev only)
         * - `loadBalancingPolicy` — `first_alive`, `round_robin`, or `random`
         * - `queryTimeout` — seconds, positive integer (client-side query deadline)
         *
         * @param url the connection URL
         * @return a fully populated, immutable [ClickHouseConfig]
         * @throws ClickHouseException if the URL is null, uses the wrong scheme, has a
         *                             missing host, or contains invalid parameter values
         */
        @JvmStatic
        public fun fromUrl(url: String?): ClickHouseConfig {
            if (url == null) {
                throw ClickHouseException("Connection URL must not be null")
            }

            // java.net.URI requires a valid absolute URI; swap scheme so the JDK does
            // not complain about the custom "chnative" scheme.
            if (!url.startsWith("chnative://")) {
                throw ClickHouseException(
                    "Invalid URL scheme — expected 'chnative://', got: $url"
                )
            }

            // java.net.URI can only model a single host in the authority. Split out the
            // comma-separated host list ourselves, then hand the JDK a rewritten URL whose
            // authority carries only the FIRST host so it can parse userinfo/path/query. The
            // endpoints (all of them) come from our own parse.
            val hostList = extractHostList(url)
            val parsedEndpoints = parseEndpoints(hostList, url)
            val singleHostUrl = rewriteToSingleHost(url, hostList, parsedEndpoints[0])

            val uri: URI
            try {
                // Replace scheme with "http" purely for parsing; ClickHouse-native TCP
                // does not use HTTP; the resulting URI object is only used to extract
                // userinfo/path/query components (host/port come from parsedEndpoints).
                uri = URI("http" + singleHostUrl.substring("chnative".length))
            } catch (e: URISyntaxException) {
                throw ClickHouseException("Malformed connection URL: $url", e)
            }

            val b = builder().endpoints(parsedEndpoints)

            // User-info: user[:password]. Parsed from the RAW authority rather than
            // uri.userInfo: the JDK percent-decodes userInfo before we can split it
            // (so an encoded %3A would be mistaken for the separator), and it returns
            // null outright for registry-based authorities (e.g. hosts containing
            // '_'). Split at the first LITERAL ':', then percent-decode each part.
            val userInfo = extractUserInfo(url)
            if (!userInfo.isNullOrEmpty()) {
                val colonIdx = userInfo.indexOf(':')
                if (colonIdx == -1) {
                    b.username(percentDecode(userInfo))
                } else {
                    val user = userInfo.substring(0, colonIdx)
                    val pass = userInfo.substring(colonIdx + 1)
                    if (user.isNotEmpty()) {
                        b.username(percentDecode(user))
                    }
                    b.password(percentDecode(pass))
                }
            }

            // Database from path: leading '/' is stripped
            val path = uri.path
            if (!path.isNullOrEmpty()) {
                // path is "/database"; strip leading slash
                val db = if (path.startsWith("/")) path.substring(1) else path
                if (db.isNotEmpty()) {
                    b.database(db)
                }
            }

            // Query parameters
            val rawQuery = uri.rawQuery
            if (!rawQuery.isNullOrEmpty()) {
                val params = parseQuery(rawQuery)
                for ((key, value) in params) {
                    when (key) {
                        "compression" -> {
                            val method = when (value.lowercase()) {
                                "lz4" -> CompressionMethod.LZ4
                                "zstd" -> CompressionMethod.ZSTD
                                "none" -> CompressionMethod.NONE
                                else -> throw ClickHouseException(
                                    "Unknown compression method '$value' — expected lz4, zstd, or none"
                                )
                            }
                            b.compression(method)
                        }
                        "insertBatchSize" -> {
                            val size = parsePositiveInt(value, "insertBatchSize")
                            b.insertBatchSize(size)
                        }
                        "connectTimeout" -> {
                            val secs = parsePositiveInt(value, "connectTimeout")
                            b.connectTimeout(Duration.ofSeconds(secs.toLong()))
                        }
                        "socketTimeout" -> {
                            val secs = parsePositiveInt(value, "socketTimeout")
                            b.socketTimeout(Duration.ofSeconds(secs.toLong()))
                        }
                        "ssl", "secure" -> b.tls(parseBoolean(value, key))
                        "sslmode" -> {
                            when (value.lowercase()) {
                                "none", "disable" -> b.tls(false)
                                "strict", "verify-full", "require", "true" -> b.tls(true)
                                "none-verify", "insecure" -> b.tls(true).insecureSkipVerify(true)
                                else -> throw ClickHouseException(
                                    "Unknown sslmode '$value' — expected none, disable, " +
                                        "strict, verify-full, require, true, none-verify, or insecure"
                                )
                            }
                        }
                        "loadBalancingPolicy" -> {
                            val policy = when (value.lowercase()) {
                                "first_alive", "firstalive" -> LoadBalancingPolicy.FIRST_ALIVE
                                "round_robin", "roundrobin" -> LoadBalancingPolicy.ROUND_ROBIN
                                "random" -> LoadBalancingPolicy.RANDOM
                                else -> throw ClickHouseException(
                                    "Unknown loadBalancingPolicy '$value' — expected first_alive, round_robin, or random"
                                )
                            }
                            b.loadBalancingPolicy(policy)
                        }
                        "queryTimeout" -> {
                            val secs = parsePositiveInt(value, "queryTimeout")
                            b.queryTimeout(Duration.ofSeconds(secs.toLong()))
                        }
                        else -> {
                            // A "settings.<name>=<value>" query parameter contributes a
                            // per-connection default ClickHouse server setting.
                            if (key.startsWith("settings.") && key.length > "settings.".length) {
                                b.setting(key.substring("settings.".length), value)
                            } else {
                                throw ClickHouseException(
                                    "Unknown query parameter in connection URL: '$key'"
                                )
                            }
                        }
                    }
                }
            }

            return b.build()
        }

        /**
         * Parses a connection URL exactly like [fromUrl], then applies
         * credential overrides supplied via a [java.util.Properties] instance.
         *
         * This overload exists primarily for the JDBC driver, where the
         * `DriverManager` may pass user/password out-of-band in the connection
         * `Properties` rather than embedding them in the URL. If [info]
         * contains a `"user"` or `"username"` key, that value overrides the
         * username parsed from the URL; if it contains a `"password"` key, that
         * value overrides the password parsed from the URL.
         *
         * @param url  the connection URL (see [fromUrl])
         * @param info connection properties; may be `null` or empty, in which case
         *             this behaves identically to [fromUrl]
         * @return a fully populated, immutable [ClickHouseConfig]
         * @throws ClickHouseException if the URL is invalid (see [fromUrl])
         */
        @JvmStatic
        public fun fromUrl(url: String?, info: java.util.Properties?): ClickHouseConfig {
            val base = fromUrl(url)
            if (info == null) {
                return base
            }

            // No isEmpty short-circuit: Hashtable.isEmpty() ignores entries a caller
            // supplied as Properties DEFAULTS, while getProperty() consults them.
            var user = info.getProperty("user")
            if (user == null) {
                user = info.getProperty("username")
            }
            val password = info.getProperty("password")

            if (user == null && password == null) {
                return base
            }

            val b = builder()
                .endpoints(base.endpoints())
                .loadBalancingPolicy(base.loadBalancingPolicy())
                .database(base.database())
                .username(base.username())
                .password(base.password())
                .accessToken(base.accessToken())
                .clientCertPath(base.clientCertPath())
                .clientKeyPath(base.clientKeyPath())
                .compression(base.compression())
                .insertBatchSize(base.insertBatchSize())
                .connectTimeout(base.connectTimeout())
                .socketTimeout(base.socketTimeout())
                .tls(base.tls())
                .trustStorePath(base.trustStorePath())
                .trustStorePassword(base.trustStorePassword())
                .keyStorePath(base.keyStorePath())
                .keyStorePassword(base.keyStorePassword())
                .verifyHostname(base.verifyHostname())
                .insecureSkipVerify(base.insecureSkipVerify())
                .settings(base.settings())
                .queryTimeout(base.queryTimeout())

            if (user != null) {
                b.username(user)
            }
            if (password != null) {
                b.password(password)
            }
            return b.build()
        }

        /**
         * Extracts the raw (still percent-encoded) `user[:password]` portion of a
         * `chnative://` URL authority — the text before the last literal `@` — or
         * `null` when the URL carries no credentials.
         */
        private fun extractUserInfo(url: String): String? {
            val authority = extractAuthority(url)
            val at = authority.lastIndexOf('@')
            return if (at >= 0) authority.substring(0, at) else null
        }

        /**
         * Percent-decodes one raw URL component per RFC 3986 (UTF-8): only `%XX`
         * escapes are decoded. A literal `'+'` is ordinary data here — `'+'`-as-space
         * is form encoding (`application/x-www-form-urlencoded`), which never applies
         * to URL components — so it is pre-escaped to survive [java.net.URLDecoder]'s
         * form-decoding verbatim.
         */
        private fun percentDecode(s: String): String =
            java.net.URLDecoder.decode(s.replace("+", "%2B"), Charsets.UTF_8)

        /** The full raw authority of a `chnative://` URL (userinfo + host list). */
        private fun extractAuthority(url: String): String {
            val afterScheme = url.substring("chnative://".length)
            var authorityEnd = afterScheme.length
            val slash = afterScheme.indexOf('/')
            val query = afterScheme.indexOf('?')
            if (slash >= 0) {
                authorityEnd = minOf(authorityEnd, slash)
            }
            if (query >= 0) {
                authorityEnd = minOf(authorityEnd, query)
            }
            return afterScheme.substring(0, authorityEnd)
        }

        /**
         * Extracts the comma-separated host-list portion of a `chnative://` URL — the
         * authority after any `user[:password]@` prefix and before the path/query. The
         * result may carry one or many `host[:port]` entries.
         */
        private fun extractHostList(url: String): String {
            var authority = extractAuthority(url)
            val at = authority.lastIndexOf('@')
            if (at >= 0) {
                authority = authority.substring(at + 1)
            }
            if (authority.isEmpty()) {
                throw ClickHouseException("Connection URL is missing a host: $url")
            }
            return authority
        }

        /**
         * Parses a comma-separated host list into endpoints, defaulting an omitted port to
         * `9000`.
         */
        private fun parseEndpoints(hostList: String, url: String): List<Endpoint> {
            val result = ArrayList<Endpoint>()
            for (entry in hostList.split(",")) {
                val trimmed = entry.trim()
                if (trimmed.isEmpty()) {
                    continue
                }
                val host: String
                var port = 9000
                // A bracketed IPv6 literal ("[::1]" or "[::1]:9000") must not be split on
                // its internal colons: only a ':' *after* the closing ']' introduces a port.
                val portColon: Int
                if (trimmed.startsWith("[")) {
                    val close = trimmed.indexOf(']')
                    if (close < 0) {
                        throw ClickHouseException(
                            "Unterminated IPv6 host in endpoint '$trimmed' of URL: $url"
                        )
                    }
                    host = trimmed.substring(1, close)
                    if (close + 1 == trimmed.length) {
                        portColon = -1
                    } else if (trimmed[close + 1] == ':') {
                        portColon = close + 1
                    } else {
                        // Reject trailing junk after "]" that isn't a ":port".
                        throw ClickHouseException(
                            "Invalid IPv6 endpoint '$trimmed' of URL: $url"
                        )
                    }
                } else {
                    portColon = trimmed.lastIndexOf(':')
                    host = if (portColon >= 0) trimmed.substring(0, portColon) else trimmed
                }
                if (portColon >= 0) {
                    try {
                        port = Integer.parseInt(trimmed.substring(portColon + 1))
                    } catch (e: NumberFormatException) {
                        throw ClickHouseException(
                            "Invalid port in endpoint '$trimmed' of URL: $url", e
                        )
                    }
                }
                result.add(Endpoint(host, port))
            }
            if (result.isEmpty()) {
                throw ClickHouseException("Connection URL is missing a host: $url")
            }
            return result
        }

        /**
         * Rewrites the URL so its authority carries only the first endpoint, letting
         * [URI] parse the userinfo/path/query while the full endpoint list is kept
         * from our own parse.
         */
        private fun rewriteToSingleHost(url: String, hostList: String, first: Endpoint): String {
            // Re-bracket an IPv6 literal (bare "::1" from our parse) so the JDK URI parser,
            // which we borrow only for userinfo/path/query, accepts the rewritten authority.
            val host = if (first.host.contains(':')) "[${first.host}]" else first.host
            return url.replaceFirst(hostList, "$host:${first.port}")
        }

        /**
         * Splits a raw query string into an ordered key-value map.
         * Duplicate keys result in the last value winning.
         */
        private fun parseQuery(rawQuery: String): Map<String, String> {
            val map = LinkedHashMap<String, String>()
            for (pair in rawQuery.split("&")) {
                if (pair.isEmpty()) {
                    continue
                }
                val eq = pair.indexOf('=')
                if (eq == -1) {
                    // bare key with no value — treat as flag; unsupported, will be caught
                    // by the switch default
                    map[pair] = ""
                } else {
                    val k = pair.substring(0, eq)
                    val v = pair.substring(eq + 1)
                    map[k] = v
                }
            }
            return map
        }

        /**
         * Parses an integer that must be strictly positive.
         *
         * @param value the raw string value
         * @param param the parameter name (used in error messages)
         * @return the parsed positive integer
         * @throws ClickHouseException if the value is not a valid positive integer
         */
        private fun parsePositiveInt(value: String, param: String): Int {
            val result: Int
            try {
                result = Integer.parseInt(value)
            } catch (e: NumberFormatException) {
                throw ClickHouseException(
                    "Parameter '$param' must be an integer, got: '$value'", e
                )
            }
            if (result <= 0) {
                throw ClickHouseException(
                    "Parameter '$param' must be positive, got: $result"
                )
            }
            return result
        }

        /**
         * Parses a boolean flag accepting `true`/`false` (case-insensitive),
         * plus `1`/`0` and `yes`/`no` as common synonyms.
         *
         * @param value the raw string value
         * @param param the parameter name (used in error messages)
         * @return the parsed boolean
         * @throws ClickHouseException if the value is not a recognised boolean
         */
        private fun parseBoolean(value: String, param: String): Boolean {
            return when (value.lowercase()) {
                "true", "1", "yes" -> true
                "false", "0", "no" -> false
                else -> throw ClickHouseException(
                    "Parameter '$param' must be a boolean (true/false), got: '$value'"
                )
            }
        }
    }
}
