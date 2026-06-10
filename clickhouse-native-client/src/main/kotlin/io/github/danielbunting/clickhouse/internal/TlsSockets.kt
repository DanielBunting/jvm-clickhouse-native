package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ConnectionException
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

/**
 * Builds the TLS plumbing for the native-TCP transport from a [ClickHouseConfig].
 *
 * The native protocol is unchanged by TLS — this helper only produces an
 * [SSLSocketFactory] that `NativeClientImpl` layers over the connection.
 * Trust/key material is resolved as follows:
 *
 *  - **Trust:** if [ClickHouseConfig.insecureSkipVerify] is set, a trust-all
 *    manager is used (DEV/TEST ONLY); otherwise, if a truststore path is given it
 *    is loaded (JKS or PKCS12, auto-detected); otherwise the platform default
 *    trust material is used (pass `null` trust managers to the context).
 *  - **Key (mTLS):** if a keystore path is given it is loaded and its
 *    [KeyManager]s drive client-certificate authentication; otherwise none.
 *
 * Hostname / endpoint identification is configured on the socket itself
 * (in `NativeClientImpl`), not here, because it depends on the connect host.
 */
internal object TlsSockets {

    /**
     * Builds an [SSLContext] per the TLS settings on [config].
     *
     * @param config the connection configuration (must have `tls()` semantics)
     * @return an initialised `SSLContext`
     * @throws ConnectionException if trust/key material cannot be loaded or the
     *                             context cannot be initialised
     */
    @JvmStatic
    fun buildContext(config: ClickHouseConfig): SSLContext {
        try {
            val keyManagers = loadKeyManagers(config)
            val trustManagers = loadTrustManagers(config)

            val ctx = SSLContext.getInstance("TLS")
            ctx.init(keyManagers, trustManagers, SecureRandom())
            return ctx
        } catch (e: ConnectionException) {
            throw e
        } catch (e: Exception) {
            throw ConnectionException("Failed to initialise TLS context: " + e.message, e)
        }
    }

    /**
     * Convenience wrapper returning the [SSLSocketFactory] from [buildContext].
     */
    @JvmStatic
    fun buildSocketFactory(config: ClickHouseConfig): SSLSocketFactory {
        return buildContext(config).socketFactory
    }

    /**
     * Resolves the key managers used for client-certificate (mTLS) authentication, or
     * `null` when no keystore is configured.
     */
    private fun loadKeyManagers(config: ClickHouseConfig): Array<KeyManager>? {
        val keyStorePath = config.keyStorePath() ?: return null
        val password = toChars(config.keyStorePassword())
        try {
            val ks = loadKeyStore(keyStorePath, password)
            val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
            kmf.init(ks, password)
            return kmf.keyManagers
        } catch (e: Exception) {
            throw ConnectionException(
                "Failed to load client-certificate keystore '" + keyStorePath + "': "
                    + e.message, e
            )
        }
    }

    /**
     * Resolves the trust managers used to verify the server certificate. Returns a
     * trust-all manager when `insecureSkipVerify`, an explicit truststore's
     * managers when a path is configured, or `null` (platform default) otherwise.
     */
    private fun loadTrustManagers(config: ClickHouseConfig): Array<TrustManager>? {
        if (config.insecureSkipVerify()) {
            // DEV/TEST ONLY — trusts every certificate, disabling MITM protection.
            return arrayOf(INSECURE_TRUST_ALL)
        }

        val trustStorePath = config.trustStorePath()
            // Platform default trust material — null trust managers tell the
            // SSLContext to use the JVM default TrustManagerFactory.
            ?: return null

        val password = toChars(config.trustStorePassword())
        try {
            val ts = loadKeyStore(trustStorePath, password)
            val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            tmf.init(ts)
            return tmf.trustManagers
        } catch (e: Exception) {
            throw ConnectionException(
                "Failed to load truststore '" + trustStorePath + "': " + e.message, e
            )
        }
    }

    /**
     * Loads a [KeyStore], auto-detecting JKS vs PKCS12: tries PKCS12 first
     * (the modern default), then falls back to JKS.
     */
    @Throws(Exception::class)
    private fun loadKeyStore(path: Path, password: CharArray?): KeyStore {
        var firstFailure: Exception? = null
        for (type in arrayOf("PKCS12", "JKS")) {
            try {
                Files.newInputStream(path).use { input ->
                    val ks = KeyStore.getInstance(type)
                    ks.load(input, password)
                    return ks
                }
            } catch (e: Exception) {
                if (firstFailure == null) {
                    firstFailure = e
                }
            }
        }
        throw firstFailure!!
    }

    private fun toChars(s: String?): CharArray? {
        return s?.toCharArray()
    }

    /**
     * DEV/TEST ONLY trust manager that unconditionally accepts every certificate
     * chain. Only ever installed when [ClickHouseConfig.insecureSkipVerify]
     * is explicitly enabled — never trust this in production.
     */
    private val INSECURE_TRUST_ALL: X509TrustManager = object : X509TrustManager {
        override fun checkClientTrusted(chain: Array<X509Certificate>?, authType: String?) {
            // intentionally trust everything — insecure-skip-verify
        }

        override fun checkServerTrusted(chain: Array<X509Certificate>?, authType: String?) {
            // intentionally trust everything — insecure-skip-verify
        }

        override fun getAcceptedIssuers(): Array<X509Certificate> {
            return arrayOf()
        }
    }
}
