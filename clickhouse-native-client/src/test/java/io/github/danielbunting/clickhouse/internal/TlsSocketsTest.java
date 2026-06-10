package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ConnectionException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link TlsSockets}: SSLContext/factory construction, truststore and
 * keystore loading, and the insecure trust-all selection. Fully offline — a self-signed
 * certificate and its PKCS12 keystore are generated in-test (no Docker, no fixtures).
 */
final class TlsSocketsTest {

    /**
     * With no trust/key material and {@code insecureSkipVerify=false}, a context using
     * the platform default trust store must still build and yield a usable factory.
     */
    @Test
    void defaultTrust_buildsUsableContext() {
        ClickHouseConfig cfg = ClickHouseConfig.builder().tls(true).build();

        SSLContext ctx = TlsSockets.buildContext(cfg);
        assertNotNull(ctx);

        SSLSocketFactory factory = TlsSockets.buildSocketFactory(cfg);
        assertNotNull(factory);
        // Platform default has supported cipher suites available.
        assertTrue(factory.getSupportedCipherSuites().length > 0);
    }

    /**
     * {@code insecureSkipVerify} must select the trust-all manager. We verify behaviour
     * by handshaking against a locally generated self-signed server cert that no real
     * truststore would accept — only a trust-all manager allows the handshake.
     */
    @Test
    void insecureSkipVerify_trustsSelfSignedServer(@TempDir Path dir) throws Exception {
        char[] pwd = "changeit".toCharArray();
        Path serverKs = dir.resolve("server.p12");
        generateSelfSignedKeyStore(serverKs, pwd, "CN=localhost");

        // Server side uses its own keystore as identity.
        SSLContext serverCtx = contextFromKeyStore(serverKs, pwd);

        // Client side: insecure-skip-verify, no truststore — must accept anything.
        ClickHouseConfig insecure = ClickHouseConfig.builder()
                .tls(true)
                .insecureSkipVerify(true)
                .build();
        SSLContext clientCtx = TlsSockets.buildContext(insecure);

        assertTrue(TlsHandshakeProbe.handshakes(serverCtx, clientCtx),
                "insecure-skip-verify client should trust the self-signed server");
    }

    /**
     * Without insecure-skip-verify and with only the platform default trust store, a
     * self-signed server cert must be rejected at handshake time.
     */
    @Test
    void defaultTrust_rejectsSelfSignedServer(@TempDir Path dir) throws Exception {
        char[] pwd = "changeit".toCharArray();
        Path serverKs = dir.resolve("server.p12");
        generateSelfSignedKeyStore(serverKs, pwd, "CN=localhost");
        SSLContext serverCtx = contextFromKeyStore(serverKs, pwd);

        ClickHouseConfig strict = ClickHouseConfig.builder().tls(true).build();
        SSLContext clientCtx = TlsSockets.buildContext(strict);

        assertTrue(!TlsHandshakeProbe.handshakes(serverCtx, clientCtx),
                "default trust should reject an unknown self-signed server cert");
    }

    /**
     * An explicit truststore containing the server's self-signed cert must let the
     * default (non-insecure) client trust that server.
     */
    @Test
    void explicitTrustStore_trustsContainedCert(@TempDir Path dir) throws Exception {
        char[] pwd = "changeit".toCharArray();
        Path serverKs = dir.resolve("server.p12");
        X509Certificate cert = generateSelfSignedKeyStore(serverKs, pwd, "CN=localhost");
        SSLContext serverCtx = contextFromKeyStore(serverKs, pwd);

        // Build a truststore holding just the server cert.
        Path trustKs = dir.resolve("trust.p12");
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("server", cert);
        try (OutputStream out = Files.newOutputStream(trustKs)) {
            ts.store(out, pwd);
        }

        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .tls(true)
                .trustStorePath(trustKs)
                .trustStorePassword(new String(pwd))
                .build();
        SSLContext clientCtx = TlsSockets.buildContext(cfg);

        assertTrue(TlsHandshakeProbe.handshakes(serverCtx, clientCtx),
                "client with explicit truststore should trust the contained cert");
    }

    /** A missing truststore path must surface a clear {@link ConnectionException}. */
    @Test
    void missingTrustStore_throwsConnectionException(@TempDir Path dir) {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .tls(true)
                .trustStorePath(dir.resolve("does-not-exist.p12"))
                .trustStorePassword("x")
                .build();

        assertThrows(ConnectionException.class, () -> TlsSockets.buildContext(cfg));
    }

    /** A keystore configured for mTLS must load and contribute key managers. */
    @Test
    void clientKeyStore_loadsForMtls(@TempDir Path dir) throws Exception {
        char[] pwd = "changeit".toCharArray();
        Path clientKs = dir.resolve("client.p12");
        generateSelfSignedKeyStore(clientKs, pwd, "CN=client");

        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .tls(true)
                .keyStorePath(clientKs)
                .keyStorePassword(new String(pwd))
                .build();

        // Should build without error; the key managers are exercised by a real mTLS
        // server in the integration tests.
        assertNotNull(TlsSockets.buildContext(cfg));
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private static SSLContext contextFromKeyStore(Path ksPath, char[] pwd) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ksPath)) {
            ks.load(in, pwd);
        }
        var kmf = javax.net.ssl.KeyManagerFactory.getInstance(
                javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pwd);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);
        return ctx;
    }

    /**
     * Generates an RSA key pair + self-signed X.509 cert into a PKCS12 keystore at
     * {@code path} (alias {@code "self"}), using the JDK {@code keytool} CLI to avoid
     * depending on internal {@code sun.security.*} APIs. Returns the certificate.
     */
    private static X509Certificate generateSelfSignedKeyStore(Path path, char[] pwd, String dn)
            throws Exception {
        String javaHome = System.getProperty("java.home");
        Path keytool = Path.of(javaHome, "bin", "keytool");
        String pw = new String(pwd);

        ProcessBuilder pb = new ProcessBuilder(
                keytool.toString(),
                "-genkeypair",
                "-alias", "self",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-sigalg", "SHA256withRSA",
                "-validity", "365",
                "-dname", dn,
                "-keystore", path.toString(),
                "-storetype", "PKCS12",
                "-storepass", pw,
                "-keypass", pw);
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String output = new String(proc.getInputStream().readAllBytes());
        int code = proc.waitFor();
        if (code != 0) {
            throw new IllegalStateException("keytool failed (" + code + "): " + output);
        }

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(path)) {
            ks.load(in, pwd);
        }
        return (X509Certificate) ks.getCertificate("self");
    }
}
