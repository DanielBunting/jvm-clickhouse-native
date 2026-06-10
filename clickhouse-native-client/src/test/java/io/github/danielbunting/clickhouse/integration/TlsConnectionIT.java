package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end validation of the optional TLS transport (feat/tls) against a real server.
 *
 * <p>The container is self-contained: at startup it generates a throwaway self-signed
 * certificate (SAN {@code localhost}/{@code 127.0.0.1}) with the in-image {@code openssl},
 * enables ClickHouse's secure native port (9440) pointing at that cert, then execs the normal
 * entrypoint. No host tooling, no committed certs.
 *
 * <p>Two paths are exercised: trust-all ({@code insecureSkipVerify}) and an explicit truststore
 * built from the server's own certificate (validating {@link ClickHouseConfig#trustStorePath()}
 * loading). Hostname verification is disabled for the truststore case because Testcontainers maps
 * the port onto the Docker host, which need not match the cert's SAN — this test targets the TLS
 * transport + trust material, not SNI/hostname matching.
 */
@Tag("integration")
@Testcontainers
class TlsConnectionIT {

    private static final int SECURE_PORT = 9440;

    private static final String TLS_CONFIG = """
            <clickhouse>
              <tcp_port_secure>9440</tcp_port_secure>
              <openSSL>
                <server>
                  <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
                  <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
                  <verificationMode>none</verificationMode>
                  <cacheSessions>true</cacheSessions>
                  <disableProtocols>sslv2,sslv3</disableProtocols>
                  <preferServerCiphers>true</preferServerCiphers>
                </server>
              </openSSL>
            </clickhouse>
            """;

    /** Re-open the {@code default} user to all networks (same rationale as ClickHouseContainer). */
    private static final String OPEN_DEFAULT_USER = """
            <clickhouse>
              <users>
                <default>
                  <networks replace="replace"><ip>::/0</ip></networks>
                </default>
              </users>
            </clickhouse>
            """;

    /** Generate a self-signed cert, hand it to the clickhouse user, then start the server. */
    private static final String ENTRYPOINT = String.join(" && ",
            "mkdir -p /etc/clickhouse-server/certs",
            "openssl req -x509 -newkey rsa:2048 -nodes -days 1"
                    + " -keyout /etc/clickhouse-server/certs/server.key"
                    + " -out /etc/clickhouse-server/certs/server.crt"
                    + " -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1",
            "chown -R clickhouse:clickhouse /etc/clickhouse-server/certs",
            "exec /entrypoint.sh");

    @Container
    static final GenericContainer<?> CH = new GenericContainer<>(ClickHouseImages.SERVER)
            .withExposedPorts(SECURE_PORT)
            .withCopyToContainer(
                    Transferable.of(TLS_CONFIG), "/etc/clickhouse-server/config.d/tls.xml")
            .withCopyToContainer(
                    Transferable.of(OPEN_DEFAULT_USER),
                    "/etc/clickhouse-server/users.d/zz-open-default.xml")
            .withCreateContainerCmdModifier(cmd -> {
                cmd.withUser("root"); // entrypoint.sh drops to the clickhouse user itself
                cmd.withEntrypoint("/bin/bash", "-c", ENTRYPOINT);
            })
            .waitingFor(Wait.forListeningPort());

    private static ClickHouseConfig.Builder baseTls() {
        return ClickHouseConfig.builder()
                .host(CH.getHost())
                .port(CH.getMappedPort(SECURE_PORT))
                .tls(true);
    }

    @Test
    void connectsOverTlsWithInsecureSkipVerify() {
        ClickHouseConfig cfg = baseTls().insecureSkipVerify(true).build();
        try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
            assertEquals(1L, conn.executeScalar("SELECT 1"),
                    "a TLS (trust-all) connection should run a query end-to-end");
        }
    }

    @Test
    void connectsOverTlsTrustingTheServerCertificate() throws Exception {
        // Pull the server's self-signed cert out of the container and trust it explicitly.
        String pem = CH.execInContainer("cat", "/etc/clickhouse-server/certs/server.crt").getStdout();
        Certificate cert;
        try (InputStream in = new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))) {
            cert = CertificateFactory.getInstance("X.509").generateCertificate(in);
        }
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ch-server", cert);
        Path tsFile = Files.createTempFile("ch-truststore", ".p12");
        tsFile.toFile().deleteOnExit();
        try (OutputStream out = Files.newOutputStream(tsFile)) {
            ts.store(out, "changeit".toCharArray());
        }

        ClickHouseConfig cfg = baseTls()
                .trustStorePath(tsFile)
                .trustStorePassword("changeit")
                .verifyHostname(false)
                .build();
        try (ClickHouseConnection conn = ClickHouseConnection.open(cfg)) {
            assertEquals(1L, conn.executeScalar("SELECT 1"),
                    "a TLS connection trusting the server cert should run a query end-to-end");
        }
    }
}
