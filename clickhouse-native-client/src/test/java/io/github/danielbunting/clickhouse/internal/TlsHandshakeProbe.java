package io.github.danielbunting.clickhouse.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

/**
 * Test-only helper that performs a real TLS handshake over a loopback socket between a
 * server {@link SSLContext} and a client {@link SSLContext}, reporting whether it
 * succeeds. Used to assert trust-manager selection in {@link TlsSocketsTest} without
 * any external server.
 */
final class TlsHandshakeProbe {

    private TlsHandshakeProbe() {
    }

    /**
     * Runs a one-shot TLS handshake on loopback.
     *
     * @return {@code true} if both sides complete the handshake, {@code false} if it
     *         fails (e.g. the client rejects the server certificate)
     */
    static boolean handshakes(SSLContext serverCtx, SSLContext clientCtx) throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try (SSLServerSocket server =
                (SSLServerSocket) serverCtx.getServerSocketFactory().createServerSocket(
                        0, 1, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(5000);
            int port = server.getLocalPort();

            Future<Boolean> serverSide = exec.submit(() -> {
                try (SSLSocket s = (SSLSocket) server.accept()) {
                    s.setSoTimeout(5000);
                    s.startHandshake();
                    return true;
                } catch (IOException e) {
                    return false;
                }
            });

            boolean clientOk;
            try (SSLSocket client = (SSLSocket) clientCtx.getSocketFactory().createSocket()) {
                client.connect(
                        new java.net.InetSocketAddress(InetAddress.getLoopbackAddress(), port),
                        5000);
                client.setSoTimeout(5000);
                // Endpoint identification is intentionally NOT set here: these probes
                // exercise trust-manager behaviour (cert chain acceptance), not hostname
                // verification.
                client.startHandshake();
                clientOk = true;
            } catch (IOException e) {
                clientOk = false;
            }

            boolean serverOk;
            try {
                serverOk = serverSide.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                serverOk = false;
            }
            return clientOk && serverOk;
        } finally {
            exec.shutdownNow();
        }
    }
}
