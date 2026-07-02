package io.github.danielbunting.clickhouse.integration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal byte-level TCP proxy for fault-injection tests against a live
 * ClickHouse server.
 *
 * <p>Listens on an ephemeral loopback port and forwards every accepted client
 * connection to the configured upstream {@code host:port}, pumping bytes in
 * both directions on two daemon threads per connection. Two fault hooks are
 * exposed, both acting on the <b>server&rarr;client</b> direction only (the
 * client&rarr;server direction is always forwarded untouched):
 *
 * <ul>
 *   <li>{@link #pauseServerToClient()} / {@link #resumeServerToClient()} —
 *       while paused, bytes read from the server are buffered instead of
 *       delivered, so a client read blocks (and can hit its socket timeout)
 *       even though the server has already answered. {@code resume} flushes
 *       the held-back bytes in order and restores normal forwarding.</li>
 *   <li>{@link #injectServerToClient(byte[])} — splices raw bytes into the
 *       server&rarr;client stream ahead of anything currently buffered.
 *       Callers are responsible for choosing a moment when no partial packet
 *       is in flight (e.g. while paused at a packet boundary).</li>
 * </ul>
 *
 * <p>Thread-safety: the pause flag, the held-back buffer and the client-side
 * output stream are all guarded by a single per-connection lock, so a pump
 * write, a resume-flush and an injection can never interleave mid-buffer.
 *
 * <p>Test infrastructure only — deliberately small: no TLS, no backpressure
 * beyond TCP's own, and faults apply to <i>all</i> live connections (tests
 * open exactly one).
 */
final class FaultInjectingProxy implements AutoCloseable {

    private final String upstreamHost;
    private final int upstreamPort;
    private final ServerSocket serverSocket;
    private final Thread acceptThread;

    private final Object lock = new Object();
    /** Held-back server->client bytes while paused; guarded by {@link #lock}. */
    private final ByteArrayOutputStream heldBack = new ByteArrayOutputStream();
    /** Whether server->client forwarding is paused; guarded by {@link #lock}. */
    private boolean paused;

    /** Live per-connection client-side output streams; guarded by {@link #lock}. */
    private final List<OutputStream> clientOutputs = new ArrayList<>();
    /** All sockets ever opened, closed (best effort) on {@link #close()}. */
    private final List<Socket> sockets = new ArrayList<>();

    private volatile boolean running = true;

    /**
     * Starts the proxy: binds an ephemeral loopback port and begins accepting.
     *
     * @param upstreamHost the real server host to forward to
     * @param upstreamPort the real server port to forward to
     * @throws IOException if the listening socket cannot be bound
     */
    FaultInjectingProxy(String upstreamHost, int upstreamPort) throws IOException {
        this.upstreamHost = upstreamHost;
        this.upstreamPort = upstreamPort;
        this.serverSocket = new ServerSocket();
        this.serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        this.acceptThread = new Thread(this::acceptLoop, "fault-proxy-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    /** The loopback address tests should connect to. */
    String host() {
        return serverSocket.getInetAddress().getHostAddress();
    }

    /** The ephemeral port tests should connect to. */
    int port() {
        return serverSocket.getLocalPort();
    }

    /**
     * Holds back all server&rarr;client bytes (buffering them) until
     * {@link #resumeServerToClient()}. Client&rarr;server traffic is unaffected.
     */
    void pauseServerToClient() {
        synchronized (lock) {
            paused = true;
        }
    }

    /**
     * Resumes server&rarr;client forwarding, first delivering (in order) any
     * bytes held back while paused.
     *
     * @throws IOException if flushing the held-back bytes to the client fails
     */
    void resumeServerToClient() throws IOException {
        synchronized (lock) {
            paused = false;
            if (heldBack.size() > 0) {
                byte[] pending = heldBack.toByteArray();
                heldBack.reset();
                for (OutputStream out : clientOutputs) {
                    out.write(pending);
                    out.flush();
                }
            }
        }
    }

    /**
     * Splices {@code bytes} into the server&rarr;client stream immediately,
     * AHEAD of anything currently held back by a pause. The caller must ensure
     * no partial server packet is in flight (e.g. inject while paused at a
     * packet boundary) or the client's framing will be corrupted in an
     * uncontrolled way.
     *
     * @param bytes the raw bytes to deliver to the client
     * @throws IOException if writing to the client fails
     */
    void injectServerToClient(byte[] bytes) throws IOException {
        synchronized (lock) {
            for (OutputStream out : clientOutputs) {
                out.write(bytes);
                out.flush();
            }
        }
    }

    /** Stops accepting, closes every socket and lets the pump threads die. */
    @Override
    public void close() {
        running = false;
        closeQuietly(serverSocket);
        synchronized (lock) {
            for (Socket s : sockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // best effort
                }
            }
            sockets.clear();
            clientOutputs.clear();
        }
    }

    // --- internals --------------------------------------------------------

    private void acceptLoop() {
        while (running) {
            final Socket client;
            final Socket server;
            try {
                client = serverSocket.accept();
                client.setTcpNoDelay(true);
                server = new Socket(upstreamHost, upstreamPort);
                server.setTcpNoDelay(true);
            } catch (IOException e) {
                return; // listener closed (shutdown) or upstream unreachable
            }
            final OutputStream toClient;
            try {
                toClient = client.getOutputStream();
            } catch (IOException e) {
                closeQuietly(client);
                closeQuietly(server);
                continue;
            }
            synchronized (lock) {
                sockets.add(client);
                sockets.add(server);
                clientOutputs.add(toClient);
            }
            pump("fault-proxy-c2s", client, server, false, null);
            pump("fault-proxy-s2c", server, client, true, toClient);
        }
    }

    /**
     * Starts a daemon thread copying {@code from} to {@code to}. The
     * server&rarr;client direction ({@code faultable == true}) honors the
     * pause/buffer machinery; the client&rarr;server direction is a plain copy.
     */
    private void pump(String name, Socket from, Socket to, boolean faultable, OutputStream out) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[8192];
            try {
                InputStream in = from.getInputStream();
                OutputStream target = faultable ? out : to.getOutputStream();
                while (running) {
                    int n = in.read(buf);
                    if (n < 0) {
                        break;
                    }
                    if (faultable) {
                        synchronized (lock) {
                            if (paused) {
                                heldBack.write(buf, 0, n);
                                continue;
                            }
                            target.write(buf, 0, n);
                            target.flush();
                        }
                    } else {
                        target.write(buf, 0, n);
                        target.flush();
                    }
                }
            } catch (IOException ignored) {
                // Socket closed / reset: terminate this direction.
            }
        }, name);
        t.setDaemon(true);
        t.start();
    }

    private static void closeQuietly(ServerSocket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // best effort
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // best effort
        }
    }
}
