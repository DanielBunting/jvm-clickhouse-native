package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.internal.NativeClientImpl;
import io.github.danielbunting.clickhouse.internal.ServerMessage;
import io.github.danielbunting.clickhouse.protocol.ServerPacket;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Fault injection for {@link NativeClientImpl#ping()} through a byte-level TCP proxy
 * ({@link FaultInjectingProxy}) in front of the real ClickHouse container. The proxy
 * can hold back server&rarr;client bytes (so a Pong arrives late) and splice bytes
 * into the server&rarr;client stream at a packet boundary (so the client reads a
 * validly-framed packet the state machine does not expect).
 *
 * <p>Packet-id injection is safe here because {@code NativeClientImpl.readMessage}
 * reads the packet code as a plain VarUInt on the raw socket reader — compression
 * framing only wraps block <i>bodies</i>, never packet ids — and both injected
 * ({@code END_OF_STREAM}) and held-back ({@code PONG}) packets are single-byte,
 * body-less packets.
 */
@Tag("integration")
class PingFaultInjectionIT extends IntegrationTestBase {

    private static ClickHouseConfig proxiedConfig(FaultInjectingProxy proxy) {
        return ClickHouseConfig.builder()
                .host(proxy.host())
                .port(proxy.port())
                .socketTimeout(Duration.ofMillis(500))
                .build();
    }

    /**
     * When {@code ping()} reads a validly-parsed but UNEXPECTED packet (the
     * unexpected-packet arm of its packet loop), it has consumed a packet the
     * connection's state machine did not account for while the real Pong is still
     * in flight — the wire is dirty, so the connection is marked poisoned
     * ({@code isPoisoned() == true}) and never returns to a pool (was knownBug 45).
     * Poisoning is exactly the signal {@code ClickHouseConnectionPool} uses for its
     * return-path hygiene (it only discards on {@code isPoisoned()}), so a false
     * ping can never leave a desynced stream eligible for recycling. The write-half
     * of ping poisons the same way; the read-timeout variant is covered by
     * {@code readMessage()}'s own IOException poisoning (pinned by
     * {@link #timedOutPingAlreadyPoisonsViaReadMessage()}).
     *
     * <p>Scenario driven here: a healthy connection through the proxy; pause the
     * server&rarr;client direction and splice a single {@code END_OF_STREAM} packet id
     * (0x05 — verified in {@code ServerPacket}) into the stream at the packet
     * boundary. {@code ping()} writes Ping, the server's real Pong is held back, and
     * the client reads the injected EOS: a clean parse that hits ping's
     * {@code else -> return false} arm. On resume the genuine Pong is delivered and
     * sits un-consumed in the receive buffer — the test proves it by reading it back
     * verbatim via {@code readMessage()}.
     *
     * <p>Asserted via {@code isPoisoned()} — the exact predicate the pool consults,
     * independent of which packet types a follow-up operation happens to tolerate
     * ({@code executeScalar}'s drain loop, for example, silently skips a stale PONG,
     * masking the desync until a stricter consumer or a body-carrying stale packet
     * corrupts a result).
     */
    @Test
    void pingConsumingUnexpectedPacketPoisonsConnection() throws Exception {
        try (FaultInjectingProxy proxy = new FaultInjectingProxy(clickHouseHost(), clickHousePort())) {
            ClickHouseConfig config = proxiedConfig(proxy);
            try (NativeClientImpl client = new NativeClientImpl(config)) {
                assertTrue(client.ping(), "sanity: ping through the un-faulted proxy succeeds");

                // Hold back the genuine Pong; splice a valid END_OF_STREAM packet id at
                // the (quiescent) packet boundary instead.
                proxy.pauseServerToClient();
                proxy.injectServerToClient(new byte[] {(byte) ServerPacket.END_OF_STREAM.code});
                boolean alive = client.ping();
                proxy.resumeServerToClient(); // the real, now-stale Pong reaches the client

                assertFalse(alive, "ping() must report false on an unexpected packet");

                // Proof of the dirty wire: the genuine Pong is still there, un-consumed.
                ServerMessage stale = client.readMessage();
                assertEquals(ServerPacket.PONG, stale.type(),
                        "the failed ping left the genuine Pong un-consumed on the wire");

                assertTrue(client.isPoisoned(),
                        "a ping that consumed an unexpected packet (with the real Pong still "
                                + "in flight) left the wire desynced: the connection must be "
                                + "poisoned so the pool discards it instead of recycling it "
                                + "into the next borrower's reads");

                // Once poisoned, ping() short-circuits to false WITHOUT touching the
                // socket — probing a known-desynced wire could only desync it further.
                assertFalse(client.ping(),
                        "a poisoned connection must answer false immediately");
            }
        }
    }

    /**
     * Companion regression pin for the read-timeout flavour of the knownBug 45 review
     * finding: a ping whose Pong is delayed past {@code socketTimeout} DOES already
     * poison the connection — the {@code SocketTimeoutException} is caught inside
     * {@code readMessage()}, which sets {@code poisoned} before wrapping it in a
     * {@code ConnectionException} that {@code ping()}'s catch then swallows into
     * {@code false}. This test passes today and must keep passing after the
     * knownBug 45 fix (which addresses the arms {@code readMessage()} cannot cover).
     */
    @Test
    void timedOutPingAlreadyPoisonsViaReadMessage() throws Exception {
        try (FaultInjectingProxy proxy = new FaultInjectingProxy(clickHouseHost(), clickHousePort())) {
            ClickHouseConfig config = proxiedConfig(proxy);
            try (NativeClientImpl client = new NativeClientImpl(config)) {
                assertTrue(client.ping(), "sanity: ping through the un-faulted proxy succeeds");

                proxy.pauseServerToClient();
                boolean alive = client.ping(); // Pong held back -> soTimeout inside readMessage
                proxy.resumeServerToClient();

                assertFalse(alive, "ping() must report false when the Pong never arrived in time");
                assertTrue(client.isPoisoned(),
                        "readMessage() poisons on the socket-timeout IOException, so the "
                                + "timed-out ping leaves the connection correctly discarded-on-return");
            }
        }
    }

    /**
     * {@code ping()} tolerates informational packets interleaved BEFORE the Pong (the
     * documented keep-draining arm, mirroring the official client): a {@code PROGRESS}
     * packet spliced into the stream ahead of the genuine Pong is skipped, the Pong is
     * then consumed, and the probe answers {@code true} with the connection left clean
     * (un-poisoned and fully usable).
     *
     * <p>The injected bytes are a validly-framed empty Progress packet for the
     * negotiated revision (&ge; 54,453 against a modern server): packet id 0x03
     * followed by six zero VarUInts — rows, bytes, total_rows, written_rows,
     * written_bytes, elapsed_ns. Injection happens while the connection is quiescent,
     * i.e. at a packet boundary, exactly as documented on
     * {@link FaultInjectingProxy#injectServerToClient}.
     */
    @Test
    void pingSkipsInterleavedProgressBeforePong() throws Exception {
        try (FaultInjectingProxy proxy = new FaultInjectingProxy(clickHouseHost(), clickHousePort())) {
            ClickHouseConfig config = proxiedConfig(proxy);
            try (NativeClientImpl client = new NativeClientImpl(config)) {
                // A stale Progress packet sits in the receive buffer ahead of the ping.
                proxy.injectServerToClient(new byte[] {
                        (byte) ServerPacket.PROGRESS.code, 0, 0, 0, 0, 0, 0});

                assertTrue(client.ping(),
                        "ping() must skip the interleaved Progress and find the Pong");
                assertFalse(client.isPoisoned(),
                        "skipping an informational packet is in-spec — no poisoning");
                assertTrue(client.ping(),
                        "the wire is byte-clean afterwards: a follow-up ping still succeeds");
            }
        }
    }

    /**
     * {@code ping()} on a CLOSED connection answers {@code false} immediately and
     * never throws — the closed/poisoned guard short-circuits before any socket I/O,
     * so pool hygiene code can probe unconditionally.
     */
    @Test
    void pingOnClosedConnectionReturnsFalse() {
        ClickHouseConfig config = ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .build();
        NativeClientImpl client = new NativeClientImpl(config);
        assertTrue(client.ping(), "sanity: the freshly opened connection answers the probe");
        client.close();
        assertFalse(client.ping(), "a closed connection reports false, no throw");
    }
}
