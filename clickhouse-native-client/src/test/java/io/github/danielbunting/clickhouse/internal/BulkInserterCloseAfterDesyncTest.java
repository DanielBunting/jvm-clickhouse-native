package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.protocol.ServerHello;
import io.github.danielbunting.clickhouse.protocol.ServerPacket;
import io.github.danielbunting.clickhouse.types.Column;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit-level fault injection for {@link BulkInserterImpl}'s close-after-failed-complete
 * path, driven by a scripted {@link NativeClient} fake (no server, no socket).
 *
 * <p>The fake scripts {@code readMessage()} responses and records every
 * {@code sendData}/{@code sendEmptyData}/{@code markPoisoned} call, so the test can
 * observe exactly what the inserter puts on the "wire" when {@code complete()}'s
 * post-terminator drain hits a validly-parsed but unexpected packet (a stale
 * {@code PONG} — precisely what a previously desynced ping leaves behind, see
 * knownBug 45).
 */
class BulkInserterCloseAfterDesyncTest {

    /** Row type for the inserter; the record component matches the sample column name. */
    public record NRow(long n) {}

    /**
     * Scripted {@link NativeClient}: returns canned {@link ServerMessage}s from a queue
     * and records the calls the inserter makes. Mirrors the real client's poisoning
     * semantics for the scripted flow: returning a validly-parsed packet (even an
     * unexpected one, like PONG) does NOT poison — {@code NativeClientImpl.readMessage}
     * only poisons on framing/I-O failures raised inside itself.
     */
    static final class ScriptedClient implements NativeClient {

        final Deque<ServerMessage> script = new ArrayDeque<>();
        int sendEmptyDataCalls;
        int sendDataCalls;
        boolean poisoned;

        @Override
        public ServerHello serverHello() {
            return new ServerHello("scripted", 25, 8, 0, 54_468L, "UTC", "scripted");
        }

        @Override
        public ZoneId serverTimezone() {
            return ZoneId.of("UTC");
        }

        @Override
        public void sendQuery(String sql) {
            // recorded implicitly: init() sends the INSERT header; nothing to assert on it
        }

        @Override
        public void sendQuery(String sql, Map<String, String> settings) {
            sendQuery(sql);
        }

        @Override
        public void sendQuery(String sql, QueryParameters params) {
            sendQuery(sql);
        }

        @Override
        public void sendQuery(String sql, Map<String, String> settings, QueryParameters params) {
            sendQuery(sql);
        }

        @Override
        public void sendData(Block block) {
            sendDataCalls++;
        }

        @Override
        public boolean ping() {
            return !poisoned;
        }

        @Override
        public void sendEmptyData() {
            sendEmptyDataCalls++;
        }

        @Override
        public ServerMessage readMessage() {
            ServerMessage next = script.poll();
            if (next == null) {
                throw new IllegalStateException("scripted client: no more scripted messages");
            }
            return next;
        }

        @Override
        public void cancel() {
            // not exercised
        }

        @Override
        public boolean isPoisoned() {
            return poisoned;
        }

        @Override
        public void markPoisoned() {
            poisoned = true;
        }

        @Override
        public void close() {
            // not exercised
        }
    }

    private static Block sampleBlock() {
        Block sample = new Block();
        sample.addColumn(new Column("n", "Int64"));
        sample.rowCount(0);
        return sample;
    }

    /**
     * After {@code complete()} fails with a {@link ProtocolException} because its
     * post-terminator drain read a validly-parsed but UNEXPECTED packet (e.g. a stale
     * {@code PONG}), the wire is desynced relative to the INSERT state machine —
     * {@code complete()} marks the client poisoned, so {@code close()}'s
     * graceful-termination gate does NOT send a second terminating empty block (the
     * first one already went out in {@code complete()}) and pools discard the
     * connection instead of recycling a desynced wire (was knownBug 46).
     *
     * <p>The script still offers an {@code END_OF_STREAM} after the stale PONG so
     * that a regressing second drain would be observed deterministically rather than
     * masked by a script under-run.
     */
    @Test
    void closeDoesNotSendSecondTerminatorAfterCompleteDesync() {
        ScriptedClient client = new ScriptedClient();
        // init(): the INSERT sample/header block defining one Int64 column "n".
        client.script.add(ServerMessage.data(sampleBlock()));
        // complete()'s drain: a validly-parsed but unexpected PONG -> ProtocolException.
        client.script.add(ServerMessage.of(ServerPacket.PONG));
        // Only consumed by close()'s buggy second drain; the correct close() never reads.
        client.script.add(ServerMessage.of(ServerPacket.END_OF_STREAM));

        BulkInserterImpl<NRow> inserter = new BulkInserterImpl<>(client, "t_kb46", NRow.class, 16);
        inserter.init();
        inserter.add(new NRow(7L));

        ProtocolException e = assertThrows(ProtocolException.class, inserter::complete,
                "complete() must surface the unexpected packet as a ProtocolException");
        assertTrue(e.getMessage().contains("PONG"),
                "sanity: the drain failed on the scripted stale PONG, got: " + e.getMessage());
        assertEquals(1, client.sendEmptyDataCalls,
                "sanity: complete() sent exactly one terminating empty block before failing");

        inserter.close();

        assertAll(
                () -> assertEquals(1, client.sendEmptyDataCalls,
                        "close() after complete()'s protocol desync must NOT send a second "
                                + "terminating empty Data block"),
                () -> assertTrue(client.poisoned,
                        "the desynced connection must be poisoned so pools discard it "
                                + "instead of recycling an out-of-sync wire"));
    }

    /**
     * Abandoning an initialized-but-uncompleted INSERT via {@code close()} terminates
     * it gracefully: buffered-but-unflushed rows are DISCARDED (abandonment must not
     * commit surprise data), the terminating empty block is sent, and the response is
     * drained through EVERY tolerated trailing packet kind (data/totals/extremes
     * echoes plus the informational progress/profile/log family) to
     * {@code END_OF_STREAM} — leaving the connection healthy (NOT poisoned) and
     * reusable by a pool.
     */
    @Test
    void closeWithoutCompleteTerminatesInsertGracefully() {
        ScriptedClient client = new ScriptedClient();
        // init(): the INSERT sample/header block.
        client.script.add(ServerMessage.data(sampleBlock()));
        // close()'s drain: the full set of tolerated trailing packets, then the end.
        client.script.add(ServerMessage.of(ServerPacket.DATA));
        client.script.add(ServerMessage.of(ServerPacket.TOTALS));
        client.script.add(ServerMessage.of(ServerPacket.EXTREMES));
        client.script.add(ServerMessage.of(ServerPacket.PROGRESS));
        client.script.add(ServerMessage.of(ServerPacket.PROFILE_INFO));
        client.script.add(ServerMessage.of(ServerPacket.LOG));
        client.script.add(ServerMessage.of(ServerPacket.PROFILE_EVENTS));
        client.script.add(ServerMessage.of(ServerPacket.END_OF_STREAM));

        BulkInserterImpl<NRow> inserter = new BulkInserterImpl<>(client, "t_close", NRow.class, 16);
        inserter.init();
        inserter.add(new NRow(7L)); // buffered, never flushed

        inserter.close();

        assertAll(
                () -> assertEquals(0, client.sendDataCalls,
                        "abandoned buffered rows must be discarded, not flushed as data"),
                () -> assertEquals(1, client.sendEmptyDataCalls,
                        "close() must send exactly one terminating empty Data block"),
                () -> assertFalse(client.poisoned,
                        "a gracefully terminated INSERT leaves the wire clean — the "
                                + "connection must stay reusable"));
    }

    /**
     * A server {@code Exception} packet met while {@code close()} drains the abandoned
     * INSERT is terminal but IN-SPEC: the INSERT is over either way, so {@code close()}
     * must return quietly (never mask the caller's original error with a secondary
     * throw) and must NOT poison — the exception was a clean protocol exchange, the
     * wire position is known.
     */
    @Test
    void serverExceptionDuringCloseDrainEndsQuietlyWithoutPoisoning() {
        ScriptedClient client = new ScriptedClient();
        client.script.add(ServerMessage.data(sampleBlock()));
        // close()'s drain: the server rejects the aborted INSERT. Deliberately the LAST
        // scripted message — a regressing over-drain would hit the fake's script
        // under-run failure and surface as poisoning.
        client.script.add(ServerMessage.exception(
                new io.github.danielbunting.clickhouse.ServerException(
                        241, "DB::Exception", "Memory limit exceeded", null)));

        BulkInserterImpl<NRow> inserter = new BulkInserterImpl<>(client, "t_exc", NRow.class, 16);
        inserter.init();
        inserter.add(new NRow(7L));

        inserter.close(); // must not throw

        assertAll(
                () -> assertEquals(1, client.sendEmptyDataCalls,
                        "the graceful terminator still went out before the server error"),
                () -> assertFalse(client.poisoned,
                        "a clean server Exception during the drain is in-spec and must "
                                + "not poison the connection"));
    }

    /**
     * A validly-parsed but protocol-violating packet (a stale {@code PONG}) during
     * {@code close()}'s graceful-termination drain proves the wire is desynced: the
     * drain's {@link ProtocolException} is swallowed by {@code close()} (close never
     * throws for this) but the connection MUST be marked poisoned so a pool discards
     * it instead of recycling an out-of-sync stream.
     */
    @Test
    void unexpectedPacketDuringCloseDrainPoisonsQuietly() {
        ScriptedClient client = new ScriptedClient();
        client.script.add(ServerMessage.data(sampleBlock()));
        // close()'s drain: a stale PONG where only INSERT-termination packets belong.
        client.script.add(ServerMessage.of(ServerPacket.PONG));

        BulkInserterImpl<NRow> inserter = new BulkInserterImpl<>(client, "t_pong", NRow.class, 16);
        inserter.init();
        inserter.add(new NRow(7L));

        inserter.close(); // must not throw — the failure is remembered as poison

        assertAll(
                () -> assertEquals(1, client.sendEmptyDataCalls,
                        "close() attempted the graceful terminator before the desync"),
                () -> assertTrue(client.poisoned,
                        "an unexpected packet mid-termination leaves an unknown wire "
                                + "offset: close() must poison so the pool discards"));
    }
}
