package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.QueryResult;
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
 * Unit tests for {@link ClickHouseConnectionImpl} that need no server: the pure
 * scalar-coercion helper ({@code coerceLong}), and — via a scripted
 * {@link NativeClient} fake — the combined {@code (sql, params, settings)} overloads,
 * which must forward BOTH the per-query settings and the server-side parameter
 * bindings into the single unified {@code sendQuery(sql, settings, params)} transport
 * call (neither side may be dropped on the way down).
 */
class ClickHouseConnectionImplTest {

    /**
     * Minimal scripted transport: replays canned {@link ServerMessage}s and records
     * the exact (settings, params) pair each {@code sendQuery} received. Does NOT
     * override {@link NativeClient#ping()}, so the interface default is observable.
     */
    private static final class RecordingClient implements NativeClient {

        final Deque<ServerMessage> script = new ArrayDeque<>();
        String lastSql;
        Map<String, String> lastSettings;
        QueryParameters lastParams;

        @Override
        public ServerHello serverHello() {
            return new ServerHello("recording", 25, 8, 0, 54_468L, "UTC", "recording");
        }

        @Override
        public ZoneId serverTimezone() {
            return ZoneId.of("UTC");
        }

        @Override
        public void sendQuery(String sql, Map<String, String> settings, QueryParameters params) {
            lastSql = sql;
            lastSettings = settings;
            lastParams = params;
        }

        @Override
        public void sendData(Block block) {
            // not exercised
        }

        @Override
        public void sendEmptyData() {
            // not exercised
        }

        @Override
        public ServerMessage readMessage() {
            ServerMessage next = script.poll();
            if (next == null) {
                throw new IllegalStateException("recording client: no more scripted messages");
            }
            return next;
        }

        @Override
        public void cancel() {
            // not exercised
        }

        @Override
        public boolean isPoisoned() {
            return false;
        }

        @Override
        public void markPoisoned() {
            // not exercised
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    private static Block headerBlock() {
        Block header = new Block();
        header.addColumn(new Column("v", "UInt8"));
        header.rowCount(0);
        return header;
    }

    /**
     * {@code execute(sql, params, settings)} must pass the settings map AND the
     * parameter bindings through to the transport in one {@code sendQuery} — the
     * combined overload exists precisely so neither leg is lost.
     */
    @Test
    void combinedExecuteForwardsBothSettingsAndParams() {
        RecordingClient client = new RecordingClient();
        client.script.add(ServerMessage.of(ServerPacket.END_OF_STREAM));

        Map<String, String> settings = Map.of("max_execution_time", "7");
        QueryParameters params = QueryParameters.of(Map.of("n", 42));
        new ClickHouseConnectionImpl(client)
                .execute("INSERT INTO t SELECT {n:UInt32}", params, settings);

        assertEquals("INSERT INTO t SELECT {n:UInt32}", client.lastSql);
        assertEquals(settings, client.lastSettings,
                "the per-query settings must reach the transport untouched");
        assertEquals(params, client.lastParams,
                "the server-side bindings must reach the transport untouched");
    }

    /**
     * {@code query(sql, params, settings)} likewise forwards both legs, hands back a
     * lazy result over the response stream, and releases the connection for the next
     * operation once that result is closed.
     */
    @Test
    void combinedQueryForwardsBothSettingsAndParams() {
        RecordingClient client = new RecordingClient();
        client.script.add(ServerMessage.data(headerBlock())); // schema header
        client.script.add(ServerMessage.of(ServerPacket.END_OF_STREAM));

        Map<String, String> settings = Map.of("max_block_size", "1000");
        QueryParameters params = QueryParameters.of(Map.of("n", 5));
        ClickHouseConnectionImpl conn = new ClickHouseConnectionImpl(client);
        try (QueryResult result = conn.query("SELECT {n:UInt32} AS v", params, settings)) {
            assertEquals("SELECT {n:UInt32} AS v", client.lastSql);
            assertEquals(settings, client.lastSettings);
            assertEquals(params, client.lastParams);
            assertEquals(java.util.List.of("v"), result.columnNames(),
                    "the lazy result exposes the scripted header schema");
        }

        // The guard was released by closing the result: a follow-up operation is legal.
        client.script.add(ServerMessage.of(ServerPacket.END_OF_STREAM));
        conn.execute("SELECT 1", QueryParameters.EMPTY, Map.of("a", "b"));
        assertEquals("SELECT 1", client.lastSql);
    }

    /**
     * {@link NativeClient#ping()}'s interface DEFAULT reports {@code false}: a
     * transport that never implemented the protocol-level Ping/Pong probe must present
     * itself as not-provably-alive (so a pool discards rather than trusts it) instead
     * of throwing or claiming liveness it cannot verify.
     */
    @Test
    void nativeClientPingDefaultsToFalse() {
        assertFalse(new RecordingClient().ping(),
                "a transport without a real Ping/Pong implementation must answer false");
    }

    @Test
    void coercesIntegerNumbers() {
        assertEquals(42L, ClickHouseConnectionImpl.coerceLong(42));
        assertEquals(42L, ClickHouseConnectionImpl.coerceLong((short) 42));
        assertEquals(42L, ClickHouseConnectionImpl.coerceLong((byte) 42));
        assertEquals(9_999_999_999L, ClickHouseConnectionImpl.coerceLong(9_999_999_999L));
    }

    @Test
    void coercesFloatingPointByTruncation() {
        assertEquals(3L, ClickHouseConnectionImpl.coerceLong(3.9d));
        assertEquals(2L, ClickHouseConnectionImpl.coerceLong(2.1f));
    }

    @Test
    void coercesBoolean() {
        assertEquals(1L, ClickHouseConnectionImpl.coerceLong(Boolean.TRUE));
        assertEquals(0L, ClickHouseConnectionImpl.coerceLong(Boolean.FALSE));
    }

    @Test
    void coercesNumericString() {
        assertEquals(123L, ClickHouseConnectionImpl.coerceLong("123"));
        assertEquals(-7L, ClickHouseConnectionImpl.coerceLong("  -7 "));
    }

    @Test
    void rejectsNull() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConnectionImpl.coerceLong(null));
    }

    @Test
    void rejectsNonNumericString() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConnectionImpl.coerceLong("abc"));
    }

    @Test
    void rejectsUncoercibleType() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConnectionImpl.coerceLong(new Object()));
    }
}
