package io.github.danielbunting.clickhouse.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the {@link ServerPacket} wire-code table and {@link ServerPacket#fromCode}.
 * {@code fromCode} returning {@code null} for an unrecognised code is what drives the
 * "Unknown server packet code" {@code ProtocolException} in {@code NativeClientImpl.readMessage}
 * (and poisons the connection), so both the known mappings and the unknown-code path are pinned.
 */
class ServerPacketTest {

    @Test
    void knownCodesRoundTripThroughFromCode() {
        for (ServerPacket p : ServerPacket.values()) {
            assertEquals(p, ServerPacket.fromCode(p.code),
                    "fromCode must resolve every declared packet's own code");
        }
    }

    @Test
    void wireCodesMatchTheFrozenProtocolContract() {
        // Spot-check the codes against the native protocol contract, including the packets
        // that readMessage recognises but does not handle (9, 12, 13).
        assertEquals(1, ServerPacket.DATA.code);
        assertEquals(2, ServerPacket.EXCEPTION.code);
        assertEquals(7, ServerPacket.TOTALS.code);
        assertEquals(8, ServerPacket.EXTREMES.code);
        assertEquals(9, ServerPacket.TABLES_STATUS_RESPONSE.code);
        assertEquals(10, ServerPacket.LOG.code);
        assertEquals(11, ServerPacket.TABLE_COLUMNS.code);
        assertEquals(12, ServerPacket.PART_UUIDS.code);
        assertEquals(13, ServerPacket.READ_TASK_REQUEST.code);
        assertEquals(14, ServerPacket.PROFILE_EVENTS.code);
    }

    @Test
    void unknownCodesResolveToNull() {
        // 15 is one past the last defined code; these feed the "Unknown server packet code" path.
        assertNull(ServerPacket.fromCode(15));
        assertNull(ServerPacket.fromCode(999));
        assertNull(ServerPacket.fromCode(-1));
    }
}
