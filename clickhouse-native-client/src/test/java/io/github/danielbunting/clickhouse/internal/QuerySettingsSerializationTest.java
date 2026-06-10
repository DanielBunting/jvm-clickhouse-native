package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryReader;
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the Query-packet settings slot serializer
 * ({@link NativeClientImpl#writeSettings}). These capture the bytes a
 * {@link io.github.danielbunting.clickhouse.protocol.BinaryWriter} produces and read them
 * back, asserting the ClickHouse settings-as-strings-with-flags wire form: for each
 * setting a name string, a flags VarUInt (0 for a normal setting), and a value string,
 * terminated by an empty name. No socket/server involved.
 */
class QuerySettingsSerializationTest {

    /** A modern revision (CH 25.x) — past DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS. */
    private static final int MODERN_REVISION = 54_470;

    private static byte[] serialize(Map<String, String> settings, int revision) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DefaultBinaryWriter writer = new DefaultBinaryWriter(bytes);
        NativeClientImpl.writeSettings(writer, settings, revision);
        writer.flush();
        return bytes.toByteArray();
    }

    /** Reads back the settings slot: (name, flags, value)* terminated by an empty name. */
    private static Map<String, String> readBack(byte[] wire) throws IOException {
        DefaultBinaryReader reader = new DefaultBinaryReader(new ByteArrayInputStream(wire));
        Map<String, String> out = new LinkedHashMap<>();
        while (true) {
            String name = reader.readString();
            if (name.isEmpty()) {
                break;
            }
            long flags = reader.readVarUInt();
            assertEquals(0L, flags, "normal setting must have flags == 0");
            String value = reader.readString();
            out.put(name, value);
        }
        // The terminator must be the last byte consumed — nothing should remain.
        assertEquals(-1, reader.readByteOrEof(), "no trailing bytes after settings terminator");
        return out;
    }

    @Test
    void emptySettingsWritesOnlyTerminator() throws IOException {
        // One empty string (the terminator) is a single zero-length VarUInt prefix: 0x00.
        byte[] wire = serialize(new LinkedHashMap<>(), MODERN_REVISION);
        assertEquals(1, wire.length);
        assertEquals(0, wire[0]);
    }

    @Test
    void nullSettingsWritesOnlyTerminator() throws IOException {
        byte[] wire = serialize(null, MODERN_REVISION);
        assertEquals(1, wire.length);
        assertEquals(0, wire[0]);
    }

    @Test
    void settingsRoundTripInOrderWithZeroFlags() throws IOException {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("max_execution_time", "30");
        settings.put("max_memory_usage", "1000000000");
        settings.put("async_insert", "1");

        byte[] wire = serialize(settings, MODERN_REVISION);
        Map<String, String> readBack = readBack(wire);

        assertEquals(settings, readBack);
        // Insertion order must be preserved on the wire.
        assertEquals("[max_execution_time, max_memory_usage, async_insert]",
                readBack.keySet().toString());
    }

    @Test
    void olderRevisionRejectsSettingsButAllowsEmpty() throws IOException {
        int oldRevision = 54_400; // below DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("max_threads", "4");

        assertThrows(ProtocolException.class, () -> serialize(settings, oldRevision));

        // Empty/none settings are fine on any revision: just the terminator.
        byte[] wire = serialize(new LinkedHashMap<>(), oldRevision);
        assertEquals(1, wire.length);
        assertEquals(0, wire[0]);
    }

    @Test
    void nullValueSerializedAsEmptyString() throws IOException {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("some_flag", null);
        Map<String, String> readBack = readBack(serialize(settings, MODERN_REVISION));
        assertTrue(readBack.containsKey("some_flag"));
        assertEquals("", readBack.get("some_flag"));
    }

    @Test
    void mergeAlwaysIncludesFlattenedSerializationDefault() {
        Map<String, String> merged = NativeClientImpl.mergeSettings(null, null);
        assertEquals("1", merged.get(NativeClientImpl.FLATTENED_SERIALIZATION_SETTING));
        assertEquals(1, merged.size());
    }

    @Test
    void perQueryOverridesConnectionDefaults() {
        Map<String, String> connection = new LinkedHashMap<>();
        connection.put("max_execution_time", "30");
        connection.put("max_threads", "4");

        Map<String, String> perQuery = new LinkedHashMap<>();
        perQuery.put("max_execution_time", "5"); // overrides the connection default
        perQuery.put("async_insert", "1");        // adds a new setting

        Map<String, String> merged = NativeClientImpl.mergeSettings(connection, perQuery);

        assertEquals("5", merged.get("max_execution_time")); // query wins
        assertEquals("4", merged.get("max_threads"));         // connection-only survives
        assertEquals("1", merged.get("async_insert"));        // query-only added
        assertEquals("1", merged.get(NativeClientImpl.FLATTENED_SERIALIZATION_SETTING));
    }

    @Test
    void callerMayOverrideFlattenedSerializationDefault() {
        Map<String, String> perQuery = new LinkedHashMap<>();
        perQuery.put(NativeClientImpl.FLATTENED_SERIALIZATION_SETTING, "0");
        Map<String, String> merged = NativeClientImpl.mergeSettings(null, perQuery);
        assertEquals("0", merged.get(NativeClientImpl.FLATTENED_SERIALIZATION_SETTING));
    }
}
