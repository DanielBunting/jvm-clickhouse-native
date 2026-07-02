package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryReader;
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the query-parameters slot serialization of the Query packet
 * ({@link NativeClientImpl#writeQueryParameters}). These capture the bytes a
 * {@link DefaultBinaryWriter} emits and read them back, asserting the
 * custom-setting wire form: per entry {@code (name, flags=CUSTOM, value)} with an
 * empty-name terminator. No socket / no server.
 */
class QueryParametersWireTest {

    /** The custom-setting flag the serializer writes between a param name and value. */
    private static final long EXPECTED_CUSTOM_FLAG = 0x02;

    private static byte[] serialize(QueryParameters params) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultBinaryWriter w = new DefaultBinaryWriter(baos);
        NativeClientImpl.writeQueryParameters(w, params);
        w.flush();
        return baos.toByteArray();
    }

    private static DefaultBinaryReader reader(byte[] bytes) {
        return new DefaultBinaryReader(new ByteArrayInputStream(bytes));
    }

    @Test
    void emptyParamsEmitOnlyTerminator() throws IOException {
        byte[] bytes = serialize(QueryParameters.EMPTY);
        DefaultBinaryReader r = reader(bytes);
        // Just the empty terminator name: a single length-0 string == one zero byte.
        assertEquals("", r.readString());
        assertEquals(1, bytes.length);
    }

    @Test
    void singleParamEmitsNameFlagValueThenTerminator() throws IOException {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("_p1", "hello");
        byte[] bytes = serialize(QueryParameters.of(p));
        DefaultBinaryReader r = reader(bytes);

        assertEquals("_p1", r.readString());
        assertEquals(EXPECTED_CUSTOM_FLAG, r.readVarUInt());
        // Value is a ClickHouse Field dump (single-quoted String), not raw text — see
        // NativeClientImpl.dumpFieldValue; verified live in MergedFeaturesIT.
        assertEquals("'hello'", r.readString());
        assertEquals("", r.readString()); // terminator
    }

    @Test
    void multipleParamsPreserveInsertionOrder() throws IOException {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("name", "Alice");
        p.put("age", 42);
        p.put("score", 3.5);
        byte[] bytes = serialize(QueryParameters.of(p));
        DefaultBinaryReader r = reader(bytes);

        assertEquals("name", r.readString());
        assertEquals(EXPECTED_CUSTOM_FLAG, r.readVarUInt());
        assertEquals("'Alice'", r.readString());

        assertEquals("age", r.readString());
        assertEquals(EXPECTED_CUSTOM_FLAG, r.readVarUInt());
        assertEquals("'42'", r.readString());

        assertEquals("score", r.readString());
        assertEquals(EXPECTED_CUSTOM_FLAG, r.readVarUInt());
        assertEquals("'3.5'", r.readString());

        assertEquals("", r.readString()); // terminator
    }

    @Test
    void nullValueIsSerializedAsNullSentinel() throws IOException {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("maybe", null);
        byte[] bytes = serialize(QueryParameters.of(p));
        DefaultBinaryReader r = reader(bytes);

        assertEquals("maybe", r.readString());
        assertEquals(EXPECTED_CUSTOM_FLAG, r.readVarUInt());
        // A NULL binding dumps as the quoted STRING '\N' — exactly what
        // clickhouse-client sends for --param_x='\N'. The server parses the restored
        // string per the placeholder type, where \N means NULL for Nullable(T). (A
        // bare NULL Field token is rejected by contexts that parse the value as
        // quoted text, e.g. INSERT ... VALUES — verified live on 25.8, was bug 19.)
        assertEquals("'\\\\N'", r.readString());
        assertEquals("", r.readString());
    }

    @Test
    void valueTextConversionMatchesClickHouseForms() {
        assertEquals("42", QueryParameters.toText(42));
        assertEquals("42", QueryParameters.toText(42L));
        assertEquals("3.5", QueryParameters.toText(3.5));
        assertEquals("true", QueryParameters.toText(Boolean.TRUE));
        assertEquals("false", QueryParameters.toText(Boolean.FALSE));
        assertEquals("2020-01-02",
                QueryParameters.toText(java.time.LocalDate.of(2020, 1, 2)));
        assertEquals("2020-01-02 03:04:05",
                QueryParameters.toText(java.time.LocalDateTime.of(2020, 1, 2, 3, 4, 5)));
        assertEquals("0.10",
                QueryParameters.toText(new java.math.BigDecimal("0.10")));
        // String passes through unquoted (server quotes via {name:Type}).
        assertEquals("O'Brien", QueryParameters.toText("O'Brien"));
        assertTrue(QueryParameters.toText(null) == null);
    }
}
