package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * Adversarial escaping coverage for the query-parameter Field-dump emitted by
 * {@link NativeClientImpl#writeQueryParameters} (via {@code dumpFieldValue}).
 * {@link QueryParametersWireTest} covers the happy wire form; this pins the escaping
 * contract: a String value is dumped as a single-quoted literal with backslash and single
 * quote escaped (FieldVisitorDump form), while other characters (newline, unicode) pass
 * through verbatim. Reading the wire string back proves what the server's
 * {@code Field::restoreFromDump} will receive. No socket / no server.
 */
class QueryParameterEscapingTest {

    /** Serializes a single {@code {name: value}} binding and returns the dumped value string. */
    private static String dumpedValueOf(Object value) throws IOException {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("p", value);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultBinaryWriter w = new DefaultBinaryWriter(baos);
        NativeClientImpl.writeQueryParameters(w, QueryParameters.of(p));
        w.flush();
        DefaultBinaryReader r = new DefaultBinaryReader(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals("p", r.readString());   // name
        r.readVarUInt();                      // custom-setting flag
        return r.readString();                // the Field dump
    }

    @Test
    void singleQuoteIsBackslashEscaped() throws IOException {
        // O'Brien -> 'O\'Brien'
        assertEquals("'O\\'Brien'", dumpedValueOf("O'Brien"));
    }

    @Test
    void backslashIsDoubled() throws IOException {
        // input is a single backslash between a and b -> 'a\\b' (doubled)
        assertEquals("'a\\\\b'", dumpedValueOf("a\\b"));
    }

    @Test
    void backslashThenQuoteBothEscaped() throws IOException {
        // input: backslash then single quote -> each escaped independently
        assertEquals("'\\\\\\''", dumpedValueOf("\\'"));
    }

    @Test
    void newlineIsNotEscaped_passesThroughVerbatim() throws IOException {
        // dumpFieldValue only escapes \\ and '; a newline is left as-is inside the quotes.
        assertEquals("'line1\nline2'", dumpedValueOf("line1\nline2"));
    }

    @Test
    void unicodeIsPreserved() throws IOException {
        assertEquals("'café ☃'", dumpedValueOf("café ☃"));
    }

    @Test
    void nullBindingDumpsAsBareNullToken() throws IOException {
        // A null value becomes the \N wire sentinel, which dumps as the bare token NULL.
        assertEquals("NULL", dumpedValueOf(null));
    }
}
