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
 * {@link QueryParametersWireTest} covers the happy wire form; this pins the two-layer
 * escaping contract. Layer 1 ({@code QueryParameters.toText}): the VALUE text is put into
 * ClickHouse ESCAPED form — backslashes and control characters get backslash escapes —
 * because the server escape-parses the restored value against the placeholder type (a raw
 * backslash is escape-interpreted into corruption and a raw newline aborts with
 * BAD_QUERY_PARAMETER; proven against a live server). Layer 2 ({@code dumpFieldValue}): the
 * whole value is dumped as a single-quoted Field literal with backslash and single quote
 * escaped (FieldVisitorDump form), which {@code Field::restoreFromDump} exactly reverses.
 * Reading the wire string back proves what the server's restore will yield. No socket /
 * no server.
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
    void backslashIsQuadrupled() throws IOException {
        // input: a single backslash between a and b. Layer 1 doubles it (ESCAPED value form,
        // so the server's escape-parse restores exactly one), layer 2 doubles each again for
        // the Field dump -> 'a\\\\b'. (The old single-doubling wire form made the server
        // escape-interpret the raw backslash: "a\b" silently became a + backspace.)
        assertEquals("'a\\\\\\\\b'", dumpedValueOf("a\\b"));
    }

    @Test
    void backslashThenQuoteBothEscaped() throws IOException {
        // input: backslash then single quote. Layer 1: \ -> \\ (the quote stays raw — quotes
        // are content for String params and structural for composite params). Layer 2 escapes
        // both backslashes and the quote for the dump.
        assertEquals("'\\\\\\\\\\''", dumpedValueOf("\\'"));
    }

    @Test
    void newlineIsEscapedBeforeDump() throws IOException {
        // Layer 1 turns the raw newline into backslash-n (the server's escaped-form parser
        // aborts on a raw newline with BAD_QUERY_PARAMETER); layer 2 then doubles the
        // backslash for the dump.
        assertEquals("'line1\\\\nline2'", dumpedValueOf("line1\nline2"));
    }

    @Test
    void unicodeIsPreserved() throws IOException {
        assertEquals("'café ☃'", dumpedValueOf("café ☃"));
    }

    @Test
    void nullBindingDumpsAsQuotedNullSentinel() throws IOException {
        // A null value becomes the \N wire sentinel, dumped like any other string:
        // '\N' with the backslash doubled — matching what clickhouse-client sends for
        // --param_x='\N'. The server parses the restored string per the placeholder
        // type, where \N means NULL for Nullable(T). (The earlier bare NULL Field
        // token was rejected by contexts that parse the value as quoted text, e.g.
        // INSERT ... VALUES — was bug 19.)
        assertEquals("'\\\\N'", dumpedValueOf(null));
    }
}
