package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the {@link QueryParameters.Builder} surface and the escape edges of
 * {@code toText} not exercised by {@link QueryParametersToTextTest}'s value-conversion matrix:
 * binding/lookup semantics, the verbatim {@code bindText} path, empty-set canonicalisation, and
 * the per-character ESCAPED-form branches.
 */
class QueryParametersBuilderTest {

    @Test
    @DisplayName("bind converts values; wireValue answers per binding state")
    void bindAndWireValueSemantics() {
        QueryParameters params = QueryParameters.builder()
                .bind("n", 42)
                .bind("nothing", null)
                .build();
        assertEquals("42", params.wireValue("n"));
        assertEquals("\\N", params.wireValue("nothing"), "a bound null answers the NULL sentinel");
        assertNull(params.wireValue("unbound"), "an unbound name answers null (not the sentinel)");
        assertEquals(2, params.asMap().size());
        assertTrue(params.asMap().containsKey("nothing"));
        assertNull(params.asMap().get("nothing"), "the map view keeps null (pre-sentinel) values");
    }

    @Test
    @DisplayName("bindText passes text through verbatim — no conversion, no escaping")
    void bindTextIsVerbatim() {
        QueryParameters params = QueryParameters.builder()
                .bindText("raw", "line\nbreak\\as-is")
                .bindText("nullText", null)
                .build();
        assertEquals("line\nbreak\\as-is", params.asMap().get("raw"),
                "bindText must not apply the ESCAPED-form conversion");
        assertEquals("\\N", params.wireValue("nullText"));
    }

    @Test
    @DisplayName("empty parameter names are rejected by bind and bindText")
    void emptyNamesRejected() {
        QueryParameters.Builder builder = QueryParameters.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.bind("", 1));
        assertThrows(IllegalArgumentException.class, () -> builder.bindText("", "x"));
    }

    @Test
    @DisplayName("empty builders and empty maps canonicalise to the shared EMPTY instance")
    void emptyCanonicalisesToSharedInstance() {
        assertSame(QueryParameters.EMPTY, QueryParameters.builder().build());
        assertSame(QueryParameters.EMPTY, QueryParameters.of(Map.of()));
        assertSame(QueryParameters.EMPTY, QueryParameters.of(null));
        assertTrue(QueryParameters.EMPTY.isEmpty());
    }

    @Test
    @DisplayName("every ESCAPED-form character branch renders its backslash escape")
    void escapedFormCharacterBranches() {
        assertEquals("back\\\\slash", QueryParameters.toText("back\\slash"));
        assertEquals("new\\nline", QueryParameters.toText("new\nline"));
        assertEquals("carriage\\rreturn", QueryParameters.toText("carriage\rreturn"));
        assertEquals("tab\\tstop", QueryParameters.toText("tab\tstop"));
        assertEquals("nul\\0byte", QueryParameters.toText("nul\u0000byte"));
        assertEquals("mixed\\\\\\n\\t", QueryParameters.toText("mixed\\\n\t"));
        // Quotes stay raw: content for String params, structural for composite params.
        assertEquals("it's \"quoted\"", QueryParameters.toText("it's \"quoted\""));
        // The fast path: text without escapable characters passes through as the same string.
        String plain = "plain text without escapes";
        assertSame(plain, QueryParameters.toText(plain));
    }

    @Test
    @DisplayName("byte-array values decode as UTF-8 and take the same ESCAPED form")
    void byteArrayValuesEscaped() {
        assertEquals("a\\nb", QueryParameters.toText(
                "a\nb".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("of() preserves insertion order across mixed bindings")
    void ofPreservesOrder() {
        QueryParameters params = QueryParameters.of(new java.util.LinkedHashMap<>(
                Map.of("z", 1)) {{
                    put("a", 2);
                }});
        assertEquals(List.copyOf(params.asMap().keySet()).get(0), "z");
    }
}
