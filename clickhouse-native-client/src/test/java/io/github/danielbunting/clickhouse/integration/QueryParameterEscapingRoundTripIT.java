package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Live-server regression coverage for the query-parameter ESCAPED-form fix in
 * {@code QueryParameters.toText}: the server escape-parses parameter value text against the
 * placeholder type, so scalar text must travel with backslashes and control characters
 * escaped. Before the fix a raw backslash was <b>silently corrupted</b> ({@code "a\b"} arrived
 * as {@code a} + backspace, 2 chars) and a raw newline aborted with BAD_QUERY_PARAMETER (457).
 *
 * <p>Every assertion here is <b>asymmetric</b>: values are written through a parameter but read
 * back through parameterless SQL (or vice versa), so a symmetric encode/decode bug cannot
 * cancel itself out. The offline two-layer wire contract is pinned in
 * {@code QueryParameterEscapingTest}; this proves the live server agrees.
 */
class QueryParameterEscapingRoundTripIT extends TypeRoundTripBase {

    /**
     * Adversarial payloads: the previously-corrupted raw backslash, control characters that
     * previously aborted the parse, escape-sequence look-alikes, quotes (which must NOT be
     * escaped) and multi-byte text.
     */
    private static final String[] PAYLOADS = {
            "a\\b",                    // raw backslash — arrived as a+backspace before the fix
            "c:\\path\\to\\file",      // repeated backslashes
            "trailing\\",              // backslash at end of text
            "line\nbreak",             // raw newline — was BAD_QUERY_PARAMETER (457)
            "tab\there",
            "cr\rreturn",
            "nul\u0000byte",
            "it's",                    // quotes stay raw and must still round trip
            "quote\"double",
            "\\N",                     // literal backslash-N must NOT collapse to SQL NULL
            "{p:String}",              // placeholder look-alike stays data
            "héllo 世界",
    };

    @Test
    @DisplayName("SELECT {p:String} echoes every adversarial payload byte-exactly")
    void selectEchoIsByteExact() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            for (String payload : PAYLOADS) {
                try (QueryResult result = conn.query(
                        "SELECT {p:String} AS v, length({p:String}) AS len",
                        QueryParameters.of(Map.of("p", payload)))) {
                    List<Object[]> rows = materialize(result);
                    assertEquals(1, rows.size());
                    assertEquals(payload, rows.get(0)[0],
                            "the echoed value must be byte-exact: " + escapeForMessage(payload));
                    assertEquals((long) payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
                            ((Number) rows.get(0)[1]).longValue(),
                            "the server-side length must match: " + escapeForMessage(payload));
                }
            }
        }
    }

    @Test
    @DisplayName("INSERT … VALUES ({p:String}) lands every payload byte-exactly (plain read-back)")
    void insertValuesArrivesByteExact() {
        withTable("param_esc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, v String) ENGINE = Memory");
            for (int i = 0; i < PAYLOADS.length; i++) {
                conn.execute(
                        "INSERT INTO " + table + " VALUES ({id:UInt32}, {p:String})",
                        QueryParameters.of(Map.of("id", i, "p", PAYLOADS[i])));
            }
            // Parameterless read-back: any corruption on the parameter path shows here.
            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(PAYLOADS.length, rows.size());
            for (int i = 0; i < PAYLOADS.length; i++) {
                assertEquals(PAYLOADS[i], rows.get(i)[0],
                        "row " + i + " must arrive byte-exact: " + escapeForMessage(PAYLOADS[i]));
            }
        });
    }

    @Test
    @DisplayName("a literal backslash-N string stays a 2-char string; only a null binding is SQL NULL")
    void literalBackslashNIsNotNull() {
        withTable("param_esc_null", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, v Nullable(String)) ENGINE = Memory");
            conn.execute("INSERT INTO " + table + " VALUES ({id:UInt32}, {p:Nullable(String)})",
                    QueryParameters.of(Map.of("id", 0, "p", "\\N")));
            conn.execute("INSERT INTO " + table + " VALUES ({id:UInt32}, {p:Nullable(String)})",
                    QueryParameters.builder().bind("id", 1).bind("p", null).build());

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals("\\N", rows.get(0)[0],
                    "the literal 2-char string must not collapse into the NULL sentinel");
            assertNull(rows.get(1)[0], "only an actual null binding is SQL NULL");
        });
    }

    @Test
    @DisplayName("the parameter path agrees with the literal path (write via param, filter via literal)")
    void parameterAndLiteralPathsAgree() {
        // Belt-and-braces asymmetry: values written through parameters are found by
        // client-side SQL literals built with ClickHouse literal escaping, proving both
        // encodings denote the same server-side value.
        withTable("param_esc_agree", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (v String) ENGINE = Memory");
            conn.execute("INSERT INTO " + table + " VALUES ({p:String})",
                    QueryParameters.of(Map.of("p", "a\\b")));
            try (QueryResult result = conn.query(
                    "SELECT count() FROM " + table + " WHERE v = 'a\\\\b'")) {
                assertEquals(1L, ((Number) materialize(result).get(0)[0]).longValue(),
                        "the SQL literal for backslash must match the param-written value");
            }
        });
    }

    /** Renders control characters visibly for assertion messages. */
    private static String escapeForMessage(String s) {
        return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
                .replace("\t", "\\t").replace("\u0000", "\\0");
    }
}
