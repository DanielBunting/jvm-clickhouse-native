package io.github.danielbunting.clickhouse.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the parts of {@link SqlPlaceholders} not already exercised transitively by
 * the JDBC module's {@code ChSqlParsingTest} (which drives {@link SqlPlaceholders#positions}
 * and the plain rewrite through {@code ChPreparedStatement}): the {@code {name:Type}}
 * extraction and the nullable-predicate rewrite variant.
 */
class SqlPlaceholdersTest {

    @Test
    @DisplayName("{name:Type} tokens are extracted in order with their type text")
    void namedParametersExtracted() {
        List<SqlPlaceholders.NamedParameter> params = SqlPlaceholders.namedParameters(
                "SELECT * FROM t WHERE a = {a:Int32} AND b = {b:Nullable(String)}");
        assertEquals(2, params.size());
        assertEquals("a", params.get(0).getName());
        assertEquals("Int32", params.get(0).getType());
        assertEquals("b", params.get(1).getName());
        assertEquals("Nullable(String)", params.get(1).getType());
    }

    @Test
    @DisplayName("a parameterized type keeps its full text, including quoted arguments")
    void parameterizedTypeTextPreserved() {
        List<SqlPlaceholders.NamedParameter> params = SqlPlaceholders.namedParameters(
                "SELECT {ts:DateTime64(3, 'UTC')}");
        assertEquals("ts", params.get(0).getName());
        assertEquals("DateTime64(3, 'UTC')", params.get(0).getType());
    }

    @Test
    @DisplayName("tokens inside strings, identifiers and comments are ignored")
    void quotedAndCommentedTokensIgnored() {
        assertTrue(SqlPlaceholders.namedParameters(
                "SELECT '{a:Int32}', `col{b:Int32}`, \"c{d:Int32}\" /* {e:Int32} */ -- {f:Int32}")
                .isEmpty());
    }

    @Test
    @DisplayName("a name referenced twice is reported once")
    void duplicateNamesReportedOnce() {
        assertEquals(1, SqlPlaceholders.namedParameters("SELECT {n:Int32} + {n:Int32}").size());
    }

    @Test
    @DisplayName("map literals and other non-identifier brace groups are not parameters")
    void nonParameterBracesIgnored() {
        assertTrue(SqlPlaceholders.namedParameters("SELECT {'k': 1}").isEmpty());
        assertTrue(SqlPlaceholders.namedParameters("SELECT {1: 2}").isEmpty());
        assertTrue(SqlPlaceholders.namedParameters("SELECT {}").isEmpty());
        assertTrue(SqlPlaceholders.namedParameters("SELECT {:Int32}").isEmpty());
        assertTrue(SqlPlaceholders.namedParameters("SELECT {name:}").isEmpty());
        assertTrue(SqlPlaceholders.namedParameters("SELECT { :Int32}").isEmpty(),
                "a name that is blank after trimming");
        assertTrue(SqlPlaceholders.namedParameters("SELECT {x: }").isEmpty(),
                "a type that is blank after trimming");
        assertTrue(SqlPlaceholders.namedParameters("SELECT {na-me:Int32}").isEmpty(),
                "a non-identifier character in the name");
    }

    @Test
    @DisplayName("the nullable predicate drives per-parameter Nullable(String) declarations")
    void rewriteWithNullablePredicate() {
        String rewritten = SqlPlaceholders.rewriteToNamedParams(
                "INSERT INTO t VALUES (?, ?, ?)",
                SqlPlaceholders.positions("INSERT INTO t VALUES (?, ?, ?)"),
                param -> param == 2);
        assertEquals(
                "INSERT INTO t VALUES ({_p1:String}, {_p2:Nullable(String)}, {_p3:String})",
                rewritten);
    }

    @Test
    @DisplayName("the single-argument rewrite declares every placeholder :String")
    void rewriteDefaultsToString() {
        assertEquals("SELECT x FROM t WHERE a = {_p1:String} AND b = {_p2:String}",
                SqlPlaceholders.rewriteToNamedParams("SELECT x FROM t WHERE a = ? AND b = ?"));
    }

    // ---- scanner behavior at the core surface ------------------------------------------------
    // The JDBC module's ChSqlParsingTest drives the same scanner exhaustively through
    // ChPreparedStatement; these cases exercise it through the core API directly (per-module
    // coverage reports only count a module's own tests).

    @Test
    @DisplayName("?s inside string literals, quoted identifiers and comments are not placeholders")
    void quotedAndCommentedQuestionMarksIgnored() {
        assertEquals(1, SqlPlaceholders.count(
                "SELECT '?', 'doubled''?', 'escaped\\'?', `col?`, \"c?\" /* ? /* nested ? */ */"
                        + " -- line ?\n # hash ?\n , ?"));
    }

    @Test
    @DisplayName("an unterminated string literal swallows the rest of the text")
    void unterminatedLiteralConsumesRest() {
        assertEquals(0, SqlPlaceholders.count("SELECT 'unterminated ? ? ?"));
    }

    @Test
    @DisplayName("?::type cast adjacency stays a placeholder; ternary ?s pair with their colon")
    void castAndTernaryDisambiguation() {
        assertEquals(1, SqlPlaceholders.count("SELECT ?::Int32"));
        assertEquals(0, SqlPlaceholders.count("SELECT a > 0 ? 1 : 2 FROM t"));
        assertEquals(1, SqlPlaceholders.count("SELECT a > 0 ? 1 : 2, ? FROM t"),
                "the comma ends the ternary expression; the later ? is a placeholder");
    }

    @Test
    @DisplayName("the colon inside a {name:Type} token never claims a ? as a ternary")
    void bracedColonIsNotTernary() {
        assertEquals(1, SqlPlaceholders.count("SELECT {n:Int32}, ? FROM t"));
    }

    @Test
    @DisplayName("an unterminated {name:Type token still parses to the end of text")
    void unterminatedBraceToken() {
        List<SqlPlaceholders.NamedParameter> params =
                SqlPlaceholders.namedParameters("SELECT {p:Int32");
        assertEquals(1, params.size());
        assertEquals("p", params.get(0).getName());
    }

    @Test
    @DisplayName("a brace group without a colon is not a parameter")
    void braceWithoutColonIgnored() {
        assertTrue(SqlPlaceholders.namedParameters("SELECT {noColon}").isEmpty());
    }
}
