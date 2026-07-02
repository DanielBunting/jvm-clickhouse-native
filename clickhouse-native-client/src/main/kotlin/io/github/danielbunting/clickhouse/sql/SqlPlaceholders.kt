package io.github.danielbunting.clickhouse.sql

import java.util.ArrayDeque
import java.util.BitSet
import java.util.function.IntPredicate

/**
 * The shared placeholder scanner for ClickHouse SQL, used by both driver layers (JDBC's
 * `ChPreparedStatement` and ADBC's parameter binding) so the two can never drift on what counts
 * as a bindable `?`.
 *
 * [positions] scans SQL text and returns the offsets of every `?` that is a real, bindable
 * positional placeholder. The scan understands:
 *
 *  - single-quoted string literals, with both the doubled-quote (`''`) and backslash
 *    (`\'`, `\\`) escapes;
 *  - backtick- and double-quoted identifiers, with the doubled-character escapes and
 *    backslash escapes;
 *  - `--`, `#` and `#!` line comments and nesting block comments (comment text is plain text);
 *  - the `?::type` cast adjacency (the `::` is not a ternary colon);
 *  - the `:` inside a braced `{name:Type}` server-side parameter (the name/Type separator,
 *    never a ternary colon); and
 *  - the ClickHouse ternary operator `cond ? a : b`, disambiguated in a single left-to-right
 *    pass: each ternary-eligible `:` pairs with the NEAREST preceding unpaired `?` at the same
 *    nesting depth, and a `?` whose expression ends (at a same-depth `,`/`;`, the enclosing
 *    bracket's close, or end of text) without being paired is a placeholder.
 *
 * [rewriteToNamedParams] turns those `?`s into named, typed server-side placeholders
 * `{_p1:String}`, `{_p2:String}`, …, and [namedParameters] extracts user-written `{name:Type}`
 * tokens so a driver can bind them by name.
 */
public object SqlPlaceholders {

    /** Generated parameter-name prefix for the positional rewrite: `_p1`, `_p2`, … */
    public const val PARAM_NAME_PREFIX: String = "_p"

    /**
     * Offsets of every bindable positional `?` in [sql], in order (see the class doc for
     * exactly what the scan understands).
     */
    @JvmStatic
    public fun positions(sql: String): List<Int> {
        val n = sql.length
        // Offsets of every real-text '?', in order; ternary '?'s are struck out
        // (by index into this list) as their pairing ':' arrives.
        val questionMarks = ArrayList<Int>()
        val ternary = BitSet()
        // Bracket-opener stack; its length is the current nesting depth.
        val openers = StringBuilder()
        // Per depth, the indexes (into questionMarks) of '?'s still eligible to be a
        // ternary condition at that depth, in scan order.
        val pending = ArrayList<ArrayDeque<Int>>()
        pending.add(ArrayDeque())
        var i = 0
        while (i < n) {
            val c = sql[i]
            val depth = openers.length
            if (c == '\'' || c == '`' || c == '"') {
                i = skipQuoted(sql, i)
            } else if (c == '#' || (c == '-' && i + 1 < n && sql[i + 1] == '-')) {
                i = skipLineComment(sql, i)
            } else if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
                i = skipBlockComment(sql, i)
            } else if (c == '(' || c == '[' || c == '{') {
                openers.append(c)
                if (pending.size <= openers.length) {
                    pending.add(ArrayDeque())
                }
                i++
            } else if (c == ')' || c == ']' || c == '}') {
                if (depth > 0) {
                    // The bracket's expression ends: its still-unpaired '?'s are
                    // placeholders for good.
                    pending[depth].clear()
                    openers.setLength(depth - 1)
                }
                i++
            } else if (c == ',' || c == ';') {
                // An expression boundary at this depth: its unpaired '?'s stay placeholders.
                pending[depth].clear()
                i++
            } else if (c == '?') {
                pending[depth].addLast(questionMarks.size)
                questionMarks.add(i)
                i++
            } else if (c == ':') {
                if (i + 1 < n && sql[i + 1] == ':') {
                    i += 2 // '::' cast operator — never a ternary colon
                } else {
                    // Inside braces the ':' is a {name:Type} separator, not a ternary
                    // colon; elsewhere it claims the nearest preceding unpaired '?'
                    // at this depth (if any) as a ternary condition.
                    val insideBraces = depth > 0 && openers[depth - 1] == '{'
                    if (!insideBraces && !pending[depth].isEmpty()) {
                        ternary.set(pending[depth].removeLast())
                    }
                    i++
                }
            } else {
                i++
            }
        }
        val positions = ArrayList<Int>(questionMarks.size)
        for (q in questionMarks.indices) {
            if (!ternary.get(q)) {
                positions.add(questionMarks[q])
            }
        }
        return positions
    }

    /** Number of bindable positional `?` placeholders in [sql]. */
    @JvmStatic
    public fun count(sql: String): Int = positions(sql).size

    /**
     * Rewrites the bindable `?` placeholders of [template] into named, typed server-side
     * placeholders `{_p1:String}`, `{_p2:String}`, …, preserving everything else verbatim.
     *
     * Every placeholder is declared `:String` — the server casts the textual value to the
     * column/expression type at bind time — except parameters for which [nullableParam]
     * answers true (1-based parameter number), which are declared `:Nullable(String)` so the
     * server accepts the `\N` null sentinel.
     *
     * @param template      the SQL with `?` placeholders
     * @param positions     the template's placeholder offsets (from [positions])
     * @param nullableParam predicate over the 1-based parameter number, or `null` for
     *                      all-`:String`
     */
    @JvmStatic
    @JvmOverloads
    public fun rewriteToNamedParams(
        template: String,
        positions: List<Int> = positions(template),
        nullableParam: IntPredicate? = null,
    ): String {
        val out = StringBuilder(template.length + 16)
        var prev = 0
        var param = 1
        for (pos in positions) {
            out.append(template, prev, pos)
            val nullable = nullableParam != null && nullableParam.test(param)
            out.append('{').append(PARAM_NAME_PREFIX).append(param)
                .append(if (nullable) ":Nullable(String)}" else ":String}")
            param++
            prev = pos + 1
        }
        out.append(template, prev, template.length)
        return out.toString()
    }

    /**
     * A user-written server-side parameter token `{name:Type}` found in SQL text: [name] is
     * the identifier before the first `:`, [type] the ClickHouse type text after it (which may
     * itself contain colons/nested braces only in string form, e.g. `DateTime('UTC')`).
     */
    public data class NamedParameter(val name: String, val type: String)

    /**
     * Extracts the user-written `{name:Type}` server-side parameter tokens of [sql], in order
     * of first appearance, skipping string literals, quoted identifiers and comments. A name
     * referenced twice is reported once (the first type wins — ClickHouse binds one value per
     * name). Brace groups that do not match the `identifier:Type` shape (e.g. a map literal
     * `{'k':1}`) are ignored.
     */
    @JvmStatic
    public fun namedParameters(sql: String): List<NamedParameter> {
        val n = sql.length
        val out = ArrayList<NamedParameter>()
        val seen = HashSet<String>()
        var i = 0
        while (i < n) {
            val c = sql[i]
            if (c == '\'' || c == '`' || c == '"') {
                i = skipQuoted(sql, i)
            } else if (c == '#' || (c == '-' && i + 1 < n && sql[i + 1] == '-')) {
                i = skipLineComment(sql, i)
            } else if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
                i = skipBlockComment(sql, i)
            } else if (c == '{') {
                val close = findBraceClose(sql, i)
                val token = parseNamedParameter(sql.substring(i + 1, close))
                if (token != null && seen.add(token.name)) {
                    out.add(token)
                }
                i = close + 1
            } else {
                i++
            }
        }
        return out
    }

    /** Index of the `}` closing the `{` at [open] (nesting-aware), or the end of text. */
    private fun findBraceClose(sql: String, open: Int): Int {
        var depth = 0
        var i = open
        while (i < sql.length) {
            when (sql[i]) {
                '\'', '`', '"' -> {
                    i = skipQuoted(sql, i)
                    continue
                }
                '{' -> depth++
                '}' -> {
                    depth--
                    if (depth == 0) {
                        return i
                    }
                }
            }
            i++
        }
        return sql.length - 1
    }

    /** Parses `identifier:Type` (both non-empty, identifier-shaped name) or returns null. */
    private fun parseNamedParameter(body: String): NamedParameter? {
        val colon = body.indexOf(':')
        if (colon <= 0 || colon == body.length - 1) {
            return null
        }
        val name = body.substring(0, colon).trim()
        val type = body.substring(colon + 1).trim()
        if (name.isEmpty() || type.isEmpty()) {
            return null
        }
        if (!name[0].isLetter() && name[0] != '_') {
            return null
        }
        if (!name.all { it.isLetterOrDigit() || it == '_' }) {
            return null
        }
        return NamedParameter(name, type)
    }

    /**
     * Skips a quoted region (string literal or quoted identifier) starting at the opening
     * quote; returns the index just past the closing quote. A backslash consumes the following
     * character; a doubled quote stays inside the region. An unterminated region consumes the
     * rest of the text.
     */
    private fun skipQuoted(sql: String, start: Int): Int {
        val q = sql[start]
        val n = sql.length
        var i = start + 1
        while (i < n) {
            val c = sql[i]
            if (c == '\\' && i + 1 < n) {
                i += 2
            } else if (c == q) {
                if (i + 1 < n && sql[i + 1] == q) {
                    i += 2
                } else {
                    return i + 1
                }
            } else {
                i++
            }
        }
        return n
    }

    /** Skips a `--`/`#`/`#!` line comment through its newline. */
    private fun skipLineComment(sql: String, start: Int): Int {
        val nl = sql.indexOf('\n', start)
        return if (nl < 0) sql.length else nl + 1
    }

    /** Skips a (nesting) block comment; an unterminated one consumes the rest. */
    private fun skipBlockComment(sql: String, start: Int): Int {
        val n = sql.length
        var depth = 1
        var i = start + 2
        while (i < n && depth > 0) {
            if (sql[i] == '/' && i + 1 < n && sql[i + 1] == '*') {
                depth++
                i += 2
            } else if (sql[i] == '*' && i + 1 < n && sql[i + 1] == '/') {
                depth--
                i += 2
            } else {
                i++
            }
        }
        return i
    }
}
