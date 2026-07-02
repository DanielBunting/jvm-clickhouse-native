package io.github.danielbunting.clickhouse

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.UUID

/**
 * An ordered, immutable set of *server-side* query parameters: name → textual
 * value pairs that ClickHouse binds into `{name:Type}` placeholders in the SQL.
 *
 * This is the safe alternative to client-side string interpolation. Instead of
 * splicing a literal into the SQL text (an injection and type-fidelity hazard), the
 * value travels separately on the Query packet and the *server* parses it
 * against the declared `Type` in the placeholder. The SQL only ever references
 * a parameter as `{name:Type}`, never the value itself.
 *
 * Values are carried on the wire as the ClickHouse *textual* form (what you
 * would type after `SET param_name = ...` on the CLI): numbers as their decimal
 * text, temporals as `yyyy-MM-dd[ HH:mm:ss]`, and everything else as its
 * `toString()`. The server casts that text to the placeholder's `Type`, so
 * a `String`-typed value need not be SQL-quoted — quoting is the server's job.
 *
 * A `null` value maps to ClickHouse's special `\N` sentinel, which the
 * server interprets as SQL `NULL` for a `Nullable(T)` placeholder.
 *
 * Instances are immutable; build one with [builder] or [of].
 */
public class QueryParameters private constructor(
    /** Insertion-ordered name → textual-value map. A `null` value means SQL NULL. */
    private val values: Map<String, String?>,
) {

    /** Whether there are no parameters (the wire then carries only the empty terminator). */
    public fun isEmpty(): Boolean = values.isEmpty()

    /**
     * The ordered name → textual-value view. A map value of `null` represents a
     * SQL `NULL` binding (serialized as the `\N` sentinel).
     *
     * @return an unmodifiable, insertion-ordered map of name to textual value
     */
    public fun asMap(): Map<String, String?> = Collections.unmodifiableMap(values)

    /**
     * The textual value to put on the wire for [name], or `null` if no such
     * parameter is bound. A bound-but-null parameter returns the `\N` sentinel.
     *
     * @param name the parameter name (as it appears in `{name:Type}`)
     * @return the wire text, the null sentinel, or `null` if unbound
     */
    public fun wireValue(name: String): String? {
        if (!values.containsKey(name)) {
            return null
        }
        return values[name] ?: NULL_SENTINEL
    }

    /** Fluent builder for [QueryParameters]; preserves insertion order. */
    public class Builder {
        private val values = LinkedHashMap<String, String?>()

        /**
         * Binds [name] to the textual form of [value] (see
         * [QueryParameters.toText]). A `null` value binds SQL NULL.
         *
         * @param name  the parameter name (as in `{name:Type}`); must be non-empty
         * @param value the value, or `null` for SQL NULL
         * @return this builder
         */
        public fun bind(name: String, value: Any?): Builder {
            require(name.isNotEmpty()) { "Query parameter name must be non-empty" }
            values[name] = toText(value)
            return this
        }

        /** Binds an already-textual value verbatim (no conversion). */
        public fun bindText(name: String, text: String?): Builder {
            require(name.isNotEmpty()) { "Query parameter name must be non-empty" }
            values[name] = text
            return this
        }

        /** Builds the immutable parameter set. */
        public fun build(): QueryParameters {
            return if (values.isEmpty()) EMPTY else QueryParameters(LinkedHashMap(values))
        }
    }

    public companion object {

        /** The empty parameter set (no bindings). */
        @JvmField
        public val EMPTY: QueryParameters = QueryParameters(emptyMap())

        /**
         * ClickHouse's textual NULL sentinel. A parameter bound to `null` is sent as
         * this string; the server treats it as SQL `NULL` for a `Nullable`
         * placeholder.
         */
        // VERIFY against CH.Native: query-parameter null sentinel is the backslash-N escape.
        private const val NULL_SENTINEL: String = "\\N"

        private val DATETIME_FORMAT: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        /**
         * Sub-second variant used when a temporal value carries a non-zero fractional
         * part, so `DateTime64` precision survives the round trip. Nine fractional
         * digits (nanoseconds) match the codec's maximum `DateTime64(9)`; ClickHouse
         * truncates the extra digits for lower-precision columns. Whole-second values
         * keep the terse [DATETIME_FORMAT] form.
         */
        private val DATETIME_FRAC_FORMAT: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")

        private fun formatDateTime(ldt: LocalDateTime): String =
            (if (ldt.nano != 0) DATETIME_FRAC_FORMAT else DATETIME_FORMAT).format(ldt)

        /**
         * Builds a parameter set from a name → value map, converting each value to its
         * ClickHouse textual form via [toText]. Iteration order of the
         * supplied map is preserved.
         *
         * @param params name → value bindings; values may be `null` for SQL NULL
         * @return an immutable parameter set
         */
        @JvmStatic
        public fun of(params: Map<String, *>?): QueryParameters {
            if (params == null || params.isEmpty()) {
                return EMPTY
            }
            val converted = LinkedHashMap<String, String?>(params.size * 2)
            for ((key, value) in params) {
                require(!key.isNullOrEmpty()) { "Query parameter name must be non-empty" }
                converted[key] = toText(value)
            }
            return QueryParameters(converted)
        }

        /** Starts a fluent builder. */
        @JvmStatic
        public fun builder(): Builder = Builder()

        /**
         * Converts a Java value to the ClickHouse textual form expected for a server-side
         * query parameter. The result is the value the server will cast via the
         * placeholder's `{name:Type}`; it is NOT SQL-quoted (the server quotes).
         *
         * @param value the bound value, or `null` for SQL NULL
         * @return the textual form, or `null` to signal a NULL binding
         */
        @JvmStatic
        public fun toText(value: Any?): String? {
            return when (value) {
                null -> null // becomes the \N sentinel at wire time
                // Composites carry their own element-level escaping (see appendElement).
                is List<*> -> renderArray(value)
                is Map<*, *> -> renderMap(value)
                // Scalars: the raw text, escaped into the ESCAPED form the server's parameter
                // parser reads (see escapeParamText).
                else -> rawText(value)?.let(::escapeParamText)
            }
        }

        /**
         * The raw (unescaped) textual form of a scalar value — the content used both directly
         * by [toText] (which applies wire escaping on top) and as the quoted-literal content of
         * composite elements in [appendElement] (which applies its own quoting instead).
         */
        private fun rawText(value: Any?): String? {
            return when (value) {
                null -> null // becomes the \N sentinel at wire time
                // ClickHouse Bool parameters accept the textual 'true'/'false'.
                is Boolean -> if (value) "true" else "false"
                is BigDecimal -> value.toPlainString()
                is Number, is BigInteger -> value.toString()
                is ByteArray -> String(value, StandardCharsets.UTF_8)
                is java.sql.Timestamp -> formatDateTime(value.toLocalDateTime())
                is Instant -> formatDateTime(LocalDateTime.ofInstant(value, ZoneOffset.UTC))
                // Zone-carrying temporals render as their UTC wall clock (the zone shifts
                // the instant, then drops out of the text), mirroring the JDBC literal path.
                is java.time.ZonedDateTime ->
                    formatDateTime(LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC))
                is java.time.OffsetDateTime ->
                    formatDateTime(LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC))
                is LocalDateTime -> formatDateTime(value)
                is java.sql.Date -> value.toLocalDate().toString()
                is LocalDate -> value.toString()
                is UUID -> value.toString()
                // IPv4/IPv6: the canonical numeric address, which the server casts to
                // IPv4/IPv6 (java.net.InetAddress.toString() would prepend the hostname).
                is java.net.InetAddress -> value.hostAddress
                // Composites travel as ClickHouse literal text — the server's parameter
                // parser reads the same [..]/{..:..} grammar as SQL literals (verified
                // empirically: quoted strings inside, backslash escapes, bare NULL).
                // String and any other reference type: its textual representation. The server
                // casts this to the placeholder's declared type, so no SQL quoting is applied.
                else -> value.toString()
            }
        }

        /**
         * Escapes a textual parameter value into the ClickHouse ESCAPED form the server's
         * parameter parser expects: backslash and the control characters NUL/newline/carriage
         * return/tab become their backslash escapes (without this, a raw `\` is
         * escape-interpreted server-side — silent corruption — and a raw newline aborts the
         * parse with BAD_QUERY_PARAMETER). Quotes are NOT escaped: they are structural in
         * composite (Array/Map) parameter text and content in String text, and the server
         * accepts them raw in both. Values without any escapable character pass through
         * unchanged.
         */
        private fun escapeParamText(s: String): String {
            var needsEscaping = false
            for (c in s) {
                if (c == '\\' || c == '\n' || c == '\r' || c == '\t' || c == '\u0000') {
                    needsEscaping = true
                    break
                }
            }
            if (!needsEscaping) {
                return s
            }
            val sb = StringBuilder(s.length + 8)
            for (c in s) {
                when (c) {
                    '\\' -> sb.append("\\\\")
                    '\n' -> sb.append("\\n")
                    '\r' -> sb.append("\\r")
                    '\t' -> sb.append("\\t")
                    '\u0000' -> sb.append("\\0")
                    else -> sb.append(c)
                }
            }
            return sb.toString()
        }

        private fun renderArray(list: List<*>): String {
            val sb = StringBuilder("[")
            for ((i, e) in list.withIndex()) {
                if (i > 0) sb.append(',')
                appendElement(sb, e)
            }
            return sb.append(']').toString()
        }

        private fun renderMap(map: Map<*, *>): String {
            val sb = StringBuilder("{")
            var first = true
            for ((k, v) in map) {
                if (!first) sb.append(',')
                first = false
                appendElement(sb, k)
                sb.append(':')
                appendElement(sb, v)
            }
            return sb.append('}').toString()
        }

        /**
         * Renders one ELEMENT of a composite value. Unlike the top-level [toText]
         * (where the server casts unquoted text against the declared type), elements
         * inside an array/map literal follow SQL-literal rules: strings, temporals,
         * UUIDs and IPs are single-quoted with `\`/`'` backslash-escaped; `null` is
         * the bare `NULL` keyword; nested lists/maps recurse.
         */
        private fun appendElement(sb: StringBuilder, value: Any?) {
            when (value) {
                null -> sb.append("NULL")
                is Boolean -> sb.append(if (value) "true" else "false")
                is BigDecimal -> sb.append(value.toPlainString())
                is Number, is BigInteger -> sb.append(value.toString())
                is List<*> -> sb.append(renderArray(value))
                is Map<*, *> -> sb.append(renderMap(value))
                else -> {
                    // Everything textual (String, temporals, UUID, IP) is quoted using the
                    // scalar rendering as its content.
                    val text = rawText(value) ?: "NULL"
                    sb.append('\'')
                    for (c in text) {
                        if (c == '\\' || c == '\'') {
                            sb.append('\\')
                        }
                        sb.append(c)
                    }
                    sb.append('\'')
                }
            }
        }
    }
}
