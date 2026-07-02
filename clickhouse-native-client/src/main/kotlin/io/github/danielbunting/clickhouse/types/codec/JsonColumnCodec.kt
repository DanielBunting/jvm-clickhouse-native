package io.github.danielbunting.clickhouse.types.codec

import io.github.danielbunting.clickhouse.ProtocolException
import io.github.danielbunting.clickhouse.protocol.BinaryReader
import io.github.danielbunting.clickhouse.protocol.BinaryWriter
import io.github.danielbunting.clickhouse.types.ColumnCodec
import io.github.danielbunting.clickhouse.types.DefaultTypeParser
import io.github.danielbunting.clickhouse.types.TypeParser
import java.io.IOException

/**
 * Self-framing column codec for ClickHouse `JSON` columns using the **FLATTENED**
 * native serialization
 * (`output_format_native_use_flattened_dynamic_and_json_serialization = 1`).
 *
 * In flattened form a JSON column is a set of typed *paths*; each path is itself a
 * `Dynamic` sub-column. The path **prefixes** (the per-path Dynamic type lists) are
 * written for all paths first, then the path **bodies** (discriminators + sub-columns) for
 * all paths — mirroring ClickHouse's prefix/data stream split.
 *
 * ## Wire format (per column data stream), reverse-engineered on ClickHouse 25.6
 *
 * ```
 * UInt64   version = 3
 * VarUInt  num_paths
 * String   path_name[num_paths]                 (sorted)
 * for each path p:                               // PREFIXES (all paths)
 *   UInt64   dynamic_version = 3
 *   VarUInt  num_types_p
 *   String   type_name[num_types_p]
 * for each path p:                               // BODIES (all paths)
 *   UInt8    discriminator[rowCount]             index into type_name, or num_types_p = NULL
 *   sub-column[i] with count_i rows
 * ```
 *
 * ## Decoded value — first cut: JSON String
 *
 * [get] returns a JSON object [String] for the row, reconstructed from the
 * decoded paths (keys sorted, paths whose value is NULL for that row omitted), e.g.
 * `{"a":1,"b":"x"}`. This faithfully round-trips the content for flat objects.
 *
 * ## ENCODE ([write]) — supported subset
 *
 * [write] is the inverse of [read] for a **restricted, flat subset**. The
 * backing `Object[]` holds one JSON object [String] per row (or `null` for an
 * empty/absent object). Each string is parsed with a minimal built-in JSON parser into a flat
 * map of `path -> scalar`; the distinct paths across all rows are sorted, and each path is
 * emitted as an embedded `Dynamic` (the same flattened `version 3` prefix + body that
 * [read] consumes). The server accepts this flattened layout on binary Native INPUT with
 * no input setting.
 *
 * ### Supported input
 *
 *  - Flat JSON objects only: `{"a":1,"b":"x","c":true}`. The top-level value must be an
 *    object; an empty object `{}` (or a `null` row) contributes no paths.
 *  - Scalar values: JSON integer → `Int64` (`Long`), JSON floating →
 *    `Float64` (`Double`), JSON string → `String`, JSON `true`/
 *    `false` → `Bool`, JSON `null` → the path is omitted for that
 *    row (matches the read side, which omits NULL paths).
 *  - Per-path type inference follows [DynamicColumnCodec]; a path that holds different
 *    scalar Java types across rows yields a multi-type Dynamic (each row's value under its
 *    own inferred type), which is valid but the on-disk JSON type may be promoted by the
 *    server.
 *
 * ### Unsupported (throws [IllegalArgumentException] at write time)
 *
 *  - Nested objects (`{"a":{"b":1}}`) and arrays (`{"a":[1,2]}`) — the minimal
 *    parser rejects non-scalar values rather than silently mis-encoding them.
 *  - A top-level non-object JSON document.
 *
 * This subset reliably round-trips the same flat int/string/bool/float objects that
 * [read] reconstructs. Richer JSON (nested trees, arrays, typed paths with dots) should
 * be inserted via SQL `VALUES` until a tree model is added.
 */
public class JsonColumnCodec
/**
 * @param parser      used to resolve path member type names discovered on the wire
 * @param declaration the full column type string (e.g. `JSON(max_dynamic_paths=8, a.b Int64)`),
 *                    or `null`/plain `JSON` for a column with no typed paths. TYPED paths are
 *                    serialized on the wire as their DECLARED type (no Dynamic framing), so the
 *                    codec must know them up front — they are not discoverable from the wire.
 */
public constructor(parser: TypeParser?, declaration: String?) : ColumnCodec<Array<Any?>> {

    private val parser: TypeParser

    /** Typed-path names, in the canonical (sorted) order the server serializes them. */
    private val typedPathNames: Array<String>

    /** Codec per typed path, positionally aligned with [typedPathNames]. */
    private val typedPathCodecs: Array<ColumnCodec<*>>

    init {
        if (parser == null) {
            throw NullPointerException("parser")
        }
        this.parser = parser
        val typed = parseTypedPaths(declaration)
        this.typedPathNames = Array(typed.size) { typed[it].first }
        this.typedPathCodecs = Array(typed.size) { parser.parse(typed[it].second) }
    }

    /** Codec for a plain `JSON` column (no typed paths). */
    public constructor(parser: TypeParser?) : this(parser, null)

    override fun typeName(): String {
        return "JSON"
    }

    override fun allocate(rowCount: Int): Array<Any?> {
        return arrayOfNulls(rowCount)
    }

    /**
     * Opaque wire-prefix state: the dynamic-path names and each dynamic path's Dynamic
     * type list, read by [readPrefix] and consumed by [readBody]. Split out because a
     * `JSON` value inside a `Dynamic` column serializes its prefix BEFORE the Dynamic
     * discriminators and its body after them (see [DynamicColumnCodec.readFlattened]).
     */
    internal class JsonPrefix(
        @JvmField val dynamicPaths: Array<String?>,
        @JvmField val dynamicPathCodecs: Array<Array<ColumnCodec<*>?>?>,
    )

    /**
     * Prefix stashed by [readStatePrefix] for the next [read] to consume. The state
     * prefix and the body are read at different wire positions when this codec sits
     * inside a `Dynamic` member (prefix before the Dynamic discriminators, body after
     * them), so the prefix travels between the two calls via this field. [read] always
     * clears it, and every [readStatePrefix] overwrites it, so a stash can never
     * outlive its own block: Dynamic member codecs are constructed fresh per block by
     * the [TypeParser], and a registry-cached top-level codec is driven strictly
     * readStatePrefix-then-read per column on the connection's single read thread.
     */
    private var pendingPrefix: JsonPrefix? = null

    /**
     * Reads the JSON serialization prefix at the column's state-prefix position and
     * stashes it in [pendingPrefix] for the next [read]. For a top-level `JSON` column
     * the state prefix immediately precedes the column body, so consuming the prefix
     * here instead of inline in [read] leaves the byte order unchanged; for a `JSON`
     * (or container-of-JSON) member of a `Dynamic` column this is the member-prefix
     * position before the discriminators, which is exactly where the server puts it.
     */
    @Throws(IOException::class)
    override fun readStatePrefix(`in`: BinaryReader) {
        pendingPrefix = readPrefix(`in`)
    }

    /**
     * Reads the JSON column's serialization PREFIX: version, dynamic-path names, and each
     * dynamic path's Dynamic type list. TYPED paths (declared in the column type) are NOT
     * on this list — they serialize as their declared type with no Dynamic framing.
     */
    @Throws(IOException::class)
    internal fun readPrefix(`in`: BinaryReader): JsonPrefix {
        val version = `in`.readUInt64()
        if (version != JSON_VERSION) {
            throw ProtocolException(
                "Unsupported JSON serialization version: " + version
                    + " (expected flattened version 3; ensure "
                    + "output_format_native_use_flattened_dynamic_and_json_serialization=1)"
            )
        }

        val numPaths = `in`.readVarUInt().toInt()
        val paths = arrayOfNulls<String>(numPaths)
        for (p in 0 until numPaths) {
            paths[p] = `in`.readString()
        }

        // Per-dynamic-path Dynamic type lists (all paths first).
        val pathCodecs = arrayOfNulls<Array<ColumnCodec<*>?>>(numPaths)
        for (p in 0 until numPaths) {
            val dynVersion = `in`.readUInt64()
            if (dynVersion != DYNAMIC_VERSION) {
                throw ProtocolException(
                    "Unsupported JSON path Dynamic version: " + dynVersion
                )
            }
            val numTypes = `in`.readVarUInt().toInt()
            val codecs = arrayOfNulls<ColumnCodec<*>>(numTypes)
            for (i in 0 until numTypes) {
                codecs[i] = parser.parse(`in`.readString())
            }
            pathCodecs[p] = codecs
        }
        return JsonPrefix(paths, pathCodecs)
    }

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    override fun read(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>) {
        // Consume the stashed prefix unconditionally: even a zero-row read (a Dynamic
        // member no row selects) retires its prefix, since the body it framed is empty.
        val stashed = pendingPrefix
        pendingPrefix = null
        if (rowCount == 0) {
            return
        }
        // No stash means no readStatePrefix ran for this column (e.g. a JSON path
        // nested inside another JSON prefix): the prefix is inline before the body.
        readBody(`in`, rowCount, dest, stashed ?: readPrefix(`in`))
    }

    /**
     * Reads the JSON column's BODY: first the typed-path sub-columns (each read directly
     * through its declared codec, in canonical order), then each dynamic path's
     * discriminators + sub-columns, then reconstructs one JSON object String per row.
     */
    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    internal fun readBody(`in`: BinaryReader, rowCount: Int, dest: Array<Any?>, prefix: JsonPrefix) {
        val paths = prefix.dynamicPaths
        val pathCodecs = prefix.dynamicPathCodecs
        val numPaths = paths.size

        // --- TYPED-PATH BODIES: plain sub-columns of the declared types, sorted order ---
        val typedValues = arrayOfNulls<Array<Any?>>(typedPathNames.size)
        for (t in typedPathNames.indices) {
            val codec = typedPathCodecs[t] as ColumnCodec<Any>
            val arr = codec.allocate(rowCount)
            codec.read(`in`, rowCount, arr)
            val boxed = arrayOfNulls<Any>(rowCount)
            for (row in 0 until rowCount) {
                boxed[row] = codec.get(arr, row)
            }
            typedValues[t] = boxed
        }

        // --- BODIES: per-path discriminators + sub-columns (all paths) ---
        // pathValues[p][row] = decoded value of path p at row, or null if absent/NULL.
        val pathValues = arrayOfNulls<Array<Any?>>(numPaths)
        for (p in 0 until numPaths) {
            pathValues[p] = arrayOfNulls(rowCount)
        }
        for (p in 0 until numPaths) {
            val codecs = pathCodecs[p]!!
            val numTypes = codecs.size

            val discriminators = IntArray(rowCount)
            val counts = IntArray(numTypes)
            for (row in 0 until rowCount) {
                val d = `in`.readUInt8()
                discriminators[row] = d
                if (d != numTypes) {
                    if (d > numTypes) {
                        throw ProtocolException(
                            "JSON path discriminator " + d + " out of range"
                        )
                    }
                    counts[d]++
                }
            }
            val subArrays = arrayOfNulls<Any>(numTypes)
            for (i in 0 until numTypes) {
                val codec = codecs[i] as ColumnCodec<Any>
                val arr = codec.allocate(counts[i])
                codec.read(`in`, counts[i], arr)
                subArrays[i] = arr
            }
            val cursor = IntArray(numTypes)
            for (row in 0 until rowCount) {
                val d = discriminators[row]
                if (d == numTypes) {
                    pathValues[p]!![row] = null
                } else {
                    val codec = codecs[d] as ColumnCodec<Any?>
                    pathValues[p]!![row] = codec.get(subArrays[d], cursor[d]++)
                }
            }
        }

        // Reconstruct one JSON object String per row, merging typed + dynamic paths in
        // sorted-name order (matching the sorted key order of the dynamic-only case).
        val allNames = arrayOfNulls<String>(typedPathNames.size + numPaths)
        val allValues = arrayOfNulls<Array<Any?>>(allNames.size)
        for (t in typedPathNames.indices) {
            allNames[t] = typedPathNames[t]
            allValues[t] = typedValues[t]
        }
        for (p in 0 until numPaths) {
            allNames[typedPathNames.size + p] = paths[p]
            allValues[typedPathNames.size + p] = pathValues[p]
        }
        val order = (0 until allNames.size).sortedBy { allNames[it] }
        val sortedNames = arrayOfNulls<String>(allNames.size)
        val sortedValues = arrayOfNulls<Array<Any?>>(allNames.size)
        for (i in order.indices) {
            sortedNames[i] = allNames[order[i]]
            sortedValues[i] = allValues[order[i]]
        }
        for (row in 0 until rowCount) {
            dest[row] = buildJson(sortedNames, sortedValues, row)
        }
    }

    @Throws(IOException::class)
    override fun write(out: BinaryWriter, src: Array<Any?>, rowCount: Int) {
        if (rowCount == 0) {
            return // empty/terminating block: no payload (mirrors read())
        }
        if (typedPathNames.isNotEmpty()) {
            // A typed path must be serialized as its declared type at its canonical wire
            // position; this writer emits every path as Dynamic, which the server would
            // misread. Reject up front (at the staged encode, so the connection is safe).
            throw IllegalArgumentException(
                "Writing into a JSON column with typed paths is not supported (typed: "
                    + typedPathNames.joinToString(", ") + ")")
        }

        // Parse each row's JSON object into a flat path->scalar map.
        // pathValues.get(p)[row] = scalar for path p at row, or null if absent/JSON null.
        val pathSet = java.util.TreeSet<String>()
        val parsed = java.util.ArrayList<Map<String, Any>>(rowCount)
        for (row in 0 until rowCount) {
            val v = src[row]
            val obj: Map<String, Any> =
                if (v == null) java.util.Collections.emptyMap() else parseFlatObject(v.toString())
            parsed.add(obj)
            pathSet.addAll(obj.keys)
        }
        val paths = pathSet.toTypedArray()
        val numPaths = paths.size

        // version + path list.
        out.writeUInt64(JSON_VERSION)
        out.writeVarUInt(numPaths.toLong())
        for (p in 0 until numPaths) {
            out.writeString(paths[p])
        }

        // Build a per-path Object[] column of the scalar values (null where absent).
        val pathColumns = arrayOfNulls<Array<Any?>>(numPaths)
        for (p in 0 until numPaths) {
            val column = arrayOfNulls<Any>(rowCount)
            for (row in 0 until rowCount) {
                column[row] = parsed[row][paths[p]]
            }
            pathColumns[p] = column
        }

        // For symmetry with read() we must split the per-path Dynamic into its PREFIX
        // (version + type list) and BODY (discriminators + sub-columns). Compute each
        // path's sorted distinct inferred types once, reused for both halves.
        val pathTypeNames = arrayOfNulls<Array<String>>(numPaths)
        for (p in 0 until numPaths) {
            val types = java.util.TreeSet<String>()
            for (row in 0 until rowCount) {
                val v = pathColumns[p]!![row]
                if (v != null) {
                    types.add(DynamicColumnCodec.inferClickHouseType(v))
                }
            }
            pathTypeNames[p] = types.toTypedArray()
        }

        // --- PREFIXES: per-path Dynamic version + type list (all paths first) ---
        for (p in 0 until numPaths) {
            out.writeUInt64(DYNAMIC_VERSION)
            val types = pathTypeNames[p]!!
            out.writeVarUInt(types.size.toLong())
            for (i in types.indices) {
                out.writeString(types[i])
            }
        }

        // --- BODIES: per-path discriminators + sub-columns (all paths) ---
        for (p in 0 until numPaths) {
            writeDynamicBody(out, pathColumns[p]!!, rowCount, pathTypeNames[p]!!)
        }
    }

    /**
     * Writes one path's flattened Dynamic body (discriminators + per-type sub-columns), with the
     * type list already known. Mirrors the body half of [read]; the prefix (version + type
     * names) is emitted separately so all path prefixes precede all path bodies.
     */
    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    private fun writeDynamicBody(
        out: BinaryWriter,
        values: Array<Any?>,
        rowCount: Int,
        typeNames: Array<String>
    ) {
        val numTypes = typeNames.size
        val typeIndex = java.util.HashMap<String, Int>()
        val codecs = arrayOfNulls<ColumnCodec<*>>(numTypes)
        for (i in 0 until numTypes) {
            typeIndex[typeNames[i]] = i
            codecs[i] = parser.parse(typeNames[i])
        }

        val discriminators = IntArray(rowCount)
        val counts = IntArray(numTypes)
        for (row in 0 until rowCount) {
            val v = values[row]
            if (v == null) {
                discriminators[row] = numTypes // NULL
            } else {
                val d = typeIndex[DynamicColumnCodec.inferClickHouseType(v)]!!
                discriminators[row] = d
                counts[d]++
            }
        }
        for (row in 0 until rowCount) {
            out.writeUInt8(discriminators[row])
        }
        for (i in 0 until numTypes) {
            val codec = codecs[i] as ColumnCodec<Any>
            val arr = codec.allocate(counts[i])
            var cursor = 0
            for (row in 0 until rowCount) {
                if (discriminators[row] == i) {
                    codec.set(arr, cursor++, values[row])
                }
            }
            codec.write(out, arr, counts[i])
        }
    }

    override fun get(array: Array<Any?>, row: Int): Any? {
        return array[row]
    }

    override fun set(array: Array<Any?>, row: Int, value: Any?) {
        array[row] = value
    }

    override fun javaType(): Class<*> {
        return String::class.java
    }

    /** Tiny recursive-descent scanner for the supported flat-object subset. */
    private class JsonScanner(private val src: String) {

        private var i: Int = 0

        fun pos(): Int {
            return i
        }

        fun skipWs() {
            while (i < src.length) {
                val c = src[i]
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    i++
                } else {
                    break
                }
            }
        }

        fun peek(): Char {
            if (i >= src.length) {
                throw IllegalArgumentException("Unexpected end of JSON")
            }
            return src[i]
        }

        fun next(): Char {
            val c = peek()
            i++
            return c
        }

        fun expect(c: Char) {
            val actual = next()
            if (actual != c) {
                throw IllegalArgumentException(
                    "Expected '" + c + "' at index " + (i - 1) + " in JSON: " + src
                )
            }
        }

        fun expectEnd() {
            if (i != src.length) {
                throw IllegalArgumentException(
                    "Trailing characters after JSON object at index " + i + ": " + src
                )
            }
        }

        fun readString(): String {
            expect('"')
            val sb = StringBuilder()
            while (true) {
                val c = next()
                if (c == '"') {
                    return sb.toString()
                }
                if (c == '\\') {
                    val e = next()
                    when (e) {
                        '"' -> sb.append('"')
                        '\\' -> sb.append('\\')
                        '/' -> sb.append('/')
                        'n' -> sb.append('\n')
                        'r' -> sb.append('\r')
                        't' -> sb.append('\t')
                        'b' -> sb.append('\b')
                        'f' -> sb.append('\u000C')
                        'u' -> {
                            val hex = src.substring(i, i + 4)
                            i += 4
                            sb.append(Integer.parseInt(hex, 16).toChar())
                        }
                        else ->
                            throw IllegalArgumentException("Bad escape \\" + e + " in JSON")
                    }
                } else {
                    sb.append(c)
                }
            }
        }

        /**
         * Reads a scalar JSON value (string / number / true / false / null). Objects and arrays
         * are rejected — the JSON write path supports flat objects only.
         */
        fun readScalar(): Any? {
            val c = peek()
            if (c == '"') {
                return readString()
            }
            if (c == 't' || c == 'f') {
                return readBoolean()
            }
            if (c == 'n') {
                expectLiteral("null")
                return null
            }
            if (c == '{' || c == '[') {
                throw IllegalArgumentException(
                    "Nested objects/arrays are not supported by JSON binary encode "
                        + "(insert via SQL VALUES); offending char '" + c
                        + "' at index " + i
                )
            }
            return readNumber()
        }

        fun readBoolean(): Boolean {
            if (peek() == 't') {
                expectLiteral("true")
                return true
            }
            expectLiteral("false")
            return false
        }

        fun expectLiteral(lit: String) {
            if (!src.regionMatches(i, lit, 0, lit.length)) {
                throw IllegalArgumentException("Expected '" + lit + "' at index " + i)
            }
            i += lit.length
        }

        fun readNumber(): Any {
            val start = i
            var floating = false
            while (i < src.length) {
                val c = src[i]
                if ((c >= '0' && c <= '9') || c == '-' || c == '+') {
                    i++
                } else if (c == '.' || c == 'e' || c == 'E') {
                    floating = true
                    i++
                } else {
                    break
                }
            }
            val num = src.substring(start, i)
            if (num.isEmpty()) {
                throw IllegalArgumentException("Expected a JSON value at index " + start)
            }
            if (floating) {
                return java.lang.Double.parseDouble(num)
            }
            return java.lang.Long.parseLong(num)
        }
    }

    public companion object {

        /** The flattened JSON / Dynamic structure version emitted by ClickHouse 25.6. */
        private const val JSON_VERSION: Long = 3L
        private const val DYNAMIC_VERSION: Long = 3L

        /**
         * Parses the TYPED-PATH declarations out of a `JSON(...)` type string:
         * `max_dynamic_*=N` parameters and `SKIP`/`SKIP REGEXP` entries are ignored;
         * every remaining `path Type` entry (path optionally backtick-quoted) is a typed
         * path. Returns (name, type) pairs sorted by name — the canonical order the
         * server serializes typed-path sub-columns in.
         */
        @JvmStatic
        internal fun parseTypedPaths(declaration: String?): List<Pair<String, String>> {
            if (declaration == null) {
                return emptyList()
            }
            val open = declaration.indexOf('(')
            if (open < 0 || !declaration.endsWith(")")) {
                return emptyList()
            }
            val inner = declaration.substring(open + 1, declaration.length - 1)
            val out = ArrayList<Pair<String, String>>()
            for (raw in DefaultTypeParser.splitTopLevel(inner)) {
                val entry = raw.trim()
                if (entry.isEmpty()) {
                    continue
                }
                // Parameters (max_dynamic_paths = N, max_dynamic_types = N) are the
                // only entries with '=' at the TOP level; an Enum type's '=' sits
                // inside the type's own parentheses (e.g. `status Enum8('ok' = 1)`).
                if (DefaultTypeParser.indexOfTopLevel(entry, '=') >= 0) {
                    continue
                }
                // `name Type` splits at the first top-level space. An entry without
                // one (a bare parameter/keyword) is not a typed path.
                val sp = DefaultTypeParser.indexOfTopLevel(entry, ' ')
                if (sp < 0) {
                    continue
                }
                val namePart = entry.substring(0, sp)
                // A skip clause is the SKIP keyword followed by a path or REGEXP
                // '...'; a path NAME merely starting with the letters SKIP (e.g.
                // `SKIPPED_AT DateTime`) is a typed path.
                if (namePart == "SKIP") {
                    continue
                }
                val name =
                    if (namePart.length >= 2 && namePart.startsWith("`") && namePart.endsWith("`")) {
                        namePart.substring(1, namePart.length - 1)
                    } else {
                        namePart
                    }
                val type = entry.substring(sp + 1).trim()
                if (type.isNotEmpty()) {
                    out.add(Pair(name, type))
                }
            }
            out.sortBy { it.first }
            return out
        }

        /** Builds a `{"path":value,...}` String for [row], omitting NULL paths. */
        @JvmStatic
        private fun buildJson(paths: Array<String?>, pathValues: Array<Array<Any?>?>, row: Int): String {
            val sb = StringBuilder("{")
            var first = true
            for (p in paths.indices) {
                val v = pathValues[p]!![row]
                if (v == null) {
                    continue
                }
                if (!first) {
                    sb.append(',')
                }
                first = false
                appendQuoted(sb, paths[p]!!)
                sb.append(':')
                appendValue(sb, v)
            }
            return sb.append('}').toString()
        }

        @JvmStatic
        private fun appendValue(sb: StringBuilder, v: Any) {
            if (v is Number || v is Boolean) {
                sb.append(v)
            } else if (v is List<*>) {
                // An array-valued path (e.g. Array(Nullable(Int64)) under Dynamic) renders
                // as a JSON array, recursing per element.
                sb.append('[')
                for ((i, e) in v.withIndex()) {
                    if (i > 0) sb.append(',')
                    if (e == null) sb.append("null") else appendValue(sb, e)
                }
                sb.append(']')
            } else {
                appendQuoted(sb, v.toString())
            }
        }

        @JvmStatic
        private fun appendQuoted(sb: StringBuilder, s: String) {
            sb.append('"')
            for (i in 0 until s.length) {
                val c = s[i]
                when (c) {
                    '"' -> sb.append("\\\"")
                    '\\' -> sb.append("\\\\")
                    '\n' -> sb.append("\\n")
                    '\r' -> sb.append("\\r")
                    '\t' -> sb.append("\\t")
                    else -> sb.append(c)
                }
            }
            sb.append('"')
        }

        // -------------------------------------------------------------------------
        // Minimal JSON parser (flat objects with scalar values) — write path only
        // -------------------------------------------------------------------------

        /**
         * Parses a flat JSON object string into a `path -> scalar` map. Numbers become
         * [Long] (integral) or [Double]; strings [String]; `true`/`false`
         * [Boolean]; JSON `null` values are dropped (the path is absent for the row).
         *
         * @throws IllegalArgumentException if the document is not a flat object of scalars
         */
        @JvmStatic
        internal fun parseFlatObject(json: String): MutableMap<String, Any> {
            val s = JsonScanner(json)
            s.skipWs()
            val out = java.util.LinkedHashMap<String, Any>()
            s.expect('{')
            s.skipWs()
            if (s.peek() == '}') {
                s.next()
                s.skipWs()
                s.expectEnd()
                return out
            }
            while (true) {
                s.skipWs()
                val key = s.readString()
                s.skipWs()
                s.expect(':')
                s.skipWs()
                val value = s.readScalar()
                if (value != null) {
                    out[key] = value
                }
                s.skipWs()
                val c = s.next()
                if (c == ',') {
                    continue
                }
                if (c == '}') {
                    break
                }
                throw IllegalArgumentException("Malformed JSON object near index " + s.pos())
            }
            s.skipWs()
            s.expectEnd()
            return out
        }
    }
}
