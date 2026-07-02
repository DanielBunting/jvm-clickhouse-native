package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.sql.SqlPlaceholders
import org.apache.arrow.vector.DurationVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.arrow.vector.IntervalYearVector
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.VectorSchemaRoot
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Builds core [QueryParameters] from one row of a bound Arrow [VectorSchemaRoot] — the bridge
 * between ADBC's `bind(root)` parameter surface and the native protocol's server-side
 * `{name:Type}` parameters.
 *
 * Two binding shapes, decided by the statement's SQL:
 *  - **Positional** (`?` placeholders): root fields map to placeholders left-to-right; field
 *    names are ignored. The field count must equal the placeholder count.
 *  - **Named** (`{name:Type}` tokens): root fields map by name; every token must have a
 *    matching field.
 *
 * Values travel as ClickHouse text (see [QueryParameters.toText]); the server casts against
 * the placeholder's declared type. Cell decoding reuses [ArrowToBlock.toJavaValue], with the
 * representation-lossy families re-widened via the field's `clickhouse.type` metadata (a
 * `FixedSizeBinary(16)` cell is a UUID or an IPv6 address only the metadata can distinguish).
 */
internal object ArrowParameters {

    /** Positional binding: root fields → `_p1.._pN` for the given placeholder count. */
    fun positionalFromRow(root: VectorSchemaRoot, row: Int, placeholderCount: Int): QueryParameters {
        val vectors = root.fieldVectors
        if (vectors.size != placeholderCount) {
            throw AdbcErrors.invalidArgument(
                "SQL has $placeholderCount positional placeholder(s) but the bound root has " +
                    "${vectors.size} field(s); bind exactly one field per '?'"
            )
        }
        val b = QueryParameters.builder()
        vectors.forEachIndexed { i, vector ->
            b.bind(SqlPlaceholders.PARAM_NAME_PREFIX + (i + 1), parameterValue(vector, row))
        }
        return b.build()
    }

    /** Named binding: root fields → the SQL's `{name:Type}` tokens, matched by field name. */
    fun namedFromRow(
        root: VectorSchemaRoot,
        row: Int,
        named: List<SqlPlaceholders.NamedParameter>,
    ): QueryParameters {
        val byName = root.fieldVectors.associateBy { it.field.name }
        val b = QueryParameters.builder()
        for (param in named) {
            val vector = byName[param.name]
                ?: throw AdbcErrors.invalidArgument(
                    "SQL references parameter '{${param.name}:${param.type}}' but the bound root " +
                        "has no field named '${param.name}' (fields: ${byName.keys})"
                )
            b.bind(param.name, parameterValue(vector, row))
        }
        return b.build()
    }

    /**
     * Decodes one Arrow cell into the boxed Java value [QueryParameters] renders as wire text.
     * Null cells bind SQL NULL.
     */
    fun parameterValue(vector: FieldVector, row: Int): Any? {
        if (vector.isNull(row)) {
            return null
        }
        val chType = vector.field.metadata[ClickHouseArrowTypes.CH_TYPE_META]
        when (vector) {
            is FixedSizeBinaryVector -> {
                val bytes = vector.get(row)
                when {
                    chType == "UUID" && bytes.size == 16 -> {
                        val buf = ByteBuffer.wrap(bytes)
                        return UUID(buf.long, buf.long)
                    }
                    (chType == "IPv6" || chType == "IPv4") -> return InetAddress.getByAddress(bytes)
                }
            }
            is UInt4Vector -> if (chType == "IPv4") {
                val raw = vector.get(row)
                return InetAddress.getByAddress(
                    byteArrayOf(
                        (raw ushr 24).toByte(), (raw ushr 16).toByte(),
                        (raw ushr 8).toByte(), raw.toByte(),
                    )
                )
            }
            // Duration/Interval have no ClickHouse parameter text form.
            is DurationVector, is IntervalYearVector -> throw AdbcErrors.notImplemented(
                "Time/Interval values are not supported as query parameters " +
                    "(field '${vector.field.name}')"
            )
            else -> Unit
        }
        return ArrowToBlock.toJavaValue(vector, row)
    }
}
