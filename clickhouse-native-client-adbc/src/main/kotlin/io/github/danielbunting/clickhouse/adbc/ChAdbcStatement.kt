package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.sql.SqlPlaceholders
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

/**
 * An [AdbcStatement] over a single native [ClickHouseConnection].
 *
 * Query path: [setSqlQuery] + [executeQuery] runs `conn.query(sql)` and streams the result
 * back as a [BlockArrowReader]. Update path: [executeUpdate] runs DDL/DML via `conn.execute`,
 * or — when the statement was produced by [ChAdbcConnection.bulkIngest] and a root was
 * [bind]-ed — ingests the bound Arrow data through the native bulk inserter.
 *
 * **Parameterized queries.** When the SQL carries placeholders, [bind] supplies parameters
 * instead of ingest data, one parameter set per ROW of the bound root:
 *
 *  - Positional `?` placeholders are rewritten to server-side `{_pN:String}` parameters
 *    (`Nullable(String)` for null cells); root fields map to them left-to-right.
 *  - User-written `{name:Type}` placeholders pass through untouched; root fields map by name.
 *
 * Values travel separately on the Query packet as ClickHouse text and the SERVER casts them
 * against each placeholder's type — no client-side string interpolation, hence no injection
 * or type-fidelity hazard. [executeQuery] uses exactly one parameter row;
 * [executeUpdate] runs the statement once per row (the batch shape). [getParameterSchema]
 * reports the expected root shape.
 */
public class ChAdbcStatement internal constructor(
    private val connection: ClickHouseConnection,
    private val connectionAllocator: BufferAllocator,
) : AdbcStatement {

    private var sqlQuery: String? = null
    private var boundRoot: VectorSchemaRoot? = null
    private var ingestTargetTable: String? = null
    private var ingestMode: BulkIngestMode? = null
    private var closed = false

    /** Offsets of positional `?` placeholders in [sqlQuery] (empty when none). */
    private var positionalOffsets: List<Int> = emptyList()

    /** User-written `{name:Type}` parameters of [sqlQuery] (empty when none). */
    private var namedParameters: List<SqlPlaceholders.NamedParameter> = emptyList()

    private fun checkOpen() {
        if (closed) {
            throw AdbcErrors.invalidState("Statement is closed")
        }
    }

    internal fun configureIngest(targetTable: String, mode: BulkIngestMode) {
        this.ingestTargetTable = targetTable
        this.ingestMode = mode
    }

    override fun setSqlQuery(query: String) {
        this.sqlQuery = query
        this.positionalOffsets = SqlPlaceholders.positions(query)
        // With positional placeholders present the root binds by position; any named tokens
        // are treated as user-managed and pass through unbound (mirrors the JDBC driver,
        // where user-written {name:Type} coexists with ? placeholders untouched).
        this.namedParameters =
            if (positionalOffsets.isEmpty()) SqlPlaceholders.namedParameters(query) else emptyList()
    }

    override fun bind(root: VectorSchemaRoot) {
        this.boundRoot = root
    }

    override fun prepare() {
        // ClickHouse native statements need no separate server prepare step; placeholder
        // analysis already happened in setSqlQuery. A no-op keeps ADBC consumers that
        // always call prepare() working.
    }

    private fun hasParameters(): Boolean =
        positionalOffsets.isNotEmpty() || namedParameters.isNotEmpty()

    /** The parameter root, validated to carry at least [minRows] rows. */
    private fun parameterRoot(minRows: Int): VectorSchemaRoot {
        val root = boundRoot
            ?: throw AdbcErrors.invalidState(
                "The SQL has parameter placeholders; bind(VectorSchemaRoot) must supply the values"
            )
        if (root.rowCount < minRows) {
            throw AdbcErrors.invalidState(
                "The bound parameter root must carry at least $minRows row(s), got ${root.rowCount}"
            )
        }
        return root
    }

    /** Builds the parameter set for [row] of [root]. */
    private fun parametersFor(root: VectorSchemaRoot, row: Int): QueryParameters =
        if (positionalOffsets.isNotEmpty()) {
            ArrowParameters.positionalFromRow(root, row, positionalOffsets.size)
        } else {
            ArrowParameters.namedFromRow(root, row, namedParameters)
        }

    /** The effective SQL for [row]: `?`s rewritten per-row (null cells go `Nullable(String)`). */
    private fun effectiveSql(sql: String, root: VectorSchemaRoot, row: Int): String =
        if (positionalOffsets.isEmpty()) {
            sql
        } else {
            SqlPlaceholders.rewriteToNamedParams(sql, positionalOffsets) { param ->
                root.fieldVectors[param - 1].isNull(row)
            }
        }

    override fun executeQuery(): AdbcStatement.QueryResult {
        checkOpen()
        val sql = sqlQuery
            ?: throw AdbcErrors.invalidState("setSqlQuery(...) must be called before executeQuery()")
        val result = try {
            if (hasParameters()) {
                val root = parameterRoot(1)
                if (root.rowCount > 1) {
                    // One result stream per execution; a multi-row parameter batch would need
                    // concatenated results. Use executeUpdate for row-per-row batches.
                    throw AdbcErrors.notImplemented(
                        "executeQuery binds exactly one parameter row, got ${root.rowCount}; " +
                            "use executeUpdate for parameter batches"
                    )
                }
                // Build the parameters first: it validates field count/names before the
                // rewrite indexes the root's vectors.
                val params = parametersFor(root, 0)
                connection.query(effectiveSql(sql, root, 0), params)
            } else {
                connection.query(sql)
            }
        } catch (e: RuntimeException) {
            // wrap (not io) so an UnsupportedTypeException (e.g. an AggregateFunction state column)
            // surfaces as NOT_IMPLEMENTED; every other failure still maps to IO.
            throw AdbcErrors.wrap("Query failed", e)
        }
        val readerAllocator =
            connectionAllocator.newChildAllocator("adbc-reader", 0, Long.MAX_VALUE)
        val reader = try {
            val schema = ClickHouseArrowTypes.schema(result.columnNames(), result.columnTypes())
            BlockArrowReader(readerAllocator, result, schema)
        } catch (t: Throwable) {
            // Free both resources even if closing the result throws, then surface a clean
            // AdbcException (e.g. an unsupported column type becomes NOT_IMPLEMENTED).
            try {
                result.close()
            } finally {
                readerAllocator.close()
            }
            throw AdbcErrors.wrap("Query failed", t)
        }
        // Affected-row count is not meaningful for a SELECT; ADBC uses -1 for "unknown".
        return AdbcStatement.QueryResult(-1, reader)
    }

    override fun executeUpdate(): AdbcStatement.UpdateResult {
        checkOpen()
        val target = ingestTargetTable
        if (target != null) {
            val root = boundRoot
                ?: throw AdbcErrors.invalidState("bind(VectorSchemaRoot) must be called before ingest")
            val rows = ArrowToBlock.ingest(connection, target, ingestMode!!, root)
            return AdbcStatement.UpdateResult(rows)
        }
        val sql = sqlQuery
            ?: throw AdbcErrors.invalidState("setSqlQuery(...) must be called before executeUpdate()")
        try {
            if (hasParameters()) {
                // One execution per parameter row — the batch shape of JDBC's executeBatch
                // under server-side parameters.
                val root = parameterRoot(1)
                for (row in 0 until root.rowCount) {
                    val params = parametersFor(root, row)
                    connection.execute(effectiveSql(sql, root, row), params)
                }
            } else {
                connection.execute(sql)
            }
        } catch (e: RuntimeException) {
            throw AdbcErrors.wrap("Update failed", e)
        }
        // ClickHouse does not report an affected-row count over the native protocol.
        return AdbcStatement.UpdateResult(-1)
    }

    /**
     * The Arrow schema of the parameter root [bind] expects: `_p1.._pN` as nullable `Utf8`
     * for positional `?` placeholders (values travel as text the server casts), or the
     * declared name/type of each user-written `{name:Type}` parameter. Empty when the SQL
     * has no parameters.
     */
    override fun getParameterSchema(): Schema {
        checkOpen()
        val sql = sqlQuery
            ?: throw AdbcErrors.invalidState("setSqlQuery(...) must be called before getParameterSchema()")
        if (positionalOffsets.isNotEmpty()) {
            return Schema(
                (1..positionalOffsets.size).map { n ->
                    Field(
                        SqlPlaceholders.PARAM_NAME_PREFIX + n,
                        FieldType(true, ArrowType.Utf8(), null),
                        null,
                    )
                }
            )
        }
        try {
            return Schema(namedParameters.map { ClickHouseArrowTypes.arrowField(it.name, it.type) })
        } catch (e: RuntimeException) {
            throw AdbcErrors.wrap("Cannot derive a parameter schema from '$sql'", e)
        }
    }

    override fun cancel() {
        connection.cancel()
    }

    override fun close() {
        // Readers own their own allocators; bound roots are owned by the caller. Nothing
        // statement-scoped to free here — the flag only arms the closed-state guards.
        closed = true
    }
}
