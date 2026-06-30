package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConnection
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

/**
 * An [AdbcStatement] over a single native [ClickHouseConnection].
 *
 * Query path: [setSqlQuery] + [executeQuery] runs `conn.query(sql)` and streams the result
 * back as a [BlockArrowReader]. Update path: [executeUpdate] runs DDL/DML via `conn.execute`,
 * or — when the statement was produced by [ChAdbcConnection.bulkIngest] and a root was
 * [bind]-ed — ingests the bound Arrow data through the native bulk inserter.
 */
public class ChAdbcStatement internal constructor(
    private val connection: ClickHouseConnection,
    private val connectionAllocator: BufferAllocator,
) : AdbcStatement {

    private var sqlQuery: String? = null
    private var boundRoot: VectorSchemaRoot? = null
    private var ingestTargetTable: String? = null
    private var ingestMode: BulkIngestMode? = null

    internal fun configureIngest(targetTable: String, mode: BulkIngestMode) {
        this.ingestTargetTable = targetTable
        this.ingestMode = mode
    }

    override fun setSqlQuery(query: String) {
        this.sqlQuery = query
    }

    override fun bind(root: VectorSchemaRoot) {
        this.boundRoot = root
    }

    override fun prepare() {
        // ClickHouse native statements need no separate prepare step; a no-op keeps
        // ADBC consumers that always call prepare() working.
    }

    override fun executeQuery(): AdbcStatement.QueryResult {
        val sql = sqlQuery
            ?: throw AdbcErrors.invalidState("setSqlQuery(...) must be called before executeQuery()")
        val result = try {
            connection.query(sql)
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
            connection.execute(sql)
        } catch (e: RuntimeException) {
            throw AdbcErrors.io("Update failed: ${e.message}", e)
        }
        // ClickHouse does not report an affected-row count over the native protocol.
        return AdbcStatement.UpdateResult(-1)
    }

    override fun cancel() {
        connection.cancel()
    }

    override fun close() {
        // Readers own their own allocators; bound roots are owned by the caller. Nothing
        // statement-scoped to free here.
    }
}
