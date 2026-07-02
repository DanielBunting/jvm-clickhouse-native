package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConnection
import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcInfoCode
import org.apache.arrow.adbc.core.AdbcStatement
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.adbc.core.StandardSchemas
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.complex.DenseUnionVector
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import java.nio.charset.StandardCharsets

/**
 * An [AdbcConnection] wrapping one native [ClickHouseConnection] (one ADBC connection = one
 * native connection; pool externally, as with the core client).
 *
 * Owns [connectionAllocator] (a child of the database allocator); statements and the readers
 * they hand out allocate from children of it, so [close] asserts nothing leaked.
 *
 * ClickHouse offers no general multi-statement transactions, so only autocommit is supported —
 * `setAutoCommit(false)`, `commit()` and `rollback()` raise `NOT_IMPLEMENTED`, mirroring the
 * JDBC module.
 */
public class ChAdbcConnection internal constructor(
    private val connection: ClickHouseConnection,
    private val connectionAllocator: BufferAllocator,
) : AdbcConnection {

    override fun createStatement(): AdbcStatement {
        checkOpen()
        return ChAdbcStatement(connection, connectionAllocator)
    }

    override fun bulkIngest(targetTableName: String, mode: BulkIngestMode): AdbcStatement {
        checkOpen()
        val statement = ChAdbcStatement(connection, connectionAllocator)
        statement.configureIngest(targetTableName, mode)
        return statement
    }

    override fun cancel() {
        checkOpen()
        connection.cancel()
    }

    override fun getTableSchema(catalog: String?, dbSchema: String?, tableName: String?): Schema {
        checkOpen()
        val table = tableName
            ?: throw AdbcErrors.invalidArgument("getTableSchema requires a table name")
        val database = dbSchema?.let { "'" + escape(it) + "'" } ?: "currentDatabase()"
        val sql = "SELECT name, type FROM system.columns WHERE database = $database " +
            "AND table = '" + escape(table) + "' ORDER BY position"

        val fields = ArrayList<org.apache.arrow.vector.types.pojo.Field>()
        try {
            connection.query(sql).use { result ->
                val blocks = result.blocks()
                while (blocks.hasNext()) {
                    val block = blocks.next()
                    if (block.isEmpty) {
                        continue
                    }
                    val nameCol = block.column(0)
                    val typeCol = block.column(1)
                    for (r in 0 until block.rowCount()) {
                        fields.add(ClickHouseArrowTypes.arrowField(nameCol.stringAt(r)!!, typeCol.stringAt(r)!!))
                    }
                }
            }
        } catch (e: RuntimeException) {
            throw AdbcErrors.io("getTableSchema failed: ${e.message}", e)
        }
        if (fields.isEmpty()) {
            throw AdbcErrors.notFound("Table '$table' not found in ${dbSchema ?: "the current database"}")
        }
        return Schema(fields)
    }

    override fun getObjects(
        depth: AdbcConnection.GetObjectsDepth,
        catalogPattern: String?,
        dbSchemaPattern: String?,
        tableNamePattern: String?,
        tableTypes: Array<String>?,
        columnNamePattern: String?,
    ): ArrowReader {
        checkOpen()
        return simpleReader(StandardSchemas.GET_OBJECTS_SCHEMA) { root ->
            try {
                GetObjectsBuilder.build(
                    connection, depth, catalogPattern, dbSchemaPattern,
                    tableNamePattern, tableTypes, columnNamePattern, root,
                )
            } catch (e: RuntimeException) {
                throw AdbcErrors.io("getObjects failed: ${e.message}", e)
            }
        }
    }

    override fun getTableTypes(): ArrowReader {
        checkOpen()
        return simpleReader(StandardSchemas.TABLE_TYPES_SCHEMA) { root ->
            val types = listOf("TABLE", "VIEW")
            val vector = root.getVector("table_type") as VarCharVector
            types.forEachIndexed { i, t -> vector.setSafe(i, t.toByteArray(StandardCharsets.UTF_8)) }
            vector.valueCount = types.size
            root.rowCount = types.size
        }
    }

    override fun getInfo(infoCodes: IntArray?): ArrowReader {
        checkOpen()
        val version = try {
            connection.query("SELECT version()").use { result ->
                val blocks = result.blocks()
                var v = "unknown"
                while (blocks.hasNext()) {
                    val block = blocks.next()
                    if (!block.isEmpty) {
                        v = block.column(0).stringAt(0) ?: "unknown"
                    }
                }
                v
            }
        } catch (e: RuntimeException) {
            throw AdbcErrors.io("getInfo failed reading server version: ${e.message}", e)
        }

        val known = linkedMapOf(
            AdbcInfoCode.VENDOR_NAME.value to "ClickHouse",
            AdbcInfoCode.VENDOR_VERSION.value to version,
            AdbcInfoCode.DRIVER_NAME.value to ChAdbcDriver.DRIVER_NAME,
            AdbcInfoCode.DRIVER_VERSION.value to ChAdbcDriver.DRIVER_VERSION,
        )
        val requested = if (infoCodes == null || infoCodes.isEmpty()) {
            known.keys.toList()
        } else {
            infoCodes.toList().filter { known.containsKey(it) }
        }

        return simpleReader(StandardSchemas.GET_INFO_SCHEMA) { root ->
            val names = root.getVector("info_name") as UInt4Vector
            val values = root.getVector("info_value") as DenseUnionVector
            val stringChild = values.getVarCharVector(INFO_STRING_TYPE_ID)
            requested.forEachIndexed { i, code ->
                names.setSafe(i, code)
                values.setTypeId(i, INFO_STRING_TYPE_ID)
                stringChild.setSafe(i, known.getValue(code).toByteArray(StandardCharsets.UTF_8))
                values.setOffset(i, i)
            }
            names.valueCount = requested.size
            values.setValueCount(requested.size)
            root.rowCount = requested.size
        }
    }

    private fun simpleReader(schema: Schema, populate: (org.apache.arrow.vector.VectorSchemaRoot) -> Unit): ArrowReader {
        val readerAllocator = connectionAllocator.newChildAllocator("adbc-metadata", 0, Long.MAX_VALUE)
        return try {
            SimpleArrowReader(readerAllocator, schema, populate)
        } catch (t: Throwable) {
            readerAllocator.close()
            throw t
        }
    }

    private fun escape(value: String): String = value.replace("'", "''")

    override fun getAutoCommit(): Boolean = true

    override fun setAutoCommit(enableAutoCommit: Boolean) {
        if (!enableAutoCommit) {
            throw AdbcErrors.notImplemented("ClickHouse supports autocommit only")
        }
    }

    override fun commit() {
        throw AdbcErrors.notImplemented("ClickHouse supports autocommit only; commit() is unsupported")
    }

    override fun rollback() {
        throw AdbcErrors.notImplemented("ClickHouse supports autocommit only; rollback() is unsupported")
    }

    private var closed = false

    private fun checkOpen() {
        if (closed) {
            throw AdbcErrors.invalidState("Connection is closed")
        }
    }

    override fun close() {
        if (closed) {
            return
        }
        closed = true
        try {
            connection.close()
        } finally {
            connectionAllocator.close()
        }
    }

    private companion object {
        // ADBC's GET_INFO `info_value` is a dense union; type id 0 is the `string_value` child.
        const val INFO_STRING_TYPE_ID: Byte = 0
    }
}
