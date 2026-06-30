package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.mapping.RowMapper
import io.github.danielbunting.clickhouse.mapping.RowMapperFactory
import org.apache.arrow.adbc.core.BulkIngestMode
import org.apache.arrow.vector.BaseIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.Decimal256Vector
import org.apache.arrow.vector.DecimalVector
import org.apache.arrow.vector.DurationVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntervalYearVector
import org.apache.arrow.vector.TimeStampMicroTZVector
import org.apache.arrow.vector.TimeStampMilliTZVector
import org.apache.arrow.vector.TimeStampNanoTZVector
import org.apache.arrow.vector.TimeStampSecTZVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDate
import java.time.Period

/**
 * Write-path bridge: feeds a bound Arrow [VectorSchemaRoot] into the native bulk inserter as
 * the inverse of [BlockToArrow]. Each Arrow cell is reconstructed into the boxed Java value the
 * column's [io.github.danielbunting.clickhouse.types.ColumnCodec] accepts (`Long`/`Double`/
 * `String`/`LocalDate`/`Instant`/`BigDecimal`/`List`/`Map`), then fed column-aligned through a
 * [RowMapperFactory] so no intermediate POJO is materialised.
 */
public object ArrowToBlock {

    /**
     * Ingests every row of [root] into [targetTable] over [connection], returning the row count.
     *
     * [BulkIngestMode] controls table creation (the table is created from [root]'s Arrow schema
     * via [ClickHouseArrowTypes.clickHouseType], `ENGINE = MergeTree ORDER BY tuple()`):
     * - [APPEND][BulkIngestMode.APPEND]: the table must already exist.
     * - [CREATE][BulkIngestMode.CREATE]: create the table (error if it exists), then insert.
     * - [CREATE_APPEND][BulkIngestMode.CREATE_APPEND]: create if absent, then insert.
     * - [REPLACE][BulkIngestMode.REPLACE]: drop any existing table, recreate, then insert.
     */
    @JvmStatic
    public fun ingest(
        connection: ClickHouseConnection,
        targetTable: String,
        mode: BulkIngestMode,
        root: VectorSchemaRoot,
    ): Long {
        prepareTable(connection, targetTable, mode, root)
        val rowCount = root.rowCount
        if (rowCount == 0) {
            return 0
        }

        val byName = HashMap<String, FieldVector>()
        for (vector in root.fieldVectors) {
            byName[vector.field.name] = vector
        }

        // The factory runs once the sample block is read; align bound vectors to the target's
        // column order (by name), then bind each row index by reading those vectors.
        val factory = RowMapperFactory { targetColumnNames ->
            val ordered = Array(targetColumnNames.size) { i ->
                byName[targetColumnNames[i]]
                    ?: throw AdbcErrors.invalidArgument(
                        "Bound data has no column '${targetColumnNames[i]}' required by table '$targetTable'"
                    )
            }
            object : RowMapper<Int> {
                override fun columnNames(): Array<String> = targetColumnNames
                override fun map(columnValues: Array<Any?>): Int =
                    throw UnsupportedOperationException("ingest mapper is write-only")
                override fun bind(value: Int, dest: Array<Any?>) {
                    for (i in ordered.indices) {
                        dest[i] = toJavaValue(ordered[i], value)
                    }
                }
            }
        }

        // Name exactly the bound columns in the INSERT so a table column absent from the root
        // takes its server-side DEFAULT instead of failing the ingest.
        val boundColumns = root.fieldVectors.map { it.field.name }
        try {
            connection.createBulkInserter(targetTable, Int::class.javaObjectType, boundColumns, factory).use { inserter ->
                inserter.init()
                for (r in 0 until rowCount) {
                    inserter.add(r)
                }
                inserter.complete()
            }
        } catch (e: RuntimeException) {
            throw AdbcErrors.io("Bulk ingest into '$targetTable' failed: ${e.message}", e)
        }
        return rowCount.toLong()
    }

    /** Creates/drops the target table as dictated by [mode] before the insert. */
    private fun prepareTable(
        connection: ClickHouseConnection,
        targetTable: String,
        mode: BulkIngestMode,
        root: VectorSchemaRoot,
    ) {
        try {
            when (mode) {
                BulkIngestMode.APPEND -> Unit
                BulkIngestMode.CREATE -> connection.execute(createTableSql(targetTable, root, ifNotExists = false))
                BulkIngestMode.CREATE_APPEND -> connection.execute(createTableSql(targetTable, root, ifNotExists = true))
                BulkIngestMode.REPLACE -> {
                    connection.execute("DROP TABLE IF EXISTS $targetTable")
                    connection.execute(createTableSql(targetTable, root, ifNotExists = false))
                }
            }
        } catch (e: RuntimeException) {
            throw AdbcErrors.io("Preparing ingest target '$targetTable' ($mode) failed: ${e.message}", e)
        }
    }

    private fun createTableSql(targetTable: String, root: VectorSchemaRoot, ifNotExists: Boolean): String {
        val columns = root.schema.fields.joinToString(", ") { field ->
            "`${field.name}` ${ClickHouseArrowTypes.clickHouseType(field)}"
        }
        val guard = if (ifNotExists) "IF NOT EXISTS " else ""
        return "CREATE TABLE $guard$targetTable ($columns) ENGINE = MergeTree ORDER BY tuple()"
    }

    /**
     * Reconstructs the boxed Java value a ClickHouse codec accepts from one Arrow cell — the
     * inverse of [BlockToArrow]. Returns the same Java families the core client yields
     * (`Long`/`Double`/`String`/`Instant`/`LocalDate`/`BigDecimal`/`List`/`Map`), so it doubles as
     * the canonical Arrow→Java decoder for equivalence checks.
     */
    @JvmStatic
    public fun toJavaValue(vector: FieldVector, index: Int): Any? {
        if (vector.isNull(index)) {
            return null
        }
        return when (vector) {
            is BitVector -> vector.get(index) != 0
            is Float4Vector -> vector.get(index).toDouble()
            is Float8Vector -> vector.get(index)
            is BaseIntVector -> vector.getValueAsLong(index)
            is VarCharVector -> String(vector.get(index), StandardCharsets.UTF_8)
            is DateDayVector -> LocalDate.ofEpochDay(vector.get(index).toLong())
            is TimeStampSecTZVector -> Instant.ofEpochSecond(vector.get(index))
            is TimeStampMilliTZVector -> Instant.ofEpochMilli(vector.get(index))
            is TimeStampMicroTZVector -> {
                val t = vector.get(index)
                Instant.ofEpochSecond(Math.floorDiv(t, 1_000_000L), Math.floorMod(t, 1_000_000L) * 1_000L)
            }
            is TimeStampNanoTZVector -> {
                val t = vector.get(index)
                Instant.ofEpochSecond(Math.floorDiv(t, 1_000_000_000L), Math.floorMod(t, 1_000_000_000L))
            }
            is DecimalVector -> vector.getObject(index)
            is Decimal256Vector -> vector.getObject(index)
            // Time/Time64 and non-calendar Intervals → Duration; calendar Intervals → Period. The
            // ClickHouse Interval/Time codecs accept either back through their `set`.
            is DurationVector -> vector.getObject(index)
            is IntervalYearVector -> Period.ofMonths(vector.get(index))
            is FixedSizeBinaryVector -> vector.get(index)
            is MapVector -> readMap(vector, index)
            is ListVector -> readList(vector, index)
            is StructVector -> readStruct(vector, index)
            else -> throw AdbcErrors.notImplemented(
                "No ingest conversion for Arrow vector ${vector.javaClass.simpleName}"
            )
        }
    }

    private fun readList(vector: ListVector, index: Int): List<Any?> {
        val start = vector.getElementStartIndex(index)
        val end = vector.getElementEndIndex(index)
        val data = vector.dataVector as FieldVector
        val out = ArrayList<Any?>(end - start)
        for (j in start until end) {
            out.add(toJavaValue(data, j))
        }
        return out
    }

    private fun readStruct(vector: StructVector, index: Int): List<Any?> {
        val childCount = vector.field.children.size
        val out = ArrayList<Any?>(childCount)
        for (i in 0 until childCount) {
            out.add(toJavaValue(vector.getChildByOrdinal(i) as FieldVector, index))
        }
        return out
    }

    private fun readMap(vector: MapVector, index: Int): Map<Any?, Any?> {
        val start = vector.getElementStartIndex(index)
        val end = vector.getElementEndIndex(index)
        val entries = vector.dataVector as StructVector
        val keys = entries.getChildByOrdinal(0) as FieldVector
        val values = entries.getChildByOrdinal(1) as FieldVector
        val out = LinkedHashMap<Any?, Any?>()
        for (j in start until end) {
            out[toJavaValue(keys, j)] = toJavaValue(values, j)
        }
        return out
    }
}
