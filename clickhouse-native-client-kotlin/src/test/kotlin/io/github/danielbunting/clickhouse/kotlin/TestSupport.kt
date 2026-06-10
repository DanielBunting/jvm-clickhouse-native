package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.BulkInserter
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.QueryResult
import io.github.danielbunting.clickhouse.protocol.Block
import io.github.danielbunting.clickhouse.types.Column
import io.github.danielbunting.clickhouse.types.codec.Float64Codec
import io.github.danielbunting.clickhouse.types.codec.Int64Codec
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream

// ---- Hand-built columns / blocks (no server required) -------------------------------------

fun longColumn(name: String, vararg values: Long, nulls: BooleanArray? = null): Column =
    Column(name, if (nulls == null) "Int64" else "Nullable(Int64)").apply {
        codec(Int64Codec())
        values(values)
        rowCount(values.size)
        if (nulls != null) nulls(nulls)
    }

fun doubleColumn(name: String, vararg values: Double): Column =
    Column(name, "Float64").apply {
        codec(Float64Codec())
        values(values)
        rowCount(values.size)
    }

fun stringColumn(name: String, vararg values: String?, nulls: BooleanArray? = null): Column {
    val codec = StringColumnCodec()
    val holder = codec.allocate(values.size)
    values.forEachIndexed { i, v -> codec.set(holder, i, v ?: "") }
    return Column(name, if (nulls == null) "String" else "Nullable(String)").apply {
        codec(codec)
        values(holder)
        rowCount(values.size)
        if (nulls != null) nulls(nulls)
    }
}

fun blockOf(vararg columns: Column): Block = Block().apply {
    val rows = columns.firstOrNull()?.rowCount() ?: 0
    columns.forEach { addColumn(it) }
    rowCount(rows)
}

/** An always-empty block (insert terminator / progress block) — should be skipped by the flow. */
fun emptyBlock(): Block = Block()

// ---- Fakes ---------------------------------------------------------------------------------

/** A canned [QueryResult] over pre-built blocks that records whether it was closed. */
class FakeQueryResult(
    private val names: List<String>,
    private val types: List<String>,
    private val blocks: List<Block>,
) : QueryResult {
    var closed: Boolean = false
        private set

    override fun columnNames(): List<String> = names
    override fun columnTypes(): List<String> = types
    override fun blocks(): Iterator<Block> = blocks.iterator()
    override fun close() {
        closed = true
    }
}

/** A [ClickHouseConnection] whose `query(sql)` hands back a fixed [FakeQueryResult]. */
class FakeConnection(private val result: FakeQueryResult) : ClickHouseConnection {
    override fun query(sql: String): QueryResult = result
    override fun executeScalar(sql: String): Long = throw NotImplementedError()
    override fun execute(sql: String) = throw NotImplementedError()
    override fun <T : Any?> query(sql: String, type: Class<T>): Stream<T> = throw NotImplementedError()
    override fun <T : Any?> createBulkInserter(table: String, type: Class<T>): BulkInserter<T> =
        throw NotImplementedError()
    override fun queryAsync(sql: String): CompletableFuture<QueryResult> = throw NotImplementedError()
    override fun close() {}
}
