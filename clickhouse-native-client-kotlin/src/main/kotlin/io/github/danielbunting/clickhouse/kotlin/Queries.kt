package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.QueryParameters
import io.github.danielbunting.clickhouse.QueryResult
import io.github.danielbunting.clickhouse.protocol.Block
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext

/*
 * Coroutine + Flow extensions over the blocking Java [ClickHouseConnection].
 *
 * Naming note: the core interface already has *blocking* members `execute(String)` and
 * `executeScalar(String)`. A Kotlin member always wins over an extension of the same
 * name+signature, so a `suspend fun execute(...)` extension would silently bind to the blocking
 * member inside a coroutine. The suspend point-ops therefore use distinct names — [command] and
 * [scalar]. The Flow APIs ([queryFlow], [query], [queryAs]) introduce signatures the core does
 * not have, so they read as natural extensions without shadowing.
 *
 * A [ClickHouseConnection] is one socket and is not thread-safe: use it from a single coroutine at
 * a time. Genuinely concurrent use is rejected by the core's connection guard
 * (`ConcurrentConnectionUseException`). For concurrency, use independent connections — see the
 * pool extensions in `Pools.kt`.
 */

/** Runs DDL/DML with no result set (the suspend analog of [ClickHouseConnection.execute]). */
suspend fun ClickHouseConnection.command(
    sql: String,
    settings: Map<String, String> = emptyMap(),
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    withContext(dispatcher) {
        if (settings.isEmpty()) execute(sql) else execute(sql, settings)
    }
}

/**
 * Runs DDL/DML with server-side [params] bound into `{name:Type}` placeholders (the suspend
 * analog of [ClickHouseConnection.execute] with [QueryParameters]). Build the parameter set
 * with [queryParametersOf] or [queryParameters].
 */
suspend fun ClickHouseConnection.command(
    sql: String,
    params: QueryParameters,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    withContext(dispatcher) { execute(sql, params) }
}

/** Runs a query returning a single numeric scalar (the suspend analog of [ClickHouseConnection.executeScalar]). */
suspend fun ClickHouseConnection.scalar(
    sql: String,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Long = withContext(dispatcher) { executeScalar(sql) }

/** Runs a single-scalar query with server-side [params] bound into `{name:Type}` placeholders. */
suspend fun ClickHouseConnection.scalar(
    sql: String,
    params: QueryParameters,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Long = withContext(dispatcher) { executeScalar(sql, params) }

/**
 * Streams a SELECT as a cold [Flow] of [ResultRow]s.
 *
 * The underlying lazy [QueryResult][io.github.danielbunting.clickhouse.QueryResult] is opened when
 * collection starts and closed in a `finally` — on normal completion **and** on cancellation —
 * which releases the connection's guard. The whole producer (the blocking query + block
 * iteration) runs on [dispatcher] via [flowOn]. Empty blocks (insert terminators / progress
 * blocks) are skipped.
 *
 * One emitted [ResultRow] per row points at the current column-major block; collect/transform it
 * synchronously (e.g. via [query]) rather than retaining it across unrelated blocks.
 */
fun ClickHouseConnection.queryFlow(
    sql: String,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<ResultRow> = resultRowFlow(dispatcher) { query(sql) }

/**
 * Streams a SELECT with server-side [params] bound into `{name:Type}` placeholders as a cold
 * [Flow] of [ResultRow]s. Same lifecycle as [queryFlow] without parameters.
 */
fun ClickHouseConnection.queryFlow(
    sql: String,
    params: QueryParameters,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<ResultRow> = resultRowFlow(dispatcher) { query(sql, params) }

/**
 * Streams a SELECT as a cold [Flow] of whole column-major [Block]s — the low-overhead
 * primitive under [queryFlow] and [queryBatched].
 *
 * Each emission is one server block (its size is whatever the server sends — governed by
 * `max_block_size`, default 65,536 rows; the final block is naturally partial). Unlike the
 * per-row [queryFlow], this emits once per block rather than once per row, so the coroutine
 * channel introduced by [flowOn] is crossed ~`rows / blockSize` times instead of once per row.
 * Read values positionally off the block's columns (e.g. `block.column(0).longAt(row)`).
 *
 * Lifecycle matches [queryFlow]: the lazy [QueryResult] is opened on collection and closed in a
 * `finally` (completion **and** cancellation); empty blocks are skipped. A [Block] is a fresh
 * object per iteration, but **consume it inside the collector** — do not retain it past flow
 * completion, when the underlying result is closed.
 */
fun ClickHouseConnection.queryBlocks(
    sql: String,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<Block> = blockFlow(dispatcher) { query(sql) }

/**
 * Streams a SELECT with server-side [params] bound into `{name:Type}` placeholders as a cold
 * [Flow] of [Block]s. Same lifecycle as [queryBlocks] without parameters.
 */
fun ClickHouseConnection.queryBlocks(
    sql: String,
    params: QueryParameters,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<Block> = blockFlow(dispatcher) { query(sql, params) }

/**
 * Streams a SELECT, mapping each row to `T` with an explicit, allocation-free lambda:
 *
 * ```
 * conn.query("SELECT id, name FROM t") { row ->
 *     Event(row.long("id"), row.string("name"))
 * }.collect { ... }
 * ```
 *
 * The mapper runs on [dispatcher] (upstream of [flowOn]); produce immutable values from it.
 */
fun <T> ClickHouseConnection.query(
    sql: String,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    map: (ResultRow) -> T,
): Flow<T> = queryFlow(sql, dispatcher).map(map)

/** [query] with server-side [params] bound into `{name:Type}` placeholders. */
fun <T> ClickHouseConnection.query(
    sql: String,
    params: QueryParameters,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    map: (ResultRow) -> T,
): Flow<T> = queryFlow(sql, params, dispatcher).map(map)

/**
 * Streams a SELECT as a cold [Flow] of fixed-size row batches: each emitted [List] holds exactly
 * [batchSize] mapped values, except the final one, which carries the remainder (a partial chunk).
 *
 * ```
 * conn.query("SELECT id, value FROM t", batchSize = 100_000) { row ->
 *     MappedRow(row.long("id"), row.double("value"))
 * }.collect { batch -> /* batch.size == 100_000, last batch partial */ }
 * ```
 *
 * This is the ergonomic throughput path: rows are accumulated across server-block boundaries and
 * emitted in one [flowOn] handoff per batch instead of per row, which is the dominant cost of the
 * per-row [query]/[queryFlow] at high row counts. The batch size is exact and independent of the
 * server's `max_block_size`.
 *
 * [map] runs on [dispatcher] (upstream of [flowOn]) and must produce **immutable** values — the
 * emitted `List<T>` outlives the [ResultRow]/[Block] it was built from, so the row views must not
 * escape it. Lifecycle matches [queryFlow] (lazy open, `finally` close on completion and
 * cancellation). Throws [IllegalArgumentException] if [batchSize] is not positive.
 */
fun <T> ClickHouseConnection.queryBatched(
    sql: String,
    batchSize: Int,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    map: (ResultRow) -> T,
): Flow<List<T>> = batchedFlow(batchSize, dispatcher, { query(sql) }, map)

/** [queryBatched] with server-side [params] bound into `{name:Type}` placeholders. */
fun <T> ClickHouseConnection.queryBatched(
    sql: String,
    params: QueryParameters,
    batchSize: Int,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    map: (ResultRow) -> T,
): Flow<List<T>> = batchedFlow(batchSize, dispatcher, { query(sql, params) }, map)

/**
 * Streams a SELECT as a cold [Flow] of [type] instances, picking the mapping strategy by what
 * [type] is:
 *
 * - **Kotlin classes** (including ordinary immutable `data class`es — `val`s, no default
 *   values needed) bind result columns to the **primary constructor's parameters by name**,
 *   via the parameter names Kotlin keeps in `@Metadata`. The kotlin-reflect resolution runs
 *   once per query; rows are then built through the plain Java constructor.
 * - **Java records** and `@JvmRecord` data classes go through the core's record mapper
 *   (canonical constructor).
 * - **Other Java classes** go through the core's POJO mapper and need a no-argument
 *   constructor plus fields matching the result columns by name.
 *
 * Either way the underlying lazy result is closed in a `finally` — on completion and
 * cancellation — releasing the connection guard. The blocking production runs on [dispatcher].
 */
fun <T : Any> ClickHouseConnection.queryAs(
    sql: String,
    type: Class<T>,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<T> = if (isPrimaryConstructorBindable(type)) {
    constructorBoundFlow(sql, type, dispatcher)
} else {
    flow {
        query(sql, type).use { stream ->
            val rows = stream.iterator()
            while (rows.hasNext()) {
                emit(rows.next())
            }
        }
    }.flowOn(dispatcher)
}

/** [queryAs] over the primary-constructor binder: header resolved once, then row-by-row. */
private fun <T : Any> ClickHouseConnection.constructorBoundFlow(
    sql: String,
    type: Class<T>,
    dispatcher: CoroutineDispatcher,
): Flow<T> = flow {
    val result = query(sql)
    try {
        val mapper = PrimaryConstructorMapper(type, result.columnNames())
        val blocks = result.blocks()
        while (blocks.hasNext()) {
            val block = blocks.next()
            if (block.isEmpty) continue
            for (row in 0 until block.rowCount()) {
                emit(mapper.map(block, row))
            }
        }
    } finally {
        result.close()
    }
}.flowOn(dispatcher)

/** Reified-type convenience for [queryAs]: `conn.queryAs<Event>("SELECT ...")`. */
inline fun <reified T : Any> ClickHouseConnection.queryAs(
    sql: String,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<T> = queryAs(sql, T::class.java, dispatcher)

/**
 * Batched [queryAs]: streams a SELECT as a cold [Flow] of fixed-size [List]s of [type] instances —
 * the auto-mapping counterpart of [queryBatched] (which takes an explicit lambda). Each list holds
 * exactly [batchSize] mapped values, except the final one, which carries the remainder.
 *
 * ```
 * conn.queryAsBatched<Event>("SELECT id, name FROM t", batchSize = 100_000)
 *     .collect { batch -> /* List<Event>, last batch partial */ }
 * ```
 *
 * Mapping-strategy selection is identical to [queryAs] (Kotlin primary-constructor binding for
 * Kotlin classes; the core record/POJO mapper otherwise), and the batching is done upstream of
 * [flowOn] so the channel is crossed once per batch, not once per row. Lifecycle matches [queryAs]
 * (lazy open, `finally` close on completion and cancellation). Throws [IllegalArgumentException] if
 * [batchSize] is not positive.
 */
fun <T : Any> ClickHouseConnection.queryAsBatched(
    sql: String,
    type: Class<T>,
    batchSize: Int,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<List<T>> {
    require(batchSize > 0) { "batchSize must be > 0, was $batchSize" }
    return if (isPrimaryConstructorBindable(type)) {
        constructorBoundBatchedFlow(sql, type, batchSize, dispatcher)
    } else {
        streamBatchedFlow(sql, type, batchSize, dispatcher)
    }
}

/** Reified-type convenience for [queryAsBatched]: `conn.queryAsBatched<Event>("SELECT ...", 100_000)`. */
inline fun <reified T : Any> ClickHouseConnection.queryAsBatched(
    sql: String,
    batchSize: Int,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): Flow<List<T>> = queryAsBatched(sql, T::class.java, batchSize, dispatcher)

/** Shared producer for the [ResultRow] flows: open lazily, close on completion/cancellation. */
private fun ClickHouseConnection.resultRowFlow(
    dispatcher: CoroutineDispatcher,
    produce: ClickHouseConnection.() -> QueryResult,
): Flow<ResultRow> = flow {
    val result = produce()
    try {
        val names = result.columnNames()
        val nameToIndex = LinkedHashMap<String, Int>(names.size)
        names.forEachIndexed { i, n -> nameToIndex.putIfAbsent(n, i) }

        val blocks = result.blocks()
        while (blocks.hasNext()) {
            val block = blocks.next()
            if (block.isEmpty) continue
            for (row in 0 until block.rowCount()) {
                emit(ResultRow(block, row, nameToIndex))
            }
        }
    } finally {
        result.close()
    }
}.flowOn(dispatcher)

/** Shared producer for the [Block] flows: one emit per non-empty server block. */
private fun ClickHouseConnection.blockFlow(
    dispatcher: CoroutineDispatcher,
    produce: ClickHouseConnection.() -> QueryResult,
): Flow<Block> = flow {
    val result = produce()
    try {
        val blocks = result.blocks()
        while (blocks.hasNext()) {
            val block = blocks.next()
            if (block.isEmpty) continue
            emit(block)
        }
    } finally {
        result.close()
    }
}.flowOn(dispatcher)

/**
 * Shared producer for the batched flows: maps rows across block boundaries into fixed-size
 * [batchSize] lists, emitting a partial final list for the remainder. A fresh list is allocated
 * per batch because [flowOn]'s buffered channel may still hold an emitted list in flight.
 */
private fun <T> ClickHouseConnection.batchedFlow(
    batchSize: Int,
    dispatcher: CoroutineDispatcher,
    produce: ClickHouseConnection.() -> QueryResult,
    map: (ResultRow) -> T,
): Flow<List<T>> {
    require(batchSize > 0) { "batchSize must be > 0, was $batchSize" }
    return flow {
        val result = produce()
        try {
            val names = result.columnNames()
            val nameToIndex = LinkedHashMap<String, Int>(names.size)
            names.forEachIndexed { i, n -> nameToIndex.putIfAbsent(n, i) }

            var batch = ArrayList<T>(batchSize)
            val blocks = result.blocks()
            while (blocks.hasNext()) {
                val block = blocks.next()
                if (block.isEmpty) continue
                for (row in 0 until block.rowCount()) {
                    batch.add(map(ResultRow(block, row, nameToIndex)))
                    if (batch.size == batchSize) {
                        emit(batch)
                        batch = ArrayList(batchSize)
                    }
                }
            }
            if (batch.isNotEmpty()) emit(batch)
        } finally {
            result.close()
        }
    }.flowOn(dispatcher)
}

/**
 * [queryAsBatched] over the primary-constructor binder: header resolved once, then rows mapped and
 * accumulated into fixed-size [batchSize] lists (partial final list), batched upstream of [flowOn].
 */
private fun <T : Any> ClickHouseConnection.constructorBoundBatchedFlow(
    sql: String,
    type: Class<T>,
    batchSize: Int,
    dispatcher: CoroutineDispatcher,
): Flow<List<T>> = flow {
    val result = query(sql)
    try {
        val mapper = PrimaryConstructorMapper(type, result.columnNames())
        var batch = ArrayList<T>(batchSize)
        val blocks = result.blocks()
        while (blocks.hasNext()) {
            val block = blocks.next()
            if (block.isEmpty) continue
            for (row in 0 until block.rowCount()) {
                batch.add(mapper.map(block, row))
                if (batch.size == batchSize) {
                    emit(batch)
                    batch = ArrayList(batchSize)
                }
            }
        }
        if (batch.isNotEmpty()) emit(batch)
    } finally {
        result.close()
    }
}.flowOn(dispatcher)

/**
 * [queryAsBatched] over the core record/POJO mapper: chunks the lazy `query(sql, type)` stream into
 * fixed-size [batchSize] lists (partial final list), batched upstream of [flowOn].
 */
private fun <T : Any> ClickHouseConnection.streamBatchedFlow(
    sql: String,
    type: Class<T>,
    batchSize: Int,
    dispatcher: CoroutineDispatcher,
): Flow<List<T>> = flow {
    query(sql, type).use { stream ->
        val rows = stream.iterator()
        var batch = ArrayList<T>(batchSize)
        while (rows.hasNext()) {
            batch.add(rows.next())
            if (batch.size == batchSize) {
                emit(batch)
                batch = ArrayList(batchSize)
            }
        }
        if (batch.isNotEmpty()) emit(batch)
    }
}.flowOn(dispatcher)
