package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.ClickHouseConnection
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext

/*
 * Suspend wrappers over the core's [BulkInserter][io.github.danielbunting.clickhouse.BulkInserter]
 * — the allocation-lean, column-buffered native insert path.
 *
 * Each call runs the full inserter lifecycle (`init` → add rows → `complete`) on [dispatcher]
 * inside a `use { }`, so the connection guard is always released. If row production fails (or
 * the coroutine is cancelled) mid-insert, the inserter is closed without `complete()`: the core
 * marks the connection poisoned (abandoned mid-INSERT) rather than risking a desynced stream —
 * a pooled connection is then discarded instead of recycled.
 */

/**
 * Bulk-inserts [rows] into [table], mapping [T]'s fields to columns by name through the core's
 * object mapper ([T] is a Java record, or a class whose fields match the target columns).
 *
 * Blocking I/O (schema fetch, block flushes) runs on [dispatcher]; the calling coroutine's
 * thread is never blocked.
 */
suspend fun <T : Any> ClickHouseConnection.bulkInsert(
    table: String,
    type: Class<T>,
    rows: Iterable<T>,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    withContext(dispatcher) {
        createBulkInserter(table, type).use { inserter ->
            inserter.init()
            inserter.addRange(rows)
            inserter.complete()
        }
    }
}

/** Reified-type convenience for [bulkInsert]: `conn.bulkInsert("events", rows)`. */
suspend inline fun <reified T : Any> ClickHouseConnection.bulkInsert(
    table: String,
    rows: Iterable<T>,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    bulkInsert(table, T::class.java, rows, dispatcher)
}

/**
 * Bulk-inserts a [Flow] of rows into [table] — the streaming form of [bulkInsert] for row
 * sources that are themselves asynchronous (another query, a channel, a paginated fetch).
 *
 * The flow is collected on [dispatcher] (the whole insert runs there); use
 * [flowOn][kotlinx.coroutines.flow.flowOn] upstream if production must happen elsewhere. Rows
 * are buffered into column blocks and flushed batch-wise by the core inserter as they arrive.
 */
suspend fun <T : Any> ClickHouseConnection.bulkInsert(
    table: String,
    type: Class<T>,
    rows: Flow<T>,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    withContext(dispatcher) {
        createBulkInserter(table, type).use { inserter ->
            inserter.init()
            rows.collect { inserter.add(it) }
            inserter.complete()
        }
    }
}

/** Reified-type convenience for the [Flow] form of [bulkInsert]. */
suspend inline fun <reified T : Any> ClickHouseConnection.bulkInsert(
    table: String,
    rows: Flow<T>,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    bulkInsert(table, T::class.java, rows, dispatcher)
}
