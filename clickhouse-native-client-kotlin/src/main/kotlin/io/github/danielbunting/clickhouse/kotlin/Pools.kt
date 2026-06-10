package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import java.time.Duration

/*
 * Coroutine extensions over [ClickHouseConnectionPool].
 *
 * Naming note (same rule as `Queries.kt`): the pool already has *blocking* members
 * `withConnection(Function)` and `useConnection(Consumer)`. A same-named suspend extension would
 * lose to the member during overload resolution, silently running blocking work on a coroutine
 * thread. The suspend scope-op is therefore named distinctly — [lease].
 */

/**
 * Opens a [ClickHouseConnectionPool] of [size] independent connections for this config, running
 * the (blocking) eager connection opening on [dispatcher].
 *
 * A single connection is one socket and one coroutine at a time; the pool is the unit of
 * concurrency — `N` coroutines can [lease] `N` connections in parallel. Close the pool with
 * `use { }` when done.
 */
suspend fun ClickHouseConfig.openPool(
    size: Int = 8,
    borrowTimeout: Duration? = null,
    validateOnBorrow: Boolean? = null,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
): ClickHouseConnectionPool = withContext(dispatcher) {
    val builder = ClickHouseConnectionPool.builder(this@openPool).size(size)
    borrowTimeout?.let(builder::borrowTimeout)
    validateOnBorrow?.let(builder::validateOnBorrow)
    builder.build()
}

/**
 * Borrows a connection, runs [block] with it, and always returns it to the pool — the suspend,
 * leak-proof analog of [ClickHouseConnectionPool.withConnection]:
 *
 * ```
 * val total = pool.lease { conn -> conn.scalar("SELECT count() FROM t") }
 * ```
 *
 * The blocking borrow (which may wait up to the pool's borrow timeout) and the return both run
 * on [dispatcher]; the return is [NonCancellable], so a cancelled [block] can never leak its
 * permit. [block] itself is a suspend function and runs in the caller's context — it may freely
 * call the suspend ops ([command], [scalar], [queryFlow], [bulkInsert]) on the leased connection.
 */
suspend fun <T> ClickHouseConnectionPool.lease(
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    block: suspend (ClickHouseConnection) -> T,
): T {
    val conn = withContext(dispatcher) { borrow() }
    try {
        return block(conn)
    } finally {
        withContext(NonCancellable + dispatcher) { conn.close() }
    }
}
