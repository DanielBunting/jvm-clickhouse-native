package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.BulkInserter
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.QueryResult
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Offline tests for the [bulkInsert] lifecycle against a recording fake: `init` before rows,
 * `complete` after, `close` always — and **no** `complete` when row production fails (the core
 * then poisons the connection rather than recycling a desynced stream).
 */
class BulkInsertTest {

    private class RecordingInserter<T> : BulkInserter<T> {
        val events = ArrayList<String>()
        val rows = ArrayList<T>()
        override fun init() { events += "init" }
        override fun add(row: T) { events += "add"; rows += row }
        override fun addRange(rows: Iterable<T>) { rows.forEach(::add) }
        override fun complete() { events += "complete" }
        override fun close() { events += "close" }
    }

    private class InserterConnection<R>(private val inserter: BulkInserter<R>) : ClickHouseConnection {
        var requestedTable: String? = null
        var requestedType: Class<*>? = null
        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> createBulkInserter(table: String, type: Class<T>): BulkInserter<T> {
            requestedTable = table
            requestedType = type
            return inserter as BulkInserter<T>
        }
        override fun query(sql: String): QueryResult = throw NotImplementedError()
        override fun <T : Any?> query(sql: String, type: Class<T>): Stream<T> = throw NotImplementedError()
        override fun executeScalar(sql: String): Long = throw NotImplementedError()
        override fun execute(sql: String) = throw NotImplementedError()
        override fun queryAsync(sql: String): CompletableFuture<QueryResult> = throw NotImplementedError()
        override fun close() {}
    }

    @Test
    fun iterableFormRunsFullLifecycleInOrder() = runTest {
        val inserter = RecordingInserter<String>()
        val conn = InserterConnection(inserter)

        conn.bulkInsert("events", listOf("a", "b", "c"))

        assertEquals(listOf("init", "add", "add", "add", "complete", "close"), inserter.events)
        assertEquals(listOf("a", "b", "c"), inserter.rows)
        assertEquals("events", conn.requestedTable)
        assertEquals(String::class.java, conn.requestedType)
    }

    @Test
    fun flowFormCollectsAllRowsThenCompletes() = runTest {
        val inserter = RecordingInserter<Int>()
        val conn = InserterConnection(inserter)

        conn.bulkInsert("numbers", flowOf(1, 2, 3, 4))

        assertEquals(listOf(1, 2, 3, 4), inserter.rows)
        assertEquals("init", inserter.events.first())
        assertEquals(listOf("complete", "close"), inserter.events.takeLast(2))
    }

    @Test
    fun failingFlowClosesWithoutComplete() = runTest {
        val inserter = RecordingInserter<Int>()
        val conn = InserterConnection(inserter)
        val failing = flow {
            emit(1)
            emit(2)
            throw IllegalStateException("source broke")
        }

        assertFailsWith<IllegalStateException> { conn.bulkInsert("numbers", failing) }

        assertEquals(listOf(1, 2), inserter.rows)
        assertFalse("complete" in inserter.events, "complete() must not run after a failed source")
        assertTrue(inserter.events.last() == "close", "inserter must still be closed")
    }
}
