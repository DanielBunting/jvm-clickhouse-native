package io.github.danielbunting.clickhouse.kotlin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class QueryFlowTest {

    private fun fakeConn(vararg blocks: io.github.danielbunting.clickhouse.protocol.Block): FakeConnection {
        val result = FakeQueryResult(
            names = listOf("id", "name"),
            types = listOf("Int64", "String"),
            blocks = blocks.toList(),
        )
        return FakeConnection(result)
    }

    @Test
    fun emitsRowsInOrderAndSkipsEmptyBlocks() = runTest {
        val conn = fakeConn(
            blockOf(longColumn("id", 1, 2), stringColumn("name", "a", "b")),
            emptyBlock(),
            blockOf(longColumn("id", 3), stringColumn("name", "c")),
        )

        val rows = conn.query("SELECT id, name FROM t", dispatcher = Dispatchers.Unconfined) { row ->
            row.long("id") to row.string("name")
        }.toList()

        assertEquals(listOf(1L to "a", 2L to "b", 3L to "c"), rows)
    }

    @Test
    fun closesResultOnNormalCompletion() = runTest {
        val result = FakeQueryResult(
            names = listOf("id", "name"),
            types = listOf("Int64", "String"),
            blocks = listOf(blockOf(longColumn("id", 1), stringColumn("name", "a"))),
        )
        val conn = FakeConnection(result)

        conn.queryFlow("SELECT id, name FROM t", dispatcher = Dispatchers.Unconfined).collect { }

        assertTrue(result.closed, "QueryResult should be closed after the flow completes")
    }

    @Test
    fun closesResultOnCancellation() = runTest {
        val result = FakeQueryResult(
            names = listOf("id", "name"),
            types = listOf("Int64", "String"),
            blocks = listOf(blockOf(longColumn("id", 1, 2, 3), stringColumn("name", "a", "b", "c"))),
        )
        val conn = FakeConnection(result)

        // take(1) cancels the upstream after the first row — the flow's finally must still close.
        val first = conn.queryFlow("SELECT id, name FROM t", dispatcher = Dispatchers.Unconfined)
            .take(1)
            .toList()

        assertEquals(1, first.size)
        assertTrue(result.closed, "QueryResult should be closed when collection is cancelled early")
    }
}
