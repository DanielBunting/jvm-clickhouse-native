package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.protocol.Block
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Unit tests for the block-granular ([queryBlocks]) and batched ([queryBatched]) flows. */
class BatchedQueryTest {

    private fun connOf(vararg blocks: Block): Pair<FakeConnection, FakeQueryResult> {
        val result = FakeQueryResult(
            names = listOf("id", "name"),
            types = listOf("Int64", "String"),
            blocks = blocks.toList(),
        )
        return FakeConnection(result) to result
    }

    /** A non-empty block of `id` + `name` columns from the given ids. */
    private fun idBlock(vararg ids: Long): Block =
        blockOf(longColumn("id", *ids), stringColumn("name", *Array(ids.size) { "n$it" }))

    // ---- queryBlocks ---------------------------------------------------------------------

    @Test
    fun blocksEmitsOnePerNonEmptyBlockAndSkipsEmpty() = runTest {
        val (conn, _) = connOf(idBlock(1, 2, 3), emptyBlock(), idBlock(4, 5))

        val blocks = conn.queryBlocks("SELECT id, name FROM t", dispatcher = Dispatchers.Unconfined).toList()

        assertEquals(2, blocks.size, "empty block should be skipped")
        assertEquals(listOf(3, 2), blocks.map { it.rowCount() })
        assertEquals(5, blocks.sumOf { it.rowCount() }, "row-count sum should equal total rows")
    }

    @Test
    fun blocksClosesResultOnCompletion() = runTest {
        val (conn, result) = connOf(idBlock(1, 2))

        conn.queryBlocks("SELECT id, name FROM t", dispatcher = Dispatchers.Unconfined).collect { }

        assertTrue(result.closed, "QueryResult should be closed after the flow completes")
    }

    @Test
    fun blocksClosesResultOnCancellation() = runTest {
        val (conn, result) = connOf(idBlock(1, 2), idBlock(3, 4))

        val first = conn.queryBlocks("SELECT id, name FROM t", dispatcher = Dispatchers.Unconfined)
            .take(1)
            .toList()

        assertEquals(1, first.size)
        assertTrue(result.closed, "QueryResult should be closed when collection is cancelled early")
    }

    // ---- queryBatched --------------------------------------------------------------------

    @Test
    fun batchedChunksExactlyWithPartialFinalAcrossBlockBoundaries() = runTest {
        // 5 rows over two blocks, batchSize 2 -> [2, 2, 1], chunks span block boundaries.
        val (conn, _) = connOf(idBlock(1, 2, 3), idBlock(4, 5))

        val batches = conn.queryBatched(
            "SELECT id, name FROM t",
            batchSize = 2,
            dispatcher = Dispatchers.Unconfined,
        ) { it.long("id") }.toList()

        assertEquals(listOf(listOf(1L, 2L), listOf(3L, 4L), listOf(5L)), batches)
    }

    @Test
    fun batchedEvenlyDivisibleHasNoTrailingEmptyChunk() = runTest {
        val (conn, _) = connOf(idBlock(1, 2, 3, 4))

        val batches = conn.queryBatched(
            "SELECT id, name FROM t",
            batchSize = 2,
            dispatcher = Dispatchers.Unconfined,
        ) { it.long("id") }.toList()

        assertEquals(listOf(listOf(1L, 2L), listOf(3L, 4L)), batches)
    }

    @Test
    fun batchedFewerRowsThanBatchYieldsSinglePartialChunk() = runTest {
        val (conn, _) = connOf(idBlock(1, 2))

        val batches = conn.queryBatched(
            "SELECT id, name FROM t",
            batchSize = 100,
            dispatcher = Dispatchers.Unconfined,
        ) { it.long("id") }.toList()

        assertEquals(listOf(listOf(1L, 2L)), batches)
    }

    @Test
    fun batchedEmptyResultEmitsNothing() = runTest {
        val (conn, _) = connOf(emptyBlock())

        val batches = conn.queryBatched(
            "SELECT id, name FROM t",
            batchSize = 10,
            dispatcher = Dispatchers.Unconfined,
        ) { it.long("id") }.toList()

        assertTrue(batches.isEmpty())
    }

    @Test
    fun batchedRejectsNonPositiveBatchSize() {
        val (conn, _) = connOf(idBlock(1))

        // Validation is eager — it throws on the queryBatched call, before any collection.
        assertFailsWith<IllegalArgumentException> {
            conn.queryBatched("SELECT id, name FROM t", batchSize = 0) { it.long("id") }
        }
    }

    @Test
    fun batchedClosesResultOnCompletion() = runTest {
        val (conn, result) = connOf(idBlock(1, 2, 3))

        conn.queryBatched(
            "SELECT id, name FROM t",
            batchSize = 2,
            dispatcher = Dispatchers.Unconfined,
        ) { it.long("id") }.collect { }

        assertTrue(result.closed, "QueryResult should be closed after the flow completes")
    }

    // ---- queryAsBatched (constructor-bound path) -----------------------------------------

    /** Kotlin data class → primary-constructor binding (the `constructorBoundBatchedFlow` path). */
    data class IdName(val id: Long, val name: String)

    @Test
    fun queryAsBatchedMapsAndChunksWithPartialFinal() = runTest {
        val (conn, _) = connOf(idBlock(1, 2, 3), idBlock(4, 5))

        val batches = conn.queryAsBatched<IdName>(
            "SELECT id, name FROM t",
            batchSize = 2,
            dispatcher = Dispatchers.Unconfined,
        ).toList()

        assertEquals(listOf(2, 2, 1), batches.map { it.size })
        assertEquals(listOf(1L, 2L, 3L, 4L, 5L), batches.flatten().map { it.id })
    }

    @Test
    fun queryAsBatchedEmptyResultEmitsNothing() = runTest {
        val (conn, _) = connOf(emptyBlock())

        val batches = conn.queryAsBatched<IdName>(
            "SELECT id, name FROM t",
            batchSize = 10,
            dispatcher = Dispatchers.Unconfined,
        ).toList()

        assertTrue(batches.isEmpty())
    }

    @Test
    fun queryAsBatchedRejectsNonPositiveBatchSize() {
        val (conn, _) = connOf(idBlock(1))

        assertFailsWith<IllegalArgumentException> {
            conn.queryAsBatched<IdName>("SELECT id, name FROM t", batchSize = 0)
        }
    }

    @Test
    fun queryAsBatchedClosesResultOnCompletion() = runTest {
        val (conn, result) = connOf(idBlock(1, 2, 3))

        conn.queryAsBatched<IdName>(
            "SELECT id, name FROM t",
            batchSize = 2,
            dispatcher = Dispatchers.Unconfined,
        ).collect { }

        assertTrue(result.closed, "QueryResult should be closed after the flow completes")
    }
}
