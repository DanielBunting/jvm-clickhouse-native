package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryAsBatched
import io.github.danielbunting.clickhouse.kotlin.queryBatched
import io.github.danielbunting.clickhouse.kotlin.queryBlocks
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Exercises the batched ([queryBatched]) and block-granular ([queryBlocks]) flows against a real
 * server, where a 1-row-count result spans several native blocks. The key property under test is
 * that [queryBatched] re-chunks to an exact client-controlled size **across** server-block
 * boundaries (a batch larger than a server block is stitched from several; a batch smaller than one
 * splits it), independent of how the server chose to frame its blocks.
 */
@Tag("integration")
class BatchedQueryIT : ClickHouseIntegrationTest() {

    /** Kotlin data class → primary-constructor binding path of queryAsBatched. */
    data class IdRow(val id: Long)

    /** `@JvmRecord` → the core record mapper (stream) path of queryAsBatched. */
    @JvmRecord
    data class IdRec(val id: Long)

    // ---- queryBatched -------------------------------------------------------------------

    @Test
    fun batchesExactlyWithPartialFinalAcrossServerBlocks() = runBlocking {
        val table = uniqueTable("k_batched_exact")
        val rowCount = 150_000L
        val batchSize = 100_000
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                val batches = conn.queryBatched("SELECT id FROM $table ORDER BY id", batchSize) { it.long("id") }
                    .toList()

                // 150k / 100k -> [100000, 50000]; the 100k batch must be stitched from >1 server block.
                assertEquals(listOf(100_000, 50_000), batches.map { it.size })
                // Value + order fidelity across batch and block boundaries.
                assertEquals((0 until rowCount).toList(), batches.flatten())
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun smallBatchesSplitServerBlocks() = runBlocking {
        val table = uniqueTable("k_batched_small")
        val rowCount = 150_000L
        val batchSize = 40_000
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                val sizes = conn.queryBatched("SELECT id FROM $table ORDER BY id", batchSize) { it.long("id") }
                    .toList()
                    .map { it.size }

                // 150k / 40k -> [40000, 40000, 40000, 30000], each batch carved from server blocks.
                assertEquals(listOf(40_000, 40_000, 40_000, 30_000), sizes)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun batchedEmptyResultYieldsNoBatches() = runBlocking {
        val table = uniqueTable("k_batched_empty")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            try {
                val batches = conn.queryBatched("SELECT id FROM $table", 1000) { it.long("id") }.toList()
                assertTrue(batches.isEmpty(), "Expected no batches from an empty table")
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun batchedEarlyTerminationReleasesConnectionForReuse() = runBlocking {
        val table = uniqueTable("k_batched_cancel")
        val rowCount = 150_000L
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                // take(1) cancels after the first batch; the flow's finally must close the result and
                // release the connection guard — proven by the connection still working afterwards.
                val first = conn.queryBatched("SELECT id FROM $table ORDER BY id", 10_000) { it.long("id") }
                    .take(1)
                    .toList()
                assertEquals(1, first.size)
                assertEquals(10_000, first[0].size)

                // If the guard had leaked, this scalar would throw ConcurrentConnectionUseException.
                assertEquals(rowCount, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    // ---- queryBlocks --------------------------------------------------------------------

    @Test
    fun blocksCoverAllRowsAcrossMultipleBlocks() = runBlocking {
        val table = uniqueTable("k_blocks_multi")
        val rowCount = 150_000L
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                val blocks = conn.queryBlocks("SELECT id FROM $table ORDER BY id").toList()

                // >65k rows is delivered as more than one native block; queryBlocks emits each.
                assertTrue(blocks.size > 1, "expected multiple server blocks, got ${blocks.size}")
                assertEquals(rowCount, blocks.sumOf { it.rowCount().toLong() })

                // Reassemble values from the column arrays to prove fidelity + order across blocks.
                val ids = buildList {
                    for (b in blocks) {
                        val col = b.column(0)
                        for (r in 0 until b.rowCount()) add(col.longAt(r))
                    }
                }
                assertEquals((0 until rowCount).toList(), ids)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun blocksEmptyResultYieldsNoBlocks() = runBlocking {
        val table = uniqueTable("k_blocks_empty")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            try {
                val blocks = conn.queryBlocks("SELECT id FROM $table").toList()
                assertTrue(blocks.isEmpty(), "Expected no blocks from an empty table")
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    // ---- queryAsBatched -----------------------------------------------------------------

    @Test
    fun queryAsBatchedConstructorBoundAcrossBlocks() = runBlocking {
        val table = uniqueTable("k_qab_ctor")
        val rowCount = 150_000L
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                val batches = conn.queryAsBatched<IdRow>("SELECT id FROM $table ORDER BY id", batchSize = 100_000)
                    .toList()

                assertEquals(listOf(100_000, 50_000), batches.map { it.size })
                assertEquals((0 until rowCount).toList(), batches.flatten().map { it.id })
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun queryAsBatchedRecordPathAcrossBlocks() = runBlocking {
        val table = uniqueTable("k_qab_rec")
        val rowCount = 150_000L
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                // @JvmRecord routes through the core record mapper (streamBatchedFlow), not the
                // primary-constructor binder — same exact-chunk + value guarantees must hold.
                val batches = conn.queryAsBatched<IdRec>("SELECT id FROM $table ORDER BY id", batchSize = 40_000)
                    .toList()

                assertEquals(listOf(40_000, 40_000, 40_000, 30_000), batches.map { it.size })
                assertEquals((0 until rowCount).toList(), batches.flatten().map { it.id })
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }
}
