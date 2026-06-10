package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.query
import io.github.danielbunting.clickhouse.kotlin.queryFlow
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Exercises the Flow streaming surface against a real server: [query] with an explicit lambda
 * mapper, [queryFlow] of [ResultRow][io.github.danielbunting.clickhouse.kotlin.ResultRow], standard
 * Flow operators over live data, multi-block streaming, and early-termination resource safety.
 */
@Tag("integration")
class QueryFlowIT : ClickHouseIntegrationTest() {

    private data class Event(val id: Long, val name: String, val score: Double)

    @Test
    fun mapsRowsToObjectsInOrder() = runBlocking {
        val table = uniqueTable("k_flow_map")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, name String, score Float64) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            conn.command("INSERT INTO $table VALUES (1,'alpha',9.5),(2,'beta',7.25),(3,'gamma',5.0)")
            try {
                val events = conn.query("SELECT id, name, score FROM $table ORDER BY id") { row ->
                    Event(row.long("id"), row.string("name")!!, row.double("score"))
                }.toList()

                assertEquals(
                    listOf(
                        Event(1, "alpha", 9.5),
                        Event(2, "beta", 7.25),
                        Event(3, "gamma", 5.0),
                    ),
                    events,
                )
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun resultRowTypedAndPositionalAccessors() = runBlocking {
        val table = uniqueTable("k_flow_row")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, name String, score Float64) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            conn.command("INSERT INTO $table VALUES (7,'solo',1.5)")
            try {
                val rows = conn.queryFlow("SELECT id, name, score FROM $table").toList()
                assertEquals(1, rows.size)
                val r = rows[0]
                assertEquals(7L, r.long("id"))
                assertEquals(7, r.int("id"))
                assertEquals(7L, r.long(0))          // positional
                assertEquals("solo", r.string("name"))
                assertEquals(1.5, r.double("score"))
                assertEquals(3, r.columnCount)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun streamsAcrossMultipleBlocks() = runBlocking {
        val table = uniqueTable("k_flow_multiblock")
        val rowCount = 50_000L
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers($rowCount)")
            try {
                // A 50k-row result is delivered as several native blocks; the flow must stitch them.
                val streamed = conn.queryFlow("SELECT id FROM $table").count().toLong()
                assertEquals(rowCount, streamed)

                // Sum via the mapper path independently corroborates value fidelity across blocks.
                val sum = conn.query("SELECT id FROM $table") { it.long("id") }
                    .toList()
                    .sum()
                assertEquals((rowCount - 1) * rowCount / 2, sum)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun supportsStandardFlowOperators() = runBlocking {
        val table = uniqueTable("k_flow_ops")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers(1000)")
            try {
                val firstFiveEvens = conn.queryFlow("SELECT id FROM $table ORDER BY id")
                    .map { it.long("id") }
                    .filter { it % 2 == 0L }
                    .take(5)
                    .toList()
                assertEquals(listOf(0L, 2L, 4L, 6L, 8L), firstFiveEvens)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun emptyResultYieldsEmptyFlow() = runBlocking {
        val table = uniqueTable("k_flow_empty")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            try {
                val rows = conn.queryFlow("SELECT id FROM $table").toList()
                assertTrue(rows.isEmpty(), "Expected no rows from an empty table")
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun earlyTerminationReleasesConnectionForReuse() = runBlocking {
        val table = uniqueTable("k_flow_cancel")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id Int64) ENGINE = MergeTree() ORDER BY id")
            conn.command("INSERT INTO $table (id) SELECT number FROM numbers(50000)")
            try {
                // take(1) cancels the flow after one row, which must close the QueryResult and
                // release the connection guard — proven by the connection still working afterwards.
                val firstId = conn.queryFlow("SELECT id FROM $table ORDER BY id")
                    .map { it.long("id") }
                    .take(1)
                    .toList()
                assertEquals(listOf(0L), firstId)

                // If the guard had leaked, this scalar would throw ConcurrentConnectionUseException.
                assertEquals(50_000L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }
}
