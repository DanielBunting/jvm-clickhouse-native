package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.bulkInsert
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.query
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Exercises the suspend [bulkInsert] wrappers end-to-end: rows go in through the core's
 * column-buffered native insert path and are read back with [query]/[scalar] for round-trip
 * equality. Covers the [Iterable] and [kotlinx.coroutines.flow.Flow] forms and a batch large
 * enough to force intermediate block flushes.
 */
@Tag("integration")
class BulkInsertIT : ClickHouseIntegrationTest() {

    /** Field names and types match the target columns exactly (Int64/String/Float64). */
    data class Event(var id: Long = 0, var name: String = "", var score: Double = 0.0)

    /** No no-arg constructor: legal for the WRITE path (the binder reads fields only). */
    data class ImmutableEvent(val id: Long, val name: String, val score: Double)

    private suspend fun io.github.danielbunting.clickhouse.ClickHouseConnection.createEventTable(table: String) {
        command(
            "CREATE TABLE IF NOT EXISTS $table (id Int64, name String, score Float64) " +
                "ENGINE = MergeTree() ORDER BY id",
        )
    }

    @Test
    fun immutableRowsInsertWithoutANoArgConstructor() = runBlocking {
        val table = uniqueTable("k_bulk_immutable")
        config().connect().use { conn ->
            conn.createEventTable(table)
            try {
                conn.bulkInsert(table, listOf(
                    ImmutableEvent(1, "val-only", 1.0),
                    ImmutableEvent(2, "no-defaults", 2.0),
                ))
                assertEquals(2L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun iterableRowsRoundTrip() = runBlocking {
        val table = uniqueTable("k_bulk_iterable")
        val rows = listOf(
            Event(1, "alpha", 1.5),
            Event(2, "beta", -2.25),
            Event(3, "gamma", 0.0),
        )
        config().connect().use { conn ->
            conn.createEventTable(table)
            try {
                conn.bulkInsert(table, rows)

                val readBack = conn.query("SELECT id, name, score FROM $table ORDER BY id") { row ->
                    Event(row.long("id"), row.string("name")!!, row.double("score"))
                }.toList()
                assertEquals(rows, readBack)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun flowOfRowsRoundTrips() = runBlocking {
        val table = uniqueTable("k_bulk_flow")
        val rows = (1L..50L).map { Event(it, "row-$it", it / 2.0) }
        config().connect().use { conn ->
            conn.createEventTable(table)
            try {
                conn.bulkInsert(table, rows.asFlow())

                assertEquals(50L, conn.scalar("SELECT count() FROM $table"))
                val readBack = conn.query("SELECT id, name, score FROM $table ORDER BY id") { row ->
                    Event(row.long("id"), row.string("name")!!, row.double("score"))
                }.toList()
                assertEquals(rows, readBack)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun largeBatchCrossesBlockFlushes() = runBlocking {
        val table = uniqueTable("k_bulk_large")
        val n = 100_000L
        config().connect().use { conn ->
            conn.createEventTable(table)
            try {
                conn.bulkInsert(table, (1L..n).map { Event(it, "n$it", 1.0) })

                assertEquals(n, conn.scalar("SELECT count() FROM $table"))
                assertEquals(n * (n + 1) / 2, conn.scalar("SELECT sum(id) FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun connectionIsReusableAfterBulkInsert() = runBlocking {
        val table = uniqueTable("k_bulk_reuse")
        config().connect().use { conn ->
            conn.createEventTable(table)
            try {
                conn.bulkInsert(table, listOf(Event(1, "a", 1.0)))
                conn.bulkInsert(table, listOf(Event(2, "b", 2.0)))
                assertEquals(2L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }
}
