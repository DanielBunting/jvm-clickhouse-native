package io.github.danielbunting.clickhouse.samples.kotlin

import io.github.danielbunting.clickhouse.samples.ClickHouseEnv
import io.github.danielbunting.clickhouse.kotlin.bulkInsert
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.scalar
import java.time.Instant
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking

/**
 * Tour of the write side through the Kotlin coroutine surface — the counterpart of the Java
 * [BulkInserts] sample.
 *
 * The suspend `bulkInsert` extensions wrap the core `BulkInserter` lifecycle
 * (`init → add → complete`, leak-proof, off the caller's thread):
 *  1. A small `Iterable` batch — one call.
 *  2. A timed 250k-row batch crossing multiple internal block flushes.
 *  3. The `Flow` form: rows produced asynchronously, inserted as they arrive — the
 *     coroutine equivalent of the Java sample's producer-style `add()` loop.
 *  4. Verification by aggregates.
 *
 * Failure semantics: if the row source throws (or the coroutine is cancelled) mid-insert,
 * the inserter closes WITHOUT `complete()` and the connection is poisoned rather than left
 * desynced — a pool would discard it instead of recycling it.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinBulkInserts
 * ```
 */

/** Immutable row; component names/types match the table (UInt64/DateTime/String/Float64). */
data class Reading(val id: Long, val ts: Instant, val label: String, val value: Double)

fun main(): Unit = runBlocking {
    val table = "bulk_demo_kotlin"
    println("=== clickhouse-native-client-kotlin BulkInserts tour ===")

    ClickHouseEnv.config().connect().use { conn ->

        println("\n[1] Creating table '$table'...")
        conn.command(
            "CREATE OR REPLACE TABLE $table (" +
                "id    UInt64," +
                "ts    DateTime," +
                "label String," +
                "value Float64" +
                ") ENGINE = MergeTree ORDER BY id",
        )
        println("    Done.")

        val now = Instant.now()

        println("\n[2] Canonical lifecycle (init -> addRange -> complete) in one bulkInsert call (3 rows)...")
        conn.bulkInsert(table, listOf(
            Reading(1, now, "boot", 0.1),
            Reading(2, now, "warm", 0.5),
            Reading(3, now, "steady", 0.9),
        ))
        println("    Done.")

        val n = 250_000
        println("\n[3] Bulk-inserting $n rows (crosses multiple block flushes)...")
        val batch = (0 until n).map { Reading(1_000L + it, now, "batch", it / 1000.0) }
        val started = System.nanoTime()
        conn.bulkInsert(table, batch)
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        println("    %,d rows in %,d ms (%,.0f rows/s)".format(n, elapsedMs, n * 1000.0 / maxOf(elapsedMs, 1)))

        // The Flow form: rows are produced inside a coroutine and inserted as they
        // arrive — no intermediate list, natural for async producers. (Built via
        // asFlow().map rather than a bare flow {} so the reified row type is inferred
        // from the flow, not defaulted by builder inference.)
        println("\n[4] Producer-style Flow (500 rows)...")
        conn.bulkInsert(table, (0 until 500).asFlow()
            .map { Reading(900_000L + it, now, "producer", it * 1.0) })
        println("    Done.")

        val count = conn.scalar("SELECT count() FROM $table")
        val labels = conn.scalar("SELECT uniqExact(label) FROM $table")
        println("\n[5] Verification: count=$count (expected ${3 + n + 500}), distinct labels=$labels (expected 5)")
    }

    println("\n=== Kotlin BulkInserts tour complete ===")
}
