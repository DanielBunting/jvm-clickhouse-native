package io.github.danielbunting.clickhouse.samples.kotlin

import io.github.danielbunting.clickhouse.samples.ClickHouseEnv
import io.github.danielbunting.clickhouse.kotlin.bulkInsert
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryAs
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.runBlocking

/**
 * Minimal end-to-end demo of the Kotlin coroutine surface — the counterpart of the Java
 * [QuickStart] sample.
 *
 * Steps:
 *  1. Connect (suspend) using the shared [ClickHouseEnv] settings.
 *  2. Create (or reuse) the `events_kotlin` table via `command`.
 *  3. Bulk-insert five rows of an **immutable** data class.
 *  4. Stream the rows back with `queryAs` (primary-constructor binding) and print them.
 *  5. Print `count()` via `scalar`.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinQuickStart
 * ```
 * Override the target server with `CH_HOST`, `CH_PORT`, `CH_DB`, `CH_USER`, `CH_PASSWORD`.
 */

/** Component names match the table's columns; `val`s only — no no-arg constructor needed. */
data class Event(val id: Long, val name: String, val score: Double)

fun main(): Unit = runBlocking {
    val table = "events_kotlin"
    println("=== clickhouse-native-client-kotlin QuickStart ===")
    val config = ClickHouseEnv.config()
    println("Connecting to ${config.host()}:${config.port()} (db=${config.database()})")

    config.connect().use { conn ->

        println("\n[1] Creating table '$table' if not exists...")
        conn.command(
            "CREATE TABLE IF NOT EXISTS $table (" +
                "id    UInt64," +
                "name  String," +
                "score Float64" +
                ") ENGINE = MergeTree ORDER BY id",
        )
        println("    Done.")

        val rows = listOf(
            Event(1, "alpha", 9.5),
            Event(2, "beta", 7.3),
            Event(3, "gamma", 8.1),
            Event(4, "delta", 6.0),
            Event(5, "epsilon", 10.0),
        )
        println("\n[2] Bulk-inserting ${rows.size} rows...")
        conn.bulkInsert(table, rows)
        println("    Inserted ${rows.size} rows.")

        println("\n[3] Querying rows (queryAs<Event>: SELECT id, name, score FROM $table ORDER BY id):")
        println("    %-6s %-12s %s".format("id", "name", "score"))
        println("    " + "-".repeat(30))
        conn.queryAs<Event>("SELECT id, name, score FROM $table ORDER BY id")
            .collect { println("    %-6d %-12s %s".format(it.id, it.name, it.score)) }

        println("\n[4] Total rows in '$table': ${conn.scalar("SELECT count() FROM $table")}")
    }

    println("\n=== Kotlin QuickStart complete ===")
}
