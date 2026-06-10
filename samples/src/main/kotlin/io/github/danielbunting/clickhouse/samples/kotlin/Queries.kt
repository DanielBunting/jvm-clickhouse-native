package io.github.danielbunting.clickhouse.samples.kotlin

import io.github.danielbunting.clickhouse.samples.ClickHouseEnv
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryAs
import io.github.danielbunting.clickhouse.kotlin.queryFlow
import io.github.danielbunting.clickhouse.kotlin.queryParametersOf
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking

/**
 * Tour of the read side through the Kotlin coroutine surface — the step-for-step counterpart
 * of the Java `Queries` sample (same seed data, same queries; only the API differs).
 *
 * Steps (numbered identically to the Java version):
 *  1. Seed a demo table from `system.numbers`.
 *  2. Scalars via suspend `scalar`.
 *  3. Row streaming: where Java iterates raw column-major blocks, Kotlin collects
 *     `queryFlow` — a cold `Flow<ResultRow>` view over the same blocks, with primitive
 *     accessors and cancellation-as-cleanup.
 *  4. Typed mapping with `queryAs` into an immutable data class (primary-constructor
 *     binding) — the Flow analog of Java's `query(sql, Class)` stream.
 *  5. Server-side query parameters via `queryParametersOf` — values travel separately from
 *     the SQL; the server casts them against `{name:Type}` placeholders.
 *  6. Async: where Java hands the blocking call to `queryAsync`'s future, a suspend call
 *     wrapped in `async { }` gives the same handoff with structured concurrency.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinQueries
 * ```
 */

/** Immutable row type for the typed-mapping step; names match the result columns. */
data class Measurement(val id: Long, val name: String, val score: Double)

fun main(): Unit = runBlocking {
    val table = "queries_demo_kotlin"
    println("=== clickhouse-native-client-kotlin Queries tour ===")

    ClickHouseEnv.config().connect().use { conn ->

        // --------------------------------------------------------------------
        // 1. Seed: a deterministic table built server-side from numbers()
        // --------------------------------------------------------------------
        println("\n[1] Creating and filling '$table' (1000 rows)...")
        conn.command(
            "CREATE OR REPLACE TABLE $table (" +
                "id    UInt64," +
                "name  String," +
                "score Float64" +
                ") ENGINE = MergeTree ORDER BY id",
        )
        conn.command(
            "INSERT INTO $table SELECT number, concat('m-', toString(number)), number / 8 " +
                "FROM numbers(1000)",
        )
        println("    Done.")

        // --------------------------------------------------------------------
        // 2. Scalars
        // --------------------------------------------------------------------
        println("\n[2] Scalars: count=${conn.scalar("SELECT count() FROM $table")}, " +
            "max(id)=${conn.scalar("SELECT max(id) FROM $table")}")

        // --------------------------------------------------------------------
        // 3. Row streaming: the Flow view over the column-major blocks
        // --------------------------------------------------------------------
        // Java iterates the blocks themselves; queryFlow flattens them to rows with
        // no-boxing accessors. The flow is lazy and cancellable — cancelling the
        // collector (e.g. take(n)) closes the result and releases the connection.
        println("\n[3] queryFlow row streaming (LIMIT 5) — the Flow view over the blocks:")
        conn.queryFlow("SELECT id, name, score FROM $table ORDER BY id LIMIT 5")
            .collect { row ->
                println("    %-4d %-8s %s".format(row.long("id"), row.string("name"), row.double("score")))
            }

        // --------------------------------------------------------------------
        // 4. Typed mapping: rows as immutable data classes
        // --------------------------------------------------------------------
        println("\n[4] Typed mapping to data class Measurement (top 3 by score):")
        conn.queryAs<Measurement>("SELECT id, name, score FROM $table ORDER BY score DESC LIMIT 3")
            .collect { println("    $it") }

        // --------------------------------------------------------------------
        // 5. Server-side query parameters
        // --------------------------------------------------------------------
        val matching = conn.scalar(
            "SELECT count() FROM $table " +
                "WHERE score >= {minScore:Float64} AND startsWith(name, {prefix:String})",
            queryParametersOf("minScore" to 120.0, "prefix" to "m-99"),
        )
        println("\n[5] Parameterized count (score >= 120, name like 'm-99%'): $matching")

        // --------------------------------------------------------------------
        // 6. Async: structured concurrency instead of a future
        // --------------------------------------------------------------------
        // The suspend call already runs off the caller's thread; async {} adds the
        // future-style handoff Java gets from queryAsync. NOTE: still one connection —
        // await the result before issuing the next operation on it.
        println("\n[6] async {} (the queryAsync analog):")
        val evenIds = async { conn.scalar("SELECT count() FROM $table WHERE id % 2 = 0") }
        println("    even ids: ${evenIds.await()}")
    }

    println("\n=== Kotlin Queries tour complete ===")
}
