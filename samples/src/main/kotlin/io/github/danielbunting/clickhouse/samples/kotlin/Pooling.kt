package io.github.danielbunting.clickhouse.samples.kotlin

import io.github.danielbunting.clickhouse.samples.ClickHouseEnv
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.lease
import io.github.danielbunting.clickhouse.kotlin.openPool
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking

/**
 * Concurrency with the connection pool, coroutine-style — the counterpart of the Java
 * [Pooling] sample.
 *
 * A single connection is one socket used by one coroutine at a time; the pool is the unit
 * of concurrency. `openPool` opens the connections off-thread (suspend); `lease { }` is the
 * suspend borrow → work → always-return scope (the return is `NonCancellable`, so a
 * cancelled block can never leak its permit).
 *
 * Steps:
 *  1. Open a pool of 4 connections.
 *  2. Launch 16 coroutines; each leases a connection, inserts 10k rows, and counts its own
 *     partition — 4 run at a time, the rest suspend (not block) waiting for a permit.
 *  3. Show pool health (`available()/size()`) after the burst.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinPooling
 * ```
 */
fun main(): Unit = runBlocking {
    val table = "pool_demo_kotlin"
    val poolSize = 4
    val tasks = 16
    println("=== clickhouse-native-client-kotlin Pooling tour ===")

    println("\n[1] Opening a pool of $poolSize connections...")
    ClickHouseEnv.config().openPool(size = poolSize).use { pool ->
        println("    size=${pool.size()}, available=${pool.available()}")

        pool.lease { conn ->
            conn.command(
                "CREATE OR REPLACE TABLE $table (" +
                    "task UInt32," +
                    "n    UInt64" +
                    ") ENGINE = MergeTree ORDER BY (task, n)",
            )
        }

        println("\n[2] Running $tasks concurrent tasks as coroutines over $poolSize connections...")
        val perTask = coroutineScope {
            (0 until tasks).map { task ->
                async(Dispatchers.Default) {
                    pool.lease { conn ->
                        conn.command("INSERT INTO $table SELECT $task, number FROM numbers(10000)")
                        conn.scalar("SELECT count() FROM $table WHERE task = $task")
                    }
                }
            }.awaitAll()
        }
        println("    All tasks done; per-task counts sum to ${perTask.sum()}")

        val rows = pool.lease { conn -> conn.scalar("SELECT count() FROM $table") }
        println("\n[3] After the burst: available=${pool.available()}/${pool.size()}, " +
            "total rows=$rows (expected ${tasks * 10_000L})")
    }

    println("\n=== Kotlin Pooling tour complete ===")
}
