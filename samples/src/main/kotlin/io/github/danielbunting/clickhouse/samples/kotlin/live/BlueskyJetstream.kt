package io.github.danielbunting.clickhouse.samples.kotlin.live

import io.github.danielbunting.clickhouse.samples.ClickHouseEnv
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.danielbunting.clickhouse.samples.kotlin.ingestBatched
import io.github.danielbunting.clickhouse.samples.kotlin.reconnecting
import io.github.danielbunting.clickhouse.samples.kotlin.webSocketText
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryFlow
import java.net.URI
import java.time.Instant
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

/**
 * Coroutine port of [BlueskyJetstream]: ingests the Bluesky Jetstream firehose (AT Protocol
 * events as compact JSON, filtered to `app.bsky.feed.post`) into `bluesky_events_kotlin`.
 *
 * Same shared pipeline as the other Kotlin stream samples:
 * `reconnecting { webSocketText(...) }` → [mapNotNull] parse → `ingestBatched`, with the
 * `NonCancellable` final flush covering Ctrl-C.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinBluesky    (Ctrl-C to stop)
 * ```
 */

private val JETSTREAM_URI = URI.create(
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
)

private val BLUESKY_JSON = ObjectMapper()

/** One Jetstream event; component names match the `bluesky_events_kotlin` columns. */
data class BlueskyEvent(
    val time: Instant,
    val did: String,
    val kind: String,
    val collection: String,
    val operation: String,
    val text: String,
)

fun main(): Unit = runBlocking {
    val table = "bluesky_events_kotlin"
    val conn = ClickHouseEnv.config().connect()
    conn.command(
        "CREATE TABLE IF NOT EXISTS $table (" +
            "time DateTime64(6,'UTC'), did String, kind String, " +
            "collection String, operation String, text String" +
            ") ENGINE=MergeTree ORDER BY time",
    )

    println("Streaming Bluesky Jetstream into '$table' (Ctrl-C to stop)...")

    val pipeline = launch {
        try {
            conn.ingestBatched(
                table = table,
                type = BlueskyEvent::class.java,
                rows = reconnecting("bluesky") { webSocketText(JETSTREAM_URI) }
                    .mapNotNull(::parseBlueskyEvent),
            ) { total, rate ->
                println("[bluesky] total=$total rows  %.0f rows/sec".format(rate))
                conn.queryFlow(
                    "SELECT collection, count() c FROM $table " +
                        "GROUP BY collection ORDER BY c DESC LIMIT 5",
                ).collect { println("    %-32s %d".format(it.string("collection"), it.long("c"))) }
            }
        } finally {
            withContext(NonCancellable + Dispatchers.IO) { conn.close() }
        }
    }

    Runtime.getRuntime().addShutdownHook(Thread {
        println("\nShutting down: flushing remaining rows...")
        pipeline.cancel()
        runBlocking { pipeline.join() }
    })
    pipeline.join()
}

/** Parses one Jetstream message; non-commit events keep empty collection/operation/text. */
private fun parseBlueskyEvent(message: String): BlueskyEvent? {
    return try {
        val root = BLUESKY_JSON.readTree(message)
        val timeUs = root.path("time_us").asLong(0)
        val commit = root.path("commit")
        BlueskyEvent(
            time = if (timeUs > 0) {
                Instant.ofEpochSecond(timeUs / 1_000_000, (timeUs % 1_000_000) * 1_000)
            } else {
                Instant.now()
            },
            did = root.path("did").asText(""),
            kind = root.path("kind").asText(""),
            collection = commit.path("collection").asText(""),
            operation = commit.path("operation").asText(""),
            text = commit.path("record").path("text").asText(""),
        )
    } catch (e: Exception) {
        System.err.println("Skipping unparseable message: ${e.message}")
        null
    }
}
