package io.github.danielbunting.clickhouse.samples.kotlin.live

import io.github.danielbunting.clickhouse.samples.ClickHouseEnv
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.danielbunting.clickhouse.samples.kotlin.ingestBatched
import io.github.danielbunting.clickhouse.samples.kotlin.reconnecting
import io.github.danielbunting.clickhouse.samples.kotlin.sseData
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryFlow
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

/**
 * Coroutine port of [WikimediaEditsStream]: ingests the Wikimedia `recentchange` SSE firehose
 * (every edit across all Wikimedia projects, unauthenticated) into `wiki_edits_kotlin`.
 *
 * The pipeline is the shared streaming shape from [Streaming.kt]:
 * `reconnecting { sseData(...) }` → [mapNotNull] parse → `ingestBatched` (size/time-batched
 * bulk inserts + periodic live aggregate, all in one coroutine — no locks). Ctrl-C cancels
 * the pipeline; the `NonCancellable` final flush writes any buffered rows before closing.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinWikimedia    (Ctrl-C to stop)
 * ```
 */

private const val WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

/** Wikimedia's User-Agent policy rejects generic agents (incl. the JDK default) with 403. */
private const val WIKI_USER_AGENT =
    "clickhouse-native-client-samples/0.1 (https://github.com/DanielBunting/CH.Native; ClickHouse Kotlin client demo)"

private val WIKI_JSON = ObjectMapper()

/** One `recentchange` event; component names match the `wiki_edits_kotlin` columns. */
data class WikiEdit(
    val dt: Instant,
    val wiki: String,
    val domain: String,
    val type: String,
    val title: String,
    val user: String,
    val bot: Int,
    val minor: Int,
    val comment: String,
    val lengthOld: Long,
    val lengthNew: Long,
    val revOld: Long,
    val revNew: Long,
)

fun main(): Unit = runBlocking {
    val table = "wiki_edits_kotlin"
    val conn = ClickHouseEnv.config().connect()
    conn.command(
        "CREATE TABLE IF NOT EXISTS $table (" +
            "dt DateTime('UTC'), wiki String, domain String, type String, title String, " +
            "user String, bot UInt8, minor UInt8, comment String, " +
            "lengthOld UInt32, lengthNew UInt32, revOld UInt64, revNew UInt64" +
            ") ENGINE=MergeTree ORDER BY (wiki, dt)",
    )

    println("Streaming $WIKI_STREAM_URL into '$table' (Ctrl-C to stop)...")

    val pipeline = launch {
        try {
            conn.ingestBatched(
                table = table,
                type = WikiEdit::class.java,
                rows = reconnecting("wikimedia") { sseData(WIKI_STREAM_URL, WIKI_USER_AGENT) }
                    .mapNotNull(::parseWikiEdit),
            ) { total, rate ->
                println("\n[ingested=$total rows, %.1f rows/sec]".format(rate))
                println("Top wikis by edit count:")
                println("  %-24s %10s".format("wiki", "count"))
                conn.queryFlow("SELECT wiki, count() AS c FROM $table GROUP BY wiki ORDER BY c DESC LIMIT 10")
                    .collect { println("  %-24s %10d".format(it.string("wiki"), it.long("c"))) }
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

/** Parses one `recentchange` JSON payload, defaulting the fields non-edit events omit. */
private fun parseWikiEdit(json: String): WikiEdit? {
    val node = try {
        WIKI_JSON.readTree(json)
    } catch (e: Exception) {
        return null
    }
    return WikiEdit(
        dt = wikiTimestamp(node),
        wiki = node.path("wiki").asText(""),
        domain = node.path("server_name").asText(""),
        type = node.path("type").asText(""),
        title = node.path("title").asText(""),
        user = node.path("user").asText(""),
        bot = if (node.path("bot").asBoolean(false)) 1 else 0,
        minor = if (node.path("minor").asBoolean(false)) 1 else 0,
        comment = node.path("comment").asText(""),
        lengthOld = node.path("length").path("old").asLong(0),
        lengthNew = node.path("length").path("new").asLong(0),
        revOld = node.path("revision").path("old").asLong(0),
        revNew = node.path("revision").path("new").asLong(0),
    )
}

/** Prefers ISO-8601 `meta.dt`, falls back to epoch-seconds `timestamp`, then to now. */
private fun wikiTimestamp(node: JsonNode): Instant {
    val metaDt = node.path("meta").path("dt")
    if (metaDt.isTextual) {
        val s = metaDt.asText().trim()
        if (s.isNotEmpty()) {
            try {
                return Instant.parse(s)
            } catch (ignored: DateTimeParseException) {
                // fall through to epoch seconds
            }
        }
    }
    val epochSeconds = node.path("timestamp").asLong(0)
    return if (epochSeconds > 0) Instant.ofEpochSecond(epochSeconds) else Instant.now()
}
