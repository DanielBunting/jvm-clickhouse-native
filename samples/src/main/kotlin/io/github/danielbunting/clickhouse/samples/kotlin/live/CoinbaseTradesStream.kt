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
 * Coroutine port of [CoinbaseTradesStream]: subscribes to the public Coinbase Exchange
 * WebSocket feed's `matches` channel and ingests trades into `crypto_trades_kotlin`.
 *
 * Where the Java sample needs a buffer monitor, a `CONN_LOCK`, and a scheduled flusher
 * thread, the coroutine pipeline is single-threaded by construction:
 * `reconnecting { webSocketText(...) }` → [mapNotNull] parse → `ingestBatched` — inserts and
 * the live aggregate run in one coroutine, so no lock exists to forget.
 *
 * Run with:
 * ```
 *   ./gradlew :samples:runKotlinCoinbase    (Ctrl-C to stop)
 * ```
 */

private val COINBASE_URI = URI.create("wss://ws-feed.exchange.coinbase.com")

private const val COINBASE_SUBSCRIBE =
    """{"type":"subscribe","product_ids":["BTC-USD","ETH-USD","SOL-USD"],"channels":["matches"]}"""

private val COINBASE_JSON = ObjectMapper()

/** One trade ("match") event; component names match the `crypto_trades_kotlin` columns. */
data class Trade(
    val time: Instant,
    val productId: String,
    val side: String,
    val price: Double,
    val size: Double,
    val tradeId: Long,
)

fun main(): Unit = runBlocking {
    val table = "crypto_trades_kotlin"
    val conn = ClickHouseEnv.config().connect()
    conn.command(
        "CREATE TABLE IF NOT EXISTS $table (" +
            "time DateTime64(3,'UTC'), productId String, side String, " +
            "price Float64, size Float64, tradeId UInt64" +
            ") ENGINE=MergeTree ORDER BY (productId, time)",
    )

    println("Streaming Coinbase trades into '$table' (Ctrl-C to stop)...")

    val pipeline = launch {
        try {
            conn.ingestBatched(
                table = table,
                type = Trade::class.java,
                rows = reconnecting("coinbase") { webSocketText(COINBASE_URI, COINBASE_SUBSCRIBE) }
                    .mapNotNull(::parseTrade),
            ) { total, rate ->
                println("ingested=$total rows, %.1f rows/sec".format(rate))
                conn.queryFlow(
                    "SELECT productId, count() c, round(avg(price),2) avg_price " +
                        "FROM $table GROUP BY productId ORDER BY c DESC",
                ).collect {
                    println("  %-10s count=%d avg_price=%s".format(
                        it.string("productId"), it.long("c"), it["avg_price"]))
                }
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

/** Parses a feed message into a [Trade], or null when it is not a trade / is malformed. */
private fun parseTrade(message: String): Trade? {
    return try {
        val node = COINBASE_JSON.readTree(message)
        val type = node.path("type").asText("")
        if (type != "match" && type != "last_match") return null
        Trade(
            time = node.path("time").asText(null)?.let(Instant::parse) ?: Instant.now(),
            productId = node.path("product_id").asText(""),
            side = node.path("side").asText(""),
            price = node.path("price").asText("").toDoubleOrNull() ?: 0.0,
            size = node.path("size").asText("").toDoubleOrNull() ?: 0.0,
            tradeId = node.path("trade_id").asLong(0),
        )
    } catch (e: Exception) {
        null // malformed/unexpected message: skip it rather than killing the stream
    }
}
