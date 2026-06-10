package io.github.danielbunting.clickhouse.samples.kotlin

import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.kotlin.bulkInsert
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.WebSocket
import java.time.Duration
import java.util.concurrent.CompletionStage
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext

/*
 * Shared plumbing for the Kotlin live-stream samples. The Java samples manage buffers with
 * locks, scheduled flusher threads, and hand-rolled WebSocket listeners; here the same shape
 * is three composable pieces:
 *
 *   source Flow (SSE or WebSocket)  ->  reconnecting { }  ->  ingestBatched(conn, ...)
 *
 * One coroutine owns the connection end-to-end (insert + live aggregate), so no lock is
 * needed — the single-coroutine rule replaces the Java versions' CONN_LOCK.
 */

/**
 * The `data:` payloads of a Server-Sent-Events stream as a cold [Flow]. The blocking HTTP
 * read runs on [Dispatchers.IO]; the response body is closed when collection stops.
 */
internal fun sseData(url: String, userAgent: String): Flow<String> = flow {
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
    val request = HttpRequest.newBuilder(URI.create(url))
        .header("Accept", "text/event-stream")
        .header("User-Agent", userAgent)
        .GET()
        .build()
    val response = http.send(request, HttpResponse.BodyHandlers.ofLines())
    if (response.statusCode() != 200) {
        response.body().close()
        error("stream returned HTTP ${response.statusCode()}")
    }
    response.body().use { lines ->
        for (line in lines.iterator()) {
            if (!line.startsWith("data:")) continue
            val json = line.removePrefix("data:").trim()
            if (json.isNotEmpty()) emit(json)
        }
    }
}.flowOn(Dispatchers.IO)

/**
 * Complete text messages of a WebSocket session as a cold [Flow]: fragments are reassembled,
 * [subscribeMessage] (if any) is sent on open, and the flow completes when the socket closes
 * (or fails with the socket's error). Cancelling the collector aborts the socket.
 */
internal fun webSocketText(uri: URI, subscribeMessage: String? = null): Flow<String> =
    callbackFlow {
        val fragments = StringBuilder()
        val listener = object : WebSocket.Listener {
            override fun onOpen(webSocket: WebSocket) = webSocket.request(1)

            override fun onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
                fragments.append(data)
                if (last) {
                    trySendBlocking(fragments.toString())
                    fragments.setLength(0)
                }
                webSocket.request(1)
                return null
            }

            override fun onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
                close()
                return null
            }

            override fun onError(webSocket: WebSocket, error: Throwable) {
                close(error)
            }
        }
        val ws = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(uri, listener).await()
        subscribeMessage?.let { ws.sendText(it, true) }
        awaitClose { ws.abort() }
    }.buffer(4096)

/**
 * Wraps a finite/fallible stream [source] in an endless reconnect loop: when the inner flow
 * completes or fails, wait [delayMs] and resubscribe. Cancellation passes straight through.
 */
internal fun reconnecting(label: String, delayMs: Long = 2_000, source: () -> Flow<String>): Flow<String> =
    flow {
        while (true) {
            try {
                emitAll(source())
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("[$label] stream error (${e.javaClass.simpleName}): ${e.message}")
            }
            System.err.println("[$label] reconnecting in ${delayMs}ms...")
            delay(delayMs)
        }
    }

/**
 * The shared ingest loop: collect [rows], bulk-inserting into [table] whenever the buffer
 * reaches [batchSize] rows or [flushIntervalMs] has passed, and calling [onReport] (total,
 * rows/sec) roughly every [reportIntervalMs]. The final partial batch is flushed in a
 * [NonCancellable] `finally`, so Ctrl-C (job cancellation) loses no buffered rows — the
 * coroutine equivalent of the Java samples' shutdown-hook flush.
 *
 * Everything here — inserts and [onReport]'s aggregate queries — runs in the calling
 * coroutine, satisfying the one-coroutine-per-connection rule without locks.
 */
internal suspend fun <T : Any> ClickHouseConnection.ingestBatched(
    table: String,
    type: Class<T>,
    rows: Flow<T>,
    batchSize: Int = 1_000,
    flushIntervalMs: Long = 2_000,
    reportIntervalMs: Long = 5_000,
    onReport: suspend (total: Long, rowsPerSec: Double) -> Unit,
) {
    val buffer = ArrayList<T>(batchSize)
    var total = 0L
    val startNanos = System.nanoTime()
    var lastFlush = System.currentTimeMillis()
    var lastReport = lastFlush
    try {
        rows.collect { row ->
            buffer += row
            val now = System.currentTimeMillis()
            if (buffer.size >= batchSize || now - lastFlush >= flushIntervalMs) {
                bulkInsert(table, type, buffer)
                total += buffer.size
                buffer.clear()
                lastFlush = now
            }
            if (now - lastReport >= reportIntervalMs) {
                val elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0
                onReport(total, if (elapsedSec > 0) total / elapsedSec else 0.0)
                lastReport = now
            }
        }
    } finally {
        withContext(NonCancellable) {
            if (buffer.isNotEmpty()) {
                bulkInsert(table, type, ArrayList(buffer))
                total += buffer.size
                buffer.clear()
            }
        }
        println("\n[$table] ingest finished; total rows: $total")
    }
}
