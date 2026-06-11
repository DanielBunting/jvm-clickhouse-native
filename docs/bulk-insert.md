# Bulk Insert

`BulkInserter` is the high-throughput write path: rows are accumulated into per-column primitive buffers and shipped as native columnar blocks, bypassing SQL string building entirely. It is this client's single biggest performance margin over row-oriented drivers — see the [benchmarks](../benchmarks/).

- [Lifecycle](#lifecycle)
- [Row mapping](#row-mapping)
- [Batch size](#batch-size)
- [Kotlin](#kotlin)
- [Failure semantics](#failure-semantics)
- [Tips](#tips)

## Lifecycle

#### Java

```java
public record Event(long id, String name, double score) {}

try (BulkInserter<Event> inserter = conn.createBulkInserter("events", Event.class)) {
    inserter.init();                        // 1. send INSERT, read the target schema, allocate buffers
    inserter.add(new Event(1, "alpha", 9.5)); // 2. buffer rows one at a time...
    inserter.addRange(moreEvents);            //    ...or in bulk; full batches flush automatically
    inserter.complete();                    // 3. flush the tail block and send the terminator
}                                           // 4. close() releases the connection guard
```

#### Kotlin

```kotlin
data class Event(val id: Long, val name: String, val score: Double)

conn.bulkInsert("events", events)   // suspend; full lifecycle handled for you
```

- **`init()`** sends the `INSERT INTO <table> VALUES` query and reads the server's sample block to learn the exact column types. Buffers are sized to the configured batch size.
- **`add(row)` / `addRange(rows)`** write each field into its column buffer; when a batch fills, the block is compressed and flushed.
- **`complete()`** flushes any buffered rows and sends the terminating empty block — only then is the insert committed from the protocol's point of view.
- **`close()`** is idempotent and safe in try-with-resources.

The inserter holds the connection exclusively from `init()` until `complete()`/`close()` — run other queries on a different connection (or use a [pool](connection-pooling.md)).

## Row mapping

`createBulkInserter(table, Class<T>)` maps the row type to columns by name:

- **Java records** — column names come from record components in declaration order.
- **POJOs** — names come from declared instance fields (subclass fields before superclass fields).
- **Kotlin data classes** — primary-constructor properties (they compile to fields, so the POJO path applies; the Kotlin module's `bulkInsert` resolves them directly).

Name matching is exact first, then case-insensitive. The accepted field types per ClickHouse column are listed in [data-types.md — Insert binding](data-types.md#insert-binding).

## Batch size

`insertBatchSize` (default **65 536 rows**) controls how many rows are buffered per block:

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .insertBatchSize(131_072)
    .build();
```

Bigger batches mean fewer flushes and better compression at the cost of memory held per in-flight block. The default is a good starting point; tune with your real row width.

## Kotlin

The `-kotlin` module wraps the lifecycle in suspend functions with `List` and `Flow` sources:

```kotlin
// from a collection
conn.bulkInsert("events", eventsList)

// from a Flow — backpressure-friendly streaming ingestion
conn.bulkInsert("events", eventsFlow)

// explicit class form (non-reified contexts)
conn.bulkInsert("events", Event::class.java, eventsList)
```

All overloads run blocking I/O on `Dispatchers.IO` (overridable via the `dispatcher` parameter). The [live-stream samples](../samples/) (`BlueskyJetstream.kt`, `WikimediaEditsStream.kt`, `CoinbaseTradesStream.kt`) feed real-time event streams through this path.

## Failure semantics

An insert that was `init()`-ed but never `complete()`-d leaves the server mid-stream — the wire is desynced and the connection cannot be reused. `close()` detects this and **poisons the connection** so a pool will discard it instead of recycling it (`isPoisoned()` returns `true`). The insert itself is not committed; rows from already-flushed blocks may have been written, so treat an aborted bulk insert as needing retry/dedup (e.g. `ReplacingMergeTree` or idempotent keys).

## Tips

- Reuse one `BulkInserter` for many `add` calls rather than creating one per row batch — `init()` costs a round-trip.
- Producer-style ingestion (`add` per event, `complete()` on shutdown) is the intended pattern for long-running pipelines.
- `LZ4` compression (the default) is almost always a net win for inserts; see the [benchmark numbers](../README.md#performance) for `NONE`/`LZ4`/`ZSTD`.
- For concurrent writers, give each its own pooled connection — one inserter per connection.

## See also

- [Quickstart](quickstart.md)
- [Data types — insert binding](data-types.md#insert-binding)
- [Connection pooling](connection-pooling.md)
- Samples: [`BulkInserts.java`](../samples/src/main/java/io/github/danielbunting/clickhouse/samples/BulkInserts.java), [`BulkInserts.kt`](../samples/src/main/kotlin/io/github/danielbunting/clickhouse/samples/kotlin/BulkInserts.kt) — `./gradlew :samples:runBulkInserts` / `:samples:runKotlinBulkInserts`
