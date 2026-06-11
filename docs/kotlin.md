# Kotlin Extensions

`clickhouse-native-client-kotlin` layers a coroutine-native surface over the core client: suspend functions for commands, `Flow` for streaming results, primary-constructor binding for data classes, and a config DSL. Nothing here replaces the core API — every extension delegates to the same connection underneath.

- [Dependency](#dependency)
- [Config DSL and connecting](#config-dsl-and-connecting)
- [Commands and scalars](#commands-and-scalars)
- [Streaming queries](#streaming-queries)
- [Typed queries with queryAs](#typed-queries-with-queryas)
- [Query parameters](#query-parameters)
- [Bulk insert](#bulk-insert)
- [Pooling](#pooling)
- [Dispatchers and cancellation](#dispatchers-and-cancellation)

## Dependency

```kotlin
dependencies {
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-kotlin:0.1.0-SNAPSHOT")
    // The coroutines runtime is an implementation detail of the module —
    // declare your own dependency on it:
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
}
```

The core module comes in transitively (`api`).

## Config DSL and connecting

```kotlin
val config = clickHouseConfig {
    host = "localhost"
    port = 9000
    database = "default"
    compression = CompressionMethod.LZ4
    settings["max_execution_time"] = "30"
    // multi-endpoint failover:
    // endpoint("ch1", 9000); endpoint("ch2", 9000)
}

config.connect().use { conn ->   // suspend — handshake runs on Dispatchers.IO
    // ...
}
```

`connect(dispatcher = Dispatchers.IO)` returns a plain `ClickHouseConnection`; close it with `use {}`. The connection is a single stateful socket — one coroutine at a time, same as the core contract.

## Commands and scalars

```kotlin
conn.command("CREATE TABLE IF NOT EXISTS t (id UInt64) ENGINE = MergeTree ORDER BY id")
conn.command("OPTIMIZE TABLE t", settings = mapOf("optimize_throw_if_noop" to "1"))
conn.command("DELETE FROM t WHERE id = {id:UInt64}", queryParametersOf("id" to 42L))

val count: Long = conn.scalar("SELECT count() FROM t")
```

Both are suspend functions; blocking I/O runs on the `dispatcher` parameter (default `Dispatchers.IO`).

## Streaming queries

`queryFlow` emits one `ResultRow` per row, lazily, as blocks arrive from the server:

```kotlin
conn.queryFlow("SELECT id, name, score FROM events").collect { row ->
    val id = row.long("id")            // primitive accessors: long/int/double
    val name = row.string("name")      // String? (null for SQL NULL)
    val score = row.doubleOrNull(2)    // by index; *OrNull variants for nullable columns
    val raw: Any? = row["name"]        // boxed access by name or index
    if (!row.isNull("score")) { /* ... */ }
}
```

`ResultRow` accessors: `long`/`longOrNull`, `int`, `double`/`doubleOrNull`, `string`, `isNull`, `get` operator, `columnCount` — each addressable by column name or index.

For ad-hoc mapping without reflection, pass a mapper:

```kotlin
val names: Flow<String> = conn.query("SELECT name FROM events") { it.string("name")!! }
```

## Typed queries with queryAs

```kotlin
data class Event(val id: Long, val name: String, val score: Double)

conn.queryAs<Event>("SELECT id, name, score FROM events")
    .collect { event -> println(event) }
```

Binding rules:

- **Kotlin classes (incl. data classes)** — result columns bind to **primary-constructor parameters by name** (via kotlin-reflect). Immutable `val`-only classes work; no no-arg constructor needed.
- **Java records / `@JvmRecord`** — the canonical constructor is used.
- **Other Java classes** — fall back to the core POJO mapper (no-arg constructor + fields matched by name).

Names match exactly first, then case-insensitively. SQL `NULL` into a non-nullable parameter throws `ClickHouseException`; nullable parameters (`String?`, `Long?`, …) accept it.

## Query parameters

Server-side `{name:Type}` binding, with two construction styles:

```kotlin
val params = queryParametersOf("minScore" to 8.0, "name" to "alpha")

val same = queryParameters {
    bind("minScore", 8.0)
    bind("name", "alpha")
}

conn.queryFlow(
    "SELECT * FROM events WHERE score > {minScore:Float64} AND name != {name:String}",
    params,
).collect { /* ... */ }
```

`command` and `scalar` accept parameters too. Note that `queryAs` does not take parameters — for parameterized *typed* queries, use `query(sql, params) { row -> ... }` with an explicit mapper.

## Bulk insert

Four overloads cover collections and flows, reified and explicit:

```kotlin
conn.bulkInsert("events", eventsList)                       // Iterable, reified
conn.bulkInsert("events", Event::class.java, eventsList)    // Iterable, explicit class
conn.bulkInsert("events", eventsFlow)                       // Flow, reified
conn.bulkInsert("events", Event::class.java, eventsFlow)    // Flow, explicit class
```

The `Flow` form collects the source and feeds rows straight into the columnar buffers — the pattern the [live-stream samples](../samples/) use for real-time ingestion. See [bulk-insert.md](bulk-insert.md) for lifecycle and failure semantics.

## Pooling

```kotlin
val pool = config.openPool(size = 8, borrowTimeout = Duration.ofSeconds(5))

val count = pool.lease { conn ->
    conn.scalar("SELECT count() FROM events")
}
```

`lease {}` returns the connection in a `NonCancellable` context, so cancellation of the calling coroutine cannot leak a connection. Details in [connection-pooling.md](connection-pooling.md).

## Dispatchers and cancellation

- Every extension takes a `dispatcher: CoroutineDispatcher = Dispatchers.IO` parameter; all blocking socket I/O (connect, query execution, block iteration, insert flushes) runs there. Flow collection itself happens in your context.
- **Cancelling a `Flow` mid-collection is safe**: the underlying `QueryResult` is closed in a `finally` block on both normal completion and cancellation, releasing the connection guard.

## See also

- [Quickstart](quickstart.md) — the Kotlin path end-to-end
- [Configuration](configuration.md#kotlin-dsl)
- Samples: [`QuickStart.kt`](../samples/src/main/kotlin/io/github/danielbunting/clickhouse/samples/kotlin/QuickStart.kt), [`Queries.kt`](../samples/src/main/kotlin/io/github/danielbunting/clickhouse/samples/kotlin/Queries.kt) — `./gradlew :samples:runKotlinQuickStart` / `:samples:runKotlinQueries`
