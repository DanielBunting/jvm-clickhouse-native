# Connection Pooling

A `ClickHouseConnection` is a single stateful socket with **one in-flight operation at a time** — concurrent use from multiple threads is rejected with `ConcurrentConnectionUseException`. For parallel queries or writers, use `ClickHouseConnectionPool`: a fixed-size, thread-safe pool of independent connections.

- [Creating a pool](#creating-a-pool)
- [Borrowing connections](#borrowing-connections)
- [Health: validation and poisoning](#health-validation-and-poisoning)
- [Kotlin](#kotlin)
- [Sizing](#sizing)

## Creating a pool

#### Java

```java
// defaults: 10s borrow timeout, validate-on-borrow enabled
try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.create(config, 8)) {
    // ...
}

// or with the builder
ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config)
    .size(16)
    .borrowTimeout(Duration.ofSeconds(5))
    .validateOnBorrow(true)
    .build();
```

#### Kotlin

```kotlin
val pool = config.openPool(size = 8)   // suspend; opens connections on Dispatchers.IO
```

Connections are opened **eagerly** at construction, so an unreachable server fails fast with `ClickHouseException`.

## Borrowing connections

A borrowed connection is exclusively yours until you close it — `close()` **returns it to the pool** rather than closing the socket:

#### Java

```java
// try-with-resources
try (ClickHouseConnection conn = pool.borrow()) {
    conn.execute("INSERT INTO t ...");
}

// leak-proof callback forms
long count = pool.withConnection(c -> c.executeScalar("SELECT count() FROM t"));
pool.useConnection(c -> c.execute("TRUNCATE TABLE staging"));
```

#### Kotlin

```kotlin
val count = pool.lease { conn ->
    conn.scalar("SELECT count() FROM t")
}
```

`borrow()` blocks up to the configured `borrowTimeout` (default 10 s) and throws `ClickHouseException` if the pool is exhausted. `pool.size()` and `pool.available()` expose capacity for monitoring.

## Health: validation and poisoning

The pool is **self-healing**. Capacity is tracked by permits, not fixed socket instances:

- **Invalidate on return** — a connection whose protocol stream is desynced, whose socket broke, or whose bulk INSERT was abandoned mid-stream is flagged [poisoned](bulk-insert.md#failure-semantics) and is closed instead of recycled.
- **Validate on borrow** (default on) — each reused idle connection is checked with `SELECT 1` before being handed out, catching connections that died silently while idle. Disable with `validateOnBorrow(false)` to shave a round-trip per borrow if your network is reliable.
- **Lazy replacement** — a discarded connection frees its permit; the next `borrow()` opens a fresh replacement. A transient outage can never permanently shrink the pool.

## Kotlin

```kotlin
val pool = config.openPool(
    size = 8,
    borrowTimeout = Duration.ofSeconds(5),
    validateOnBorrow = true,
)

val total = pool.lease { conn ->
    conn.scalar("SELECT count() FROM events")
}

pool.close()
```

`lease {}` borrows on `Dispatchers.IO`, runs your suspend block, and returns the connection in a `NonCancellable` context — the connection cannot leak even if the calling coroutine is cancelled mid-block.

## Sizing

Each connection supports one in-flight operation, so size the pool to your **peak concurrent operations**, not your thread count. Starting points:

- Read-heavy services: number of concurrent request handlers that touch ClickHouse.
- Ingestion pipelines: one connection per concurrent `BulkInserter`.
- Remember ClickHouse server limits (`max_concurrent_queries`, per-user quotas) — a huge pool just moves queueing to the server.

## See also

- [Configuration](configuration.md)
- [Bulk insert — failure semantics](bulk-insert.md#failure-semantics)
- Samples: [`Pooling.java`](../samples/src/main/java/io/github/danielbunting/clickhouse/samples/Pooling.java), [`Pooling.kt`](../samples/src/main/kotlin/io/github/danielbunting/clickhouse/samples/kotlin/Pooling.kt) — `./gradlew :samples:runPooling` / `:samples:runKotlinPooling`
