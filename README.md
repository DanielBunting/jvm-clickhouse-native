# jvm-clickhouse-native

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![JVM](https://img.shields.io/badge/JVM-17%2B-orange)](https://adoptium.net/)

A high-performance JVM client for ClickHouse using the native binary TCP protocol (port 9000), with a column-major data plane, a coroutine-native Kotlin surface, and a JDBC driver.

## Quick Start

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("localhost").port(9000)
    .build();

try (ClickHouseConnection conn = ClickHouseConnection.open(config)) {
    long one = conn.executeScalar("SELECT 1");
    System.out.println(one); // 1
}
```

#### Kotlin

```kotlin
val config = clickHouseConfig { host = "localhost"; port = 9000 }

config.connect().use { conn ->
    println(conn.scalar("SELECT 1")) // 1
}
```

Full walkthrough (table, insert, typed reads): [docs/quickstart.md](docs/quickstart.md).

## Features

- **Native binary protocol** — direct TCP on port 9000; no HTTP layer, no row-format translation
- **Column-major data plane** — blocks decode into primitive arrays with lazy string decoding and unboxed accessors
- **Bulk insert** — `BulkInserter` buffers rows into columnar blocks with reflection mapping for records, POJOs, and data classes
- **Server-side query parameters** — `{name:Type}` placeholders bound in the query packet; no injection, no escaping
- **Typed results** — `query(sql, MyType.class)` → `Stream<T>` in Java, `queryAs<T>(sql)` → `Flow<T>` in Kotlin
- **Compression** — LZ4 (default) and Zstd with CityHash128 checksums
- **Async & cancellation** — `queryAsync` returns `CompletableFuture`; `cancel()` sends a real Cancel packet cross-thread
- **Connection pooling** — semaphore-based, self-healing, validate-on-borrow, poison detection
- **Failover & load balancing** — multi-endpoint configs with `FIRST_ALIVE`, `ROUND_ROBIN`, `RANDOM` policies
- **TLS / mTLS / tokens** — trust stores, client certificates, access-token auth
- **JDBC 4.3 driver** — `jdbc:chnative://`, prepared statements, multi-row batch collapse
- **Kotlin coroutines** — suspend functions, `Flow` streaming, config DSL, `Flow`-sourced bulk insert
- **50+ ClickHouse types** — through `Variant`, `Dynamic`, `JSON`, and Geo
- **Cross-client tested** — round-trip compatibility with the official JDBC driver verified on ClickHouse 25.8–26.5

## Supported ClickHouse Types

| Category | ClickHouse Types | JVM Mapping |
|---|---|---|
| **Signed integers** | `Int8` … `Int64`, `Int128`, `Int256` | `Byte`, `Short`, `Integer`, `Long`, `BigInteger` |
| **Unsigned integers** | `UInt8` … `UInt64`, `UInt128`, `UInt256` | `Integer`/`Long` (widened; `UInt64` = raw bits), `BigInteger` |
| **Floating point** | `Float32`, `Float64`, `BFloat16` | `Float`, `Double`, `Float` |
| **Decimal / Bool** | `Decimal(P,S)`, `Bool` | `BigDecimal`, `Boolean` |
| **Strings** | `String`, `FixedString(N)` | `String` (lazy UTF-8 decode) |
| **Temporal** | `Date`, `Date32`, `DateTime`, `DateTime64(P)`, `Time`, `Time64(P)`, `Interval*` | `LocalDate`, `Instant`, `Duration`, `Period` |
| **Identifiers** | `UUID`, `IPv4`, `IPv6` | `UUID`, `Inet4Address`, `Inet6Address` |
| **Enums** | `Enum8`, `Enum16` | `String` (name) |
| **Composites** | `Array(T)`, `Tuple(...)`, `Map(K,V)`, `Nullable(T)`, `LowCardinality(T)`, `SimpleAggregateFunction` | `List`, `List`, `LinkedHashMap`, `T?`, `T` (transparent) |
| **Geo** | `Point`, `Ring`, `LineString`, `Polygon`, `MultiLineString`, `MultiPolygon` | nested `List`s of `Double` |
| **Semi-structured** | `Variant(...)`, `Dynamic`, `JSON` | active member's type / JSON `String` |

Full reference with notes and insert-binding rules: [docs/data-types.md](docs/data-types.md).

## Installation

> **Not yet on Maven Central.** Until the first release, build from source and publish locally:
>
> ```bash
> git clone https://github.com/DanielBunting/jvm-clickhouse-native
> cd jvm-clickhouse-native && ./gradlew publishToMavenLocal
> ```

```kotlin
// Gradle (Kotlin DSL) — with mavenLocal() in repositories
dependencies {
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client:0.1.0-SNAPSHOT")
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-kotlin:0.1.0-SNAPSHOT") // Kotlin extensions
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-jdbc:0.1.0-SNAPSHOT")   // JDBC driver
}
```

```xml
<!-- Maven -->
<dependency>
    <groupId>io.github.danielbunting.clickhouse</groupId>
    <artifactId>clickhouse-native-client</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Basic Usage

### Execute a query

#### Java

```java
conn.execute("CREATE TABLE IF NOT EXISTS events (id UInt64, name String, score Float64) "
    + "ENGINE = MergeTree ORDER BY id");
long count = conn.executeScalar("SELECT count() FROM events");
```

#### Kotlin

```kotlin
conn.command("CREATE TABLE IF NOT EXISTS events (id UInt64, name String, score Float64) ENGINE = MergeTree ORDER BY id")
val count = conn.scalar("SELECT count() FROM events")
```

### Query with parameters

Parameters bind **server-side** via `{name:Type}` placeholders:

#### Java

```java
QueryParameters params = QueryParameters.builder()
    .bind("minScore", 8.0)
    .build();

try (QueryResult result = conn.query(
        "SELECT * FROM events WHERE score > {minScore:Float64}", params)) {
    // iterate result.blocks()
}
```

#### Kotlin

```kotlin
conn.queryFlow(
    "SELECT * FROM events WHERE score > {minScore:Float64}",
    queryParametersOf("minScore" to 8.0),
).collect { row -> println(row.string("name")) }
```

### Typed results

#### Java

```java
public record Event(long id, String name, double score) {}

try (Stream<Event> events = conn.query("SELECT id, name, score FROM events", Event.class)) {
    events.forEach(System.out::println);
}
```

#### Kotlin

```kotlin
data class Event(val id: Long, val name: String, val score: Double)

conn.queryAs<Event>("SELECT id, name, score FROM events")
    .collect { println(it) }
```

### Bulk insert

#### Java

```java
try (BulkInserter<Event> inserter = conn.createBulkInserter("events", Event.class)) {
    inserter.init();
    inserter.addRange(events);
    inserter.complete();
}
```

#### Kotlin

```kotlin
conn.bulkInsert("events", events)        // List or Flow sources
```

### JDBC

```java
try (Connection conn = DriverManager.getConnection(
        "jdbc:chnative://default@localhost:9000/default");
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery("SELECT id, name FROM events")) {
    while (rs.next()) {
        System.out.println(rs.getLong("id") + ": " + rs.getString("name"));
    }
}
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Quickstart](docs/quickstart.md) | Get up and running in minutes |
| [Configuration](docs/configuration.md) | Builder reference, URLs, endpoints/failover, compression, timeouts |
| [Authentication & TLS](docs/authentication.md) | Passwords, access tokens, TLS, mutual TLS |
| [Data types](docs/data-types.md) | Every ClickHouse ↔ JVM mapping, accessors, insert binding |
| [Bulk insert](docs/bulk-insert.md) | High-throughput columnar writes |
| [Connection pooling](docs/connection-pooling.md) | Concurrency, validation, self-healing |
| [Kotlin extensions](docs/kotlin.md) | Coroutines, `Flow`, config DSL |
| [JDBC driver](docs/jdbc.md) | `jdbc:chnative://` usage and limitations |
| [Cross-client compatibility](docs/cross-client-compatibility.md) | Round-trip behavior vs the official driver, ClickHouse 25.8–26.5 |

## Samples

Runnable end-to-end demos live in [`samples/`](samples/). All honor `CH_HOST`, `CH_PORT`, `CH_DB`, `CH_USER`, `CH_PASSWORD` (defaults: `localhost:9000`, user `default`).

| Sample | What it covers | Run |
|--------|----------------|-----|
| QuickStart | Connect, DDL, bulk insert, query, scalar — mirror of the quickstart doc. Start here. | `:samples:runQuickStart` / `:samples:runKotlinQuickStart` |
| Queries | SELECT patterns: blocks, parameters, typed mapping | `:samples:runQueries` / `:samples:runKotlinQueries` |
| BulkInserts | Inserter lifecycle, large batches, producer-style ingestion | `:samples:runBulkInserts` / `:samples:runKotlinBulkInserts` |
| Pooling | Connection pool usage under concurrency | `:samples:runPooling` / `:samples:runKotlinPooling` |
| Streaming | Multi-block result iteration | (no run task yet — see `Streaming.java`/`Streaming.kt`) |
| Wikimedia / Coinbase / Bluesky | Live event streams ingested via bulk insert (needs internet) | `:samples:runWikimedia`, `:samples:runCoinbase`, `:samples:runBluesky` (+ `runKotlin*` variants) |

## Performance

JMH benchmarks against the official `clickhouse-jdbc` 0.6.5 (HTTP transport) and HousePower `clickhouse-native-jdbc` 2.7.1 (native TCP), reading/writing a 1M-row table. Lower is better; **bold** marks the best value per column. Allocation is `gc.alloc.rate.norm` per operation.

### Streaming read — 1M rows (columnar access)

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** | **9.2 ms** | 16.0 MB |
| clickhouse-jdbc (HTTP) | 33.0 ms | **7.1 MB** |
| housepower (native) | 62.4 ms | 384.3 MB |

### Typed mapping — 1M rows → objects

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** (`query(sql, Class)`) | **28.9 ms** | 144.0 MB |
| clickhouse-jdbc (HTTP) | 34.0 ms | **39.2 MB** |
| housepower (native) | 65.2 ms | 416.3 MB |

### String-heavy read — 1M rows

| Driver | All columns | Projected (2 cols) |
|---|---|---|
| jvm-clickhouse-native | 238.0 ms / 1.58 GB | 32.7 ms / 197.6 MB |
| clickhouse-jdbc (HTTP) | **162.8 ms / 0.67 GB** | **24.6 ms / 84.2 MB** |
| housepower (native) | 234.9 ms / 2.42 GB | 32.9 ms / 303.0 MB |

> The all-columns string-materialization case is currently a known loss against the official HTTP driver — lazy string decoding pays off when you don't touch every string cell, and we're working on the full-materialization path.

### Bulk insert — 1M rows

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** (LZ4) | **0.122 s** | 101.6 MB |
| jvm-clickhouse-native (Zstd) | 0.126 s | 82.7 MB |
| jvm-clickhouse-native (none) | 0.160 s | **58.4 MB** |
| housepower (native) | 0.166 s | 770.5 MB |
| clickhouse-jdbc (HTTP) | 0.266 s | 970.9 MB |

~1.4× faster than the fastest competitor with **7–10× less allocation**.

### Kotlin Flow overhead — 1M-row read (ours only)

| API | Time | Alloc / op |
|---|---|---|
| Java columnar blocks | **10.3 ms** | 16.0 MB |
| Java `query(sql, Class)` | 31.2 ms | 144.0 MB |
| Kotlin `queryFlow` | 174.0 ms | 83.1 MB |

Per-row `Flow` emission has real overhead; for hot paths, read columnar blocks and wrap your own batching.

### Test conditions

- Apple M5, 24 GB RAM, macOS 26.5.1; OpenJDK 17.0.18 (Homebrew)
- ClickHouse `clickhouse/clickhouse-server:25.8` in Docker (localhost loopback — network cost is minimized, protocol/allocation cost dominates)
- JMH 2 warmup + 3 measurement iterations, 1 fork, `-prof gc`; 1M-row table, June 2026 run
- Drivers: this client 0.1.0-SNAPSHOT, `com.clickhouse:clickhouse-jdbc:0.6.5`, `com.github.housepower:clickhouse-native-jdbc:2.7.1`

Reproduce with `./gradlew :benchmarks:jmh` (requires Docker) — see [`benchmarks/`](benchmarks/).

## Requirements

- **JVM 17+**
- **ClickHouse 25.8 – 26.5** (the integration-tested matrix; other versions likely work)
- Docker — only for integration tests and benchmarks (Testcontainers)

## Contributing

```bash
./gradlew build                                            # compile + unit tests
./gradlew :clickhouse-native-client:integrationTest        # integration tests (Docker)
./gradlew :clickhouse-native-client:integrationTestAllVersions  # full version matrix
```

Issues and PRs welcome.

## License

[Apache License 2.0](LICENSE)
