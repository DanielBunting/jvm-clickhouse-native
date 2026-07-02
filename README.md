# jvm-clickhouse-native

[![CI](https://github.com/DanielBunting/jvm-clickhouse-native/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DanielBunting/jvm-clickhouse-native/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.danielbunting.clickhouse/clickhouse-native-client)](https://central.sonatype.com/artifact/io.github.danielbunting.clickhouse/clickhouse-native-client)
[![Snapshot](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Fcentral.sonatype.com%2Frepository%2Fmaven-snapshots%2Fio%2Fgithub%2Fdanielbunting%2Fclickhouse%2Fclickhouse-native-client%2Fmaven-metadata.xml&label=snapshot&color=blue)](https://central.sonatype.com/repository/maven-snapshots/io/github/danielbunting/clickhouse/clickhouse-native-client/maven-metadata.xml)
[![codecov](https://codecov.io/gh/DanielBunting/jvm-clickhouse-native/branch/main/graph/badge.svg)](https://codecov.io/gh/DanielBunting/jvm-clickhouse-native)
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
- **ADBC driver** — Arrow Database Connectivity; results stream as Arrow `VectorSchemaRoot` batches, plus Arrow-native bulk ingest
- **Kotlin coroutines** — suspend functions, `Flow` streaming (per-row, batched `List<T>`, or whole blocks), config DSL, `Flow`-sourced bulk insert
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

Releases are on [Maven Central](https://central.sonatype.com/artifact/io.github.danielbunting.clickhouse/clickhouse-native-client); all four modules always share one version. The Maven Central badge above (or the [Releases page](https://github.com/DanielBunting/jvm-clickhouse-native/releases)) shows the latest — substitute it for `<version>` below.

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client:<version>")
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-kotlin:<version>") // Kotlin extensions
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-jdbc:<version>")   // JDBC driver
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-adbc:<version>")   // ADBC (Arrow) driver
}
```

```xml
<!-- Maven -->
<dependency>
    <groupId>io.github.danielbunting.clickhouse</groupId>
    <artifactId>clickhouse-native-client</artifactId>
    <version><!-- version --></version>
</dependency>
```

### Snapshots

Every merge to `main` publishes a `-SNAPSHOT` build (the snapshot badge above shows the current one) to the Central Portal snapshots repository. Snapshots are mutable and expire after ~90 days — fine for trying out unreleased fixes, not for shipping. Add the repository:

```kotlin
repositories {
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots/")
}
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

### ADBC

```java
Map<String, Object> params = new HashMap<>();
AdbcDriver.PARAM_URI.set(params, "chnative://localhost:9000/default");

try (BufferAllocator allocator = new RootAllocator();
     AdbcDatabase database = new ChAdbcDriver(allocator).open(params);
     AdbcConnection connection = database.connect();
     AdbcStatement statement = connection.createStatement()) {
    statement.setSqlQuery("SELECT id, name FROM events ORDER BY id");
    try (AdbcStatement.QueryResult result = statement.executeQuery()) {
        ArrowReader reader = result.getReader();
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
            // each Arrow batch holds root.getRowCount() rows
        }
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
| [ADBC driver](docs/adbc.md) | Arrow Database Connectivity: Arrow-batch results, ingest, metadata, type mapping |
| [Cross-client compatibility](docs/cross-client-compatibility.md) | Round-trip behavior vs the official driver, ClickHouse 25.8–26.5 |

## Samples

Runnable end-to-end demos live in [`samples/`](samples/). All honor `CH_HOST`, `CH_PORT`, `CH_DB`, `CH_USER`, `CH_PASSWORD` (defaults: `localhost:9000`, user `default`).

| Sample | What it covers | Run |
|--------|----------------|-----|
| QuickStart | Connect, DDL, bulk insert, query, scalar — mirror of the quickstart doc. Start here. | `:samples:runQuickStart` / `:samples:runKotlinQuickStart` |
| Queries | SELECT patterns: blocks, parameters, typed mapping | `:samples:runQueries` / `:samples:runKotlinQueries` |
| BulkInserts | Inserter lifecycle, large batches, producer-style ingestion | `:samples:runBulkInserts` / `:samples:runKotlinBulkInserts` |
| Pooling | Connection pool usage under concurrency | `:samples:runPooling` / `:samples:runKotlinPooling` |
| AdbcQuickStart | ADBC driver discovery, SELECT, Arrow schema + streaming batches | `:samples:runAdbcQuickStart` |
| Streaming | Multi-block result iteration | (no run task yet — see `Streaming.java`/`Streaming.kt`) |
| Wikimedia / Coinbase / Bluesky | Live event streams ingested via bulk insert (needs internet) | `:samples:runWikimedia`, `:samples:runCoinbase`, `:samples:runBluesky` (+ `runKotlin*` variants) |

## Performance

JMH benchmarks against the official Java driver 0.9.0 — both its v2 JDBC driver (`clickhouse-jdbc`, HTTP) and its idiomatic `client-v2` API — and HousePower `clickhouse-native-jdbc` 2.7.1 (native TCP), reading/writing a 1M-row table. Lower is better; **bold** marks the best value per column. Allocation is `gc.alloc.rate.norm` per operation.

### Streaming read — 1M rows (columnar access)

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** | **14.0 ms** | **15.3 MB** |
| client-v2 (binary reader) | 47.7 ms | 249.3 MB |
| clickhouse-jdbc v2 (HTTP) | 53.1 ms | 249.4 MB |
| housepower (native) | 59.5 ms | 366.5 MB |

### Typed mapping — 1M rows → objects

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** (`query(sql, Class)`) | **28.8 ms** | **137.3 MB** |
| client-v2 (binary reader → record) | 53.0 ms | 279.9 MB |
| clickhouse-jdbc v2 (HTTP) | 54.7 ms | 279.9 MB |
| housepower (native) | 60.9 ms | 397.0 MB |

### String-heavy read — 1M rows

| Driver | All columns (8) | Projected (1 of 8) |
|---|---|---|
| jvm-clickhouse-native | 279.7 ms / 1.51 GB | 34.8 ms / 188.4 MB |
| client-v2 (binary reader) | 239.2 ms / **0.78 GB** | 32.4 ms / 168.8 MB |
| clickhouse-jdbc v2 (HTTP) | 254.3 ms / **0.78 GB** | 32.7 ms / **168.7 MB** |
| housepower (native) | **231.9 ms** / 2.31 GB | **31.3 ms** / 289.0 MB |

> The all-columns string-materialization case is a known loss on allocation (~1.9× the official driver) and hovers around time-parity session to session (a tie in June, ~10–20% behind in July) — lazy string decoding pays off when you don't touch every string cell, and we're working on the full-materialization path.

### Bulk insert — 1M rows

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** (none) | **0.107 s** | **59.7 MB** |
| jvm-clickhouse-native (LZ4) | 0.120 s | 97.0 MB |
| client-v2 (POJO insert) | 0.121 s | 337.0 MB |
| jvm-clickhouse-native (Zstd) | 0.124 s | 78.9 MB |
| housepower (native) | 0.180 s | 734.9 MB |
| clickhouse-jdbc v1 (legacy, HTTP) | 0.250 s | 926.1 MB |
| clickhouse-jdbc v2 (HTTP) | 1.029 s¹ | 2.24 GB |

¹ The v2 JDBC batch path is a known regression over both v1 and its own `client-v2` underpinnings — and requires backtick-quoting the `user` column to avoid an even slower client-side parser fallback (see `BulkInsertBenchmark` javadoc).

Against `client-v2` — the official driver's fastest insert path — single-client wall clock on loopback is a tie for the compressed variants (uncompressed leads outright); the durable differences are **~3.5–5.6× less allocation** here and the native wire format's lower server-side cost (no row→column transpose).

### Wide bulk insert — 1M rows × 30 fixed-width columns

Per-cell costs (the server's row→column transpose for row-major formats, per-cell dispatch in row-shaped driver APIs) scale with column count, so a 30-column numeric table (`bench_wide`: 15 × UInt64 + 15 × Float64, 240 raw B/row — no strings, so UTF-8 work doesn't mask the format difference) separates the drivers far more than the 5-column table:

| Driver | Time | Alloc / op |
|---|---|---|
| **jvm-clickhouse-native** (none) | **0.62 s** | **47.5 MB** |
| clickhouse-jdbc v1 (legacy, HTTP) | 0.99 s | 932.0 MB |
| jvm-clickhouse-native (LZ4) | 1.02 s¹ | 360.7 MB |
| client-v2 (POJO insert) | 1.17 s | 1.83 GB |
| housepower (native) | 1.37 s | 4.76 GB |
| clickhouse-jdbc v2 (HTTP) | 3.06 s¹ | 3.79 GB |

¹ Wall-clock CIs are wide at this payload size (±0.6–1.8 s over 3 iterations for the LZ4 and v2-JDBC variants) — treat mid-table time ordering as approximate; the allocation column (tight CIs) is the reliable signal. On a pure-numeric schema our column buffers produce almost no garbage: **47.5 MB vs 0.9–4.8 GB** for the row-API drivers (~20–100×).

Server-side accounting for the same inserts (`system.query_log`, via the `InsertBreakdown` diagnostic in the benchmarks module): native blocks cost the server **540 ms CPU** per 1M wide rows vs **729 ms** for client-v2's RowBinary (+35% row→column transpose tax, up from ~+20% on the narrow table) and **1,414 ms** for v2 JDBC. Native-protocol clients fully pipeline (client wall ≈ server duration); the HTTP JDBC paths serialize-then-ship, stacking client and server time.

### JDBC vs JDBC — 200k rows

Same JDBC API on both sides (ours over native TCP, official v2 over HTTP) — what an unmodified JDBC application actually feels:

| Scenario | jvm-clickhouse-native | clickhouse-jdbc v2 |
|---|---|---|
| Batch `INSERT` (`executeBatch`) | **119.2 ms** / **367.7 MB** | 187.7 ms / 430.9 MB |
| `getString` scan | **21.0 ms** / 185.3 MB | 31.6 ms / **86.9 MB** |
| `getLong` over UInt64 | **3.6 ms** / **4.2 MB** | 9.3 ms / 35.3 MB |
| `getObject` over UInt64 | **4.2 ms** / **21.0 MB** | 8.5 ms / 35.3 MB |

1.5–2.6× faster on every pairing; the one allocation loss (`getString`, ~2.1×) mirrors the string-materialization note above, while the primitive accessors allocate up to **8× less**.

### Kotlin Flow overhead — 1M-row read (ours only)

| API | Time | Alloc / op |
|---|---|---|
| Java columnar blocks | **13.2 ms** | **15.3 MB** |
| Kotlin `queryBatched` (100k batches) | 19.3 ms | 72.9 MB |
| Java `query(sql, Class)` | 28.6 ms | 137.4 MB |
| Kotlin `queryFlow` (per-row) | 181.8 ms | 79.3 MB |

Per-row `Flow` emission has real overhead — almost entirely the per-element `flowOn` channel handoff. `queryBatched(sql, batchSize) { … }` emits fixed-size `List<T>` chunks (partial final chunk), crossing that channel once per batch instead of once per row: at 100k it runs **~9× faster** than per-row `queryFlow`. For raw columnar throughput, `queryBlocks(sql)` emits whole blocks.

### Test conditions

- Apple M5, 24 GB RAM, macOS 26.5.1; OpenJDK 17.0.18 (Homebrew)
- ClickHouse `clickhouse/clickhouse-server:25.8` in Docker (localhost loopback — network cost is minimized, protocol/allocation cost dominates)
- JMH 2 warmup + 3 measurement iterations, 1 fork, `-prof gc`; 1M-row table, July 2026 run (all tables from a single session)
- Drivers: this client (built from `DB/further-test-porting`, July 2026, post bug-fix pass 35–47), `com.clickhouse:clickhouse-jdbc:0.9.0` (v2 JDBC; legacy v1 via its bundled `DriverV1`), `com.clickhouse:client-v2:0.9.0`, `com.github.housepower:clickhouse-native-jdbc:2.7.1`

Reproduce with `./gradlew :benchmarks:jmh` (requires Docker) — see [`benchmarks/`](benchmarks/). Full matrix (v1/v2 JDBC, client-v2, HousePower, wide tables, and a client-vs-server time split) in [docs/performance-comparison.md](docs/performance-comparison.md).

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
