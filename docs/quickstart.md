# Quickstart

Connect, create a table, bulk-insert, and query — in about five minutes.

## Prerequisites

- **JDK 17+**
- A running ClickHouse server. The fastest way is Docker:

```bash
docker run -d --name clickhouse -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server:25.8
```

The native client talks to the **native TCP port 9000** (not the HTTP port 8123).

## Add the dependency

The library is not yet on Maven Central. Until the first release, publish it locally:

```bash
git clone https://github.com/DanielBunting/jvm-clickhouse-native
cd jvm-clickhouse-native
./gradlew publishToMavenLocal
```

Then depend on it with `mavenLocal()` in your repositories:

```kotlin
// build.gradle.kts
repositories { mavenLocal(); mavenCentral() }

dependencies {
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client:0.1.0-SNAPSHOT")
    // Kotlin coroutines/Flow extensions (optional):
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-kotlin:0.1.0-SNAPSHOT")
    // JDBC driver (optional):
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-jdbc:0.1.0-SNAPSHOT")
}
```

## Open a connection

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("localhost")
    .port(9000)
    .database("default")
    .username("default")
    .password("")
    .build();

try (ClickHouseConnection conn = ClickHouseConnection.open(config)) {
    // ...
}
```

#### Kotlin

```kotlin
val config = clickHouseConfig {
    host = "localhost"
    port = 9000
    database = "default"
}

config.connect().use { conn ->
    // ...
}
```

A URL works too: `ClickHouseConfig.fromUrl("chnative://default@localhost:9000/default")`. See [configuration.md](configuration.md) for every option.

## Create a table

#### Java

```java
conn.execute(
    "CREATE TABLE IF NOT EXISTS events ("
    + "id    UInt64,"
    + "name  String,"
    + "score Float64"
    + ") ENGINE = MergeTree ORDER BY id");
```

#### Kotlin

```kotlin
conn.command(
    """
    CREATE TABLE IF NOT EXISTS events (
        id    UInt64,
        name  String,
        score Float64
    ) ENGINE = MergeTree ORDER BY id
    """,
)
```

## Insert rows

Define a row type whose component names match the table columns, then bulk-insert. Rows are buffered into columnar blocks and shipped over the native protocol — no SQL string building.

#### Java

```java
public record Event(long id, String name, double score) {}

List<Event> rows = List.of(
    new Event(1L, "alpha", 9.5),
    new Event(2L, "beta",  7.3));

try (BulkInserter<Event> inserter = conn.createBulkInserter("events", Event.class)) {
    inserter.init();          // fetch the target schema, allocate column buffers
    inserter.addRange(rows);  // buffer rows (flushes blocks as batches fill)
    inserter.complete();      // flush the tail and terminate the insert
}
```

#### Kotlin

```kotlin
data class Event(val id: Long, val name: String, val score: Double)

val rows = listOf(
    Event(1, "alpha", 9.5),
    Event(2, "beta", 7.3))

conn.bulkInsert("events", rows)
```

See [bulk-insert.md](bulk-insert.md) for lifecycle details, batch sizing, and `Flow` sources.

## Query rows back

#### Java

Results stream as columnar blocks:

```java
try (QueryResult result = conn.query("SELECT id, name, score FROM events ORDER BY id")) {
    var blocks = result.blocks();
    while (blocks.hasNext()) {
        Block block = blocks.next();
        for (int row = 0; row < block.rowCount(); row++) {
            System.out.printf("%s %s %s%n",
                block.column(0).value(row),
                block.column(1).value(row),
                block.column(2).value(row));
        }
    }
}
```

Or map straight to objects:

```java
try (Stream<Event> events = conn.query("SELECT id, name, score FROM events", Event.class)) {
    events.forEach(System.out::println);
}
```

#### Kotlin

`queryAs` binds rows to the data class's primary constructor and streams them as a `Flow`:

```kotlin
conn.queryAs<Event>("SELECT id, name, score FROM events ORDER BY id")
    .collect { println(it) }
```

## Scalar queries

#### Java

```java
long count = conn.executeScalar("SELECT count() FROM events");
```

#### Kotlin

```kotlin
val count = conn.scalar("SELECT count() FROM events")
```

## Parameterized queries

Parameters are bound **server-side** with `{name:Type}` placeholders — no SQL injection, no client-side escaping:

#### Java

```java
QueryParameters params = QueryParameters.builder()
    .bind("minScore", 8.0)
    .build();

try (QueryResult result = conn.query(
        "SELECT * FROM events WHERE score > {minScore:Float64}", params)) {
    // ...
}
```

#### Kotlin

```kotlin
conn.query(
    "SELECT id, name, score FROM events WHERE score > {minScore:Float64}",
    queryParametersOf("minScore" to 8.0),
) { row -> Event(row.long("id"), row.string("name")!!, row.double("score")) }
    .collect { println(it) }
```

## Runnable samples

The [samples module](../samples/) contains this exact flow plus more:

```bash
CH_HOST=localhost CH_PORT=9000 ./gradlew :samples:runQuickStart        # Java
CH_HOST=localhost CH_PORT=9000 ./gradlew :samples:runKotlinQuickStart  # Kotlin
```

All samples honor `CH_HOST`, `CH_PORT`, `CH_DB`, `CH_USER`, `CH_PASSWORD` (defaults: `localhost:9000`, `default` database/user, empty password).

## Next steps

- [Configuration](configuration.md) — endpoints, failover, compression, timeouts, settings
- [Data types](data-types.md) — every ClickHouse ↔ JVM type mapping
- [Bulk insert](bulk-insert.md) — high-throughput ingestion
- [Connection pooling](connection-pooling.md) — concurrency
- [Kotlin extensions](kotlin.md) — coroutines, `Flow`, the config DSL
- [JDBC driver](jdbc.md) — drop-in `java.sql` usage
