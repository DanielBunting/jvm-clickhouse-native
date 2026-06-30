# ADBC Driver

`clickhouse-native-client-adbc` is an [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) driver over the native client — the standard `org.apache.arrow.adbc.core` API, native TCP transport underneath, results handed back as Arrow [`VectorSchemaRoot`](https://arrow.apache.org/docs/java/) batches. It targets Arrow-native consumers: query results stream straight into Arrow vectors with no row-by-row boxing, and writes ingest an Arrow `VectorSchemaRoot` directly.

- [Dependency](#dependency)
- [Connecting](#connecting)
- [Queries](#queries)
- [Bulk ingest](#bulk-ingest)
- [Metadata](#metadata)
- [Type mapping](#type-mapping)
- [Resource ownership](#resource-ownership)
- [Limitations](#limitations)

## Dependency

```kotlin
dependencies {
    implementation("io.github.danielbunting.clickhouse:clickhouse-native-client-adbc:<version>")
    // Driver-manager discovery (optional — only if you connect via AdbcDriverManager):
    implementation("org.apache.arrow.adbc:adbc-driver-manager:0.23.0")
    // An Arrow off-heap allocator implementation (the driver hands out off-heap buffers):
    runtimeOnly("org.apache.arrow:arrow-memory-netty:18.3.0")
}
```

The driver is built against **adbc-java 0.23.0 / Arrow 18.3.0** (kept in lockstep — the ADBC result shapes drift across releases). On JDK 17 Arrow's off-heap allocator reflects into `java.nio`, so the running JVM needs:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Connecting

Every ADBC handle owns Arrow memory, so a connection starts from a `BufferAllocator`. Two ways to obtain a database:

**Directly** — construct the driver with a caller-owned allocator:

```java
try (BufferAllocator allocator = new RootAllocator();
     AdbcDatabase database = new ChAdbcDriver(allocator).open(params);
     AdbcConnection connection = database.connect();
     AdbcStatement statement = connection.createStatement()) {
    // ...
}
```

**Via the driver manager** — how an external Arrow consumer discovers it:

```java
AdbcDriverManager.getInstance().registerDriver(ChAdbcDriver.DRIVER_NAME, ChAdbcDriver.FACTORY);
AdbcDatabase database = AdbcDriverManager.getInstance()
        .connect(ChAdbcDriver.DRIVER_NAME, allocator, params);
```

The target is given either as a [`chnative://` URI](configuration.md#url-format) or as discrete parameters:

```java
Map<String, Object> params = new HashMap<>();

// URI form — all the native URL query parameters (compression, TLS, timeouts, settings.*) work here:
AdbcDriver.PARAM_URI.set(params, "chnative://localhost:9000/default?compression=lz4");

// ...or discrete host/port/database:
params.put(AdbcParams.PARAM_HOST, "localhost");
params.put(AdbcParams.PARAM_PORT, 9000);          // Integer or numeric String
params.put(AdbcParams.PARAM_DATABASE, "default");
```

`AdbcDriver.PARAM_USERNAME` / `PARAM_PASSWORD` set credentials and override any in the URI. Invalid parameters (bad scheme, missing host, non-numeric port) fail fast with `AdbcException(INVALID_ARGUMENT)`; an unreachable server or rejected handshake surfaces from `connect()` as `AdbcException(IO)`.

## Queries

```java
statement.setSqlQuery("SELECT id, name, score FROM events ORDER BY id");
try (AdbcStatement.QueryResult result = statement.executeQuery()) {
    ArrowReader reader = result.getReader();
    VectorSchemaRoot root = reader.getVectorSchemaRoot();   // schema known up front, before any batch
    while (reader.loadNextBatch()) {
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        for (int r = 0; r < root.getRowCount(); r++) {
            // id.get(r), name.get(r) ...
        }
    }
}
```

- Results **stream**: each native block becomes one Arrow batch, so a large result never materialises in full. An empty result still exposes the column schema and simply yields zero batches.
- The reader owns its off-heap buffers; closing the `QueryResult` frees them.
- `AdbcStatement.cancel()` (and `AdbcConnection.cancel()`) sends a real Cancel packet for the in-flight query.
- DDL/DML go through `executeUpdate()`; ClickHouse's native protocol reports no affected-row count for these, so `UpdateResult.getAffectedRows()` is `-1`.

## Bulk ingest

`bulkIngest` feeds a bound `VectorSchemaRoot` straight into the native bulk inserter — the inverse of the read bridge, no intermediate POJO:

```java
try (VectorSchemaRoot root = VectorSchemaRoot.create(
        ClickHouseArrowTypes.schema(List.of("id", "name"), List.of("Int64", "String")), allocator)) {
    // fill the vectors and root.setRowCount(n) ...
    try (AdbcStatement ingest = connection.bulkIngest("events", BulkIngestMode.APPEND)) {
        ingest.bind(root);
        long rows = ingest.executeUpdate().getAffectedRows();
    }
}
```

`ClickHouseArrowTypes.schema(names, types)` is a convenience for building a target Arrow schema from ClickHouse type strings; an Arrow consumer can equally bind a root it built itself.

`BulkIngestMode` controls table creation (the table is built from the bound Arrow schema as `ENGINE = MergeTree ORDER BY tuple()`):

| Mode | Behavior |
|---|---|
| `APPEND` | The table must already exist. |
| `CREATE` | Create the table (error if it exists), then insert. |
| `CREATE_APPEND` | Create if absent, then insert. |
| `REPLACE` | Drop any existing table, recreate, then insert. |

- Only the bound columns are named in the `INSERT`, so a table column absent from the root takes its server-side `DEFAULT`.
- The exact source ClickHouse type is preserved in Arrow field metadata on read, so a read→`CREATE`-ingest round trip recreates the original column type (e.g. `Date` stays `Date`, `UUID` stays `UUID` rather than collapsing to the structural `FixedString`).
- A zero-row root is a no-op returning `0`; a bound value the target codec can't accept (e.g. a non-numeric `String` into an `Int64` column) fails as an `AdbcException`.

## Metadata

| Method | Backed by |
|---|---|
| `getTableSchema(catalog, dbSchema, table)` | `system.columns` → the same Arrow schema the reader produces (`NOT_FOUND` if the table is absent) |
| `getObjects(depth, …)` | `system.databases` / `system.tables` / `system.columns`, populated down to the requested `GetObjectsDepth` |
| `getTableTypes()` | `TABLE`, `VIEW` |
| `getInfo(codes)` | vendor name/version, driver name/version |

ClickHouse has no catalog layer, so `getObjects` exposes a single unnamed catalog (`""`) whose db-schemas are ClickHouse databases — consistent with `getTableSchema`, which treats `dbSchema` as the database.

## Type mapping

Representation choices are fixed and asserted by the test suite (`BlockToArrowTest`, `AdbcTypeRepresentationIT`):

| ClickHouse | Arrow |
|---|---|
| `Int8` … `Int64` | `Int(width, signed)` |
| `UInt8` … `UInt64` | `Int(width, unsigned)` — `UInt64` carries raw bits |
| `Float32` / `Float64` | `FloatingPoint(SINGLE / DOUBLE)` |
| `Bool` | `Bool` |
| `String` | `Utf8` (arbitrary bytes — NUL-safe, UTF-8 preserved) |
| `FixedString(N)` | `FixedSizeBinary(N)`, NUL-padded |
| `Date` / `Date32` | `Date(DAY)` |
| `DateTime` | `Timestamp(SECOND, tz)` |
| `DateTime64(p)` | `Timestamp(MILLI/MICRO/NANO, tz)` — nearest unit ≥ `p` |
| `Decimal(P, S)` | `Decimal(P, S, 128 or 256)` |
| `UUID` | `FixedSizeBinary(16)`, big-endian |
| `IPv4` | `Int(32, unsigned)`, network byte order |
| `IPv6` | `FixedSizeBinary(16)` |
| `Enum8` / `Enum16` | `Int(8 / 16, signed)` — the underlying value, not the label |
| `Nullable(T)` | `T`, field marked nullable |
| `LowCardinality(T)` | unwrapped `T` (dictionary encoding is a later optimisation) |
| `Array(T)` | `List<T>` |
| `Map(K, V)` | `Map` (`struct<key, value>`, `keysSorted = false`) |
| `Tuple(…)` | `Struct` |

A type with no Arrow mapping (e.g. `Int128`/`Int256`) raises `AdbcException(NOT_IMPLEMENTED)` rather than a raw error.

## Resource ownership

Allocators form a parent→child tree: **driver → database → connection → statement/reader**. Each handle owns a child of its parent and closes it on `close()`, and every reader frees its batch buffers when its `QueryResult` is closed. Off-heap Arrow buffers don't show up in a heap dump, so close handles in reverse order (try-with-resources does this) — a leaked buffer is otherwise invisible.

## Performance

ADBC trades a little read throughput for Arrow-native results. `AdbcSelectBenchmark` (in `benchmarks/`) runs the same `SELECT id, value FROM bench` over 1M rows through both the ADBC reader and the core client's native columnar path, summing the same two columns:

```bash
./gradlew :benchmarks:jmh    # runs AdbcSelectBenchmark with the rest; the gc profiler reports bytes/op
```

In a local run (JDK 17, ClickHouse 25.x via Testcontainers on loopback), the ADBC path costs roughly **~1.5× the wall clock and ~3× the on-heap allocation** of the raw native read — the price of transcoding each native block into an Arrow `VectorSchemaRoot` and managing the reader's buffers. Two caveats that matter:

- The `gc` profiler counts **on-heap** allocation only; Arrow's batch buffers are **off-heap** (memory-netty), so ADBC's real memory traffic is higher than the heap figure shows. The on-heap cost is mostly per-batch vector setup, not the column data.
- Docker-on-macOS wall clock is noisy — treat the **ratio**, not the absolute milliseconds, as the finding. The native path's absolute numbers live in [performance-comparison.md](performance-comparison.md).

The overhead pays off when the consumer is already Arrow-native: the result lands as Arrow vectors ready for Arrow compute, Arrow Flight, or an engine like DuckDB/Polars with **no further conversion**, so an end-to-end pipeline can come out ahead even though the isolated read is slower than the columnar `Block` path. If you're staying in plain JVM types, the core client or [JDBC driver](jdbc.md) read paths are faster and lighter.

## Limitations

- **Autocommit only.** ClickHouse has no general multi-statement transactions, so `setAutoCommit(false)`, `commit()` and `rollback()` raise `NOT_IMPLEMENTED` (mirroring the [JDBC driver](jdbc.md#limitations)).
- **One connection = one native connection**, single in-flight statement — pool externally, as with the core client.
- **No dictionary encoding** yet: `LowCardinality(T)` is delivered as plain `T`.

## See also

- [Configuration / URL parameters](configuration.md#url-format)
- [Data types](data-types.md) — the core client's ClickHouse ↔ JVM mapping
- [Bulk insert](bulk-insert.md) — the native write path the ingest bridge sits on
- Runnable sample: `AdbcQuickStart.java` (`./gradlew :samples:runAdbcQuickStart`)
