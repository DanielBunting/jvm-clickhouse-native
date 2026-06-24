# Performance Comparison

Full benchmark matrix comparing **jvm-clickhouse-native** against the official
[ClickHouse Java client](https://github.com/ClickHouse/clickhouse-java) `0.9.0` —
in both its **v2 JDBC** driver and its idiomatic **`client-v2`** API, plus the
legacy **v1** JDBC driver where it is still instructive — and the community
[HousePower `clickhouse-native-jdbc`](https://github.com/housepower/ClickHouse-Native-JDBC)
(native TCP).

Each row shows **time / allocation** (median wall-clock from JMH `AverageTime`
mode, bytes-per-op from the `gc` profiler's `gc.alloc.rate.norm`). **Bold marks
the best value in each column of the row** — time and allocation are judged
independently, so a row can have two bold cells in different competitors.

> One structural fact frames the whole matrix: the official driver is **HTTP +
> RowBinary** (the server transposes row-major bytes into columns and, on reads,
> back again). This library and HousePower speak the **native TCP protocol**,
> shipping columnar `Block`s the server stores almost verbatim. The largest and
> most durable differences below are allocation and server-side CPU, not
> single-client wall clock on loopback.

## Test conditions

| Item | Value |
|---|---|
| CPU | Apple M5 (10 logical / 10 physical cores) |
| RAM | 24 GB |
| OS | macOS 26.5.1 (Darwin 25.5.0) |
| JVM | OpenJDK 17.0.18 (Homebrew), 64-bit Server VM |
| ClickHouse | `clickhouse/clickhouse-server:25.8` (Docker, Testcontainers; localhost loopback) |
| jvm-clickhouse-native | built from `main`, June 2026 |
| `com.clickhouse:clickhouse-jdbc` | `0.9.0` — v2 driver (`ClickHouseDriver`, HTTP 8123); legacy v1 via the bundled `DriverV1` |
| `com.clickhouse:client-v2` | `0.9.0` (HTTP 8123) — idiomatic non-JDBC API |
| `com.github.housepower:clickhouse-native-jdbc` | `2.7.1` (native TCP, port 9000) |
| JMH config | `Fork=1`, `Warmup=2`, `Measurement=3`, `-prof gc`; 1M-row table |

All numbers are a single June 2026 session. Docker-on-macOS wall clock is noisy
(see the per-section caveats); treat the **ratios and the allocation column** as
the durable findings, not absolute milliseconds.

## Streaming reads — `id + value` (`StreamingSelectBenchmark`)

Fully consuming `SELECT id, value FROM bench` (1M rows), summing primitives with
no boxing — measures wire/decode throughput and allocation.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (columnar blocks) | **8.9 ms** / **16.0 MB** |
| client-v2 (binary reader) | 49.2 ms / 261.5 MB |
| clickhouse-jdbc v2 (HTTP) | 53.9 ms / 261.5 MB |
| housepower (native) | 58.7 ms / 384.3 MB |

The durable win is **16× less allocation**: v2's binary reader materialises a
`Map`-backed record per row even behind positional getters, while our columnar
path touches only the backing arrays. Wall clock follows from that — ~5.5× faster
in this session — but on loopback that ratio is the noisier figure (see the
note above on treating allocation, not absolute ms, as the durable finding).

## Materialised typed reads — 1M rows → objects (`MappedSelectBenchmark`)

Each driver builds one `record (long id, double value)` per row, so all produce
the same materialised objects.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (`query(sql, Class)`) | **27.6 ms** / **144.0 MB** |
| client-v2 (binary reader → record) | 49.2 ms / 293.5 MB |
| clickhouse-jdbc v2 (HTTP) | 54.2 ms / 293.5 MB |
| housepower (native) | 57.0 ms / 416.3 MB |

## String-heavy reads — 1M rows × 8 wide String columns (`StringSelectBenchmark`)

Lazy String materialisation. `all` reads every cell of all 8 columns;
`projected` reads only 1 of the 8 (the unread columns are never decoded into
`String`).

| Library | All 8 columns | Projected (1 of 8) |
|---|---|---|
| jvm-clickhouse-native | 240.2 ms / 1.58 GB | **29.0 ms** / 197.6 MB |
| client-v2 (binary reader) | **229.1 ms** / **0.81 GB** | 33.0 ms / 177.0 MB |
| clickhouse-jdbc v2 (HTTP) | 257.0 ms / **0.81 GB** | 32.6 ms / **176.9 MB** |
| housepower (native) | 237.9 ms / 2.42 GB | 31.5 ms / 303.0 MB |

The all-columns case is a statistical tie on time and a known **allocation** loss
against the official driver (lazy String decode pays off only when you skip
cells; the full-materialisation path is being worked on). When you project,
non-materialisation of the unread 7 columns shows.

## Bulk insert — 1M rows × 5 columns (`BulkInsertBenchmark`)

`(id UInt64, ts DateTime, user String, value Float64, status UInt8)`.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (LZ4) | **0.116 s** / 101.6 MB |
| client-v2 (POJO insert) | 0.121 s / 385.3 MB |
| jvm-clickhouse-native (Zstd) | 0.124 s / 82.7 MB |
| jvm-clickhouse-native (none) | 0.228 s¹ / **58.4 MB** |
| housepower (native) | 0.178 s / 770.5 MB |
| clickhouse-jdbc v1 (legacy, HTTP) | 0.257 s / 971.0 MB |
| clickhouse-jdbc v2 (HTTP) | 1.154 s² / 2.33 GB |

Against `client-v2` — the official driver's fastest insert path — single-client
wall clock on loopback is a tie; the durable wins are **~4× less allocation**
and lower server-side CPU (§ *Where the insert time goes*).

¹ One outlier iteration in this session (±2.4 s CI over 3 iterations); adjacent
runs measure ~0.11 s. ² The v2 JDBC batch path is a regression over both v1 and
its own `client-v2` underpinnings, and needs the `user` column backtick-quoted
to avoid an even slower client-side parser fallback (see `BulkInsertBenchmark`
javadoc).

## Wide bulk insert — 1M rows × 30 fixed-width columns (`WideBulkInsertBenchmark`)

`bench_wide`: 15 × `UInt64` + 15 × `Float64`, 240 raw B/row, no strings. Per-cell
costs (the server's row→column transpose, per-cell driver dispatch) scale with
column count, so this separates the drivers far more than the 5-column table.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (none) | 1.43 s³ / **16.3 MB** |
| jvm-clickhouse-native (LZ4) | 1.21 s / 378.2 MB |
| client-v2 (POJO insert) | 1.23 s / 1.92 GB |
| clickhouse-jdbc v1 (legacy, HTTP) | **0.97 s³** / 977.2 MB |
| housepower (native) | 1.58 s / 4.99 GB |
| clickhouse-jdbc v2 (HTTP) | 3.33 s / 3.97 GB |

³ Wall-clock CIs are wide at this payload size (±2 s over 3 iterations) — treat
time ordering among the top four as a tie; the allocation column (tight CIs) is
the reliable signal. On a pure-numeric schema our column buffers produce almost
no garbage: **16 MB vs 1.9–5.0 GB** for the row-API drivers (~120–300×).

## Where the insert time goes — client vs server (`InsertBreakdown`)

Not a JMH benchmark: a diagnostic that performs each 1M-row insert, then reads
that exact INSERT's server-side accounting from `system.query_log`. **Client
wall** is what the app sees; **server duration** / **server CPU** are the
server's own figures; **net recv wait** is time the server's query thread sat
idle waiting for the client to send bytes. `client wall ≈ server duration` means
production and ingestion overlap (streaming); `client wall ≫ server duration`
means the client serialises the whole body first (serialize-then-ship).

**Narrow (5 columns):**

| Variant | Client wall | Server duration | Server CPU | Net recv wait | Wall − server |
|---|---|---|---|---|---|
| ours (native, none) | 143 ms | 142 ms | **80 ms** | 80 ms | **1 ms** |
| ours (native, LZ4) | 154 ms | 153 ms | 85 ms | 73 ms | 1 ms |
| client-v2 (POJO) | 272 ms | 263 ms | 120 ms | 140 ms | 9 ms |
| jdbc v1 (legacy, HTTP) | 312 ms | 87 ms | 88 ms | 7 ms | 225 ms |
| jdbc v2 (HTTP) | 1,346 ms | 314 ms | 416 ms | 48 ms | 1,032 ms |
| housepower (native) | 345 ms | 341 ms | 101 ms | 183 ms | 4 ms |

**Wide (30 fixed-width columns):**

| Variant | Client wall | Server duration | Server CPU | Net recv wait | Wall − server |
|---|---|---|---|---|---|
| ours (native, none) | 866 ms | 865 ms | **569 ms** | 344 ms | **1 ms** |
| ours (native, LZ4) | 1,380 ms | 1,377 ms | 1,078 ms | 338 ms | 3 ms |
| client-v2 (POJO) | 1,501 ms | 1,488 ms | 777 ms | 856 ms | 13 ms |
| jdbc v1 (legacy, HTTP) | 1,372 ms | 687 ms | 674 ms | 54 ms | 685 ms |
| jdbc v2 (HTTP) | 5,169 ms | 1,310 ms | 1,559 ms | 148 ms | 3,859 ms |
| housepower (native) | 2,109 ms | 2,100 ms | 812 ms | 1,355 ms | 9 ms |

Two structural facts the wide table makes loud:

- **Native blocks cost the server less CPU.** 569 ms vs client-v2's 777 ms per 1M
  wide rows — a +37% row→column transpose tax for RowBinary (up from tens of ms
  on the narrow table), and +174% for v2 JDBC. This lands on the one resource you
  can't scale horizontally.
- **Native clients pipeline; HTTP JDBC stacks.** For our client `wall − server ≈
  1 ms` — production and ingestion fully overlap. The JDBC paths spend hundreds
  of ms (v2: thousands) serialising before the server sees a byte, so client and
  server time add instead of overlapping.

LZ4 is a **net loss on loopback** for wide numeric data (+500 ms server
decompression CPU for bytes that never cross a real network) — compression
defaults are a network-bandwidth trade, not a universal win.

## Kotlin Flow overhead — 1M-row read (`KotlinStreamingSelectBenchmark`, ours only)

| API | Time / Alloc |
|---|---|
| Java columnar blocks | **9.4 ms** / **16.0 MB** |
| Java `query(sql, Class)` | 27.9 ms / 144.0 MB |
| Kotlin `queryFlow` | 178.2 ms / 83.8 MB |

Per-row `Flow` emission has real overhead; for hot paths, read columnar blocks
and batch yourself.

## Reproducing

Requires Docker (Testcontainers spins the server; override with `-Dch.host`).

```bash
# Whole suite
./gradlew :benchmarks:jmh

# Build the runnable JMH jar, then target one benchmark with the gc profiler
./gradlew :benchmarks:jmhJar
java -jar benchmarks/build/libs/benchmarks-*-jmh.jar "BulkInsertBenchmark"      -p rows=1000000 -prof gc
java -jar benchmarks/build/libs/benchmarks-*-jmh.jar "WideBulkInsertBenchmark"  -p rows=1000000 -prof gc
java -jar benchmarks/build/libs/benchmarks-*-jmh.jar "StreamingSelectBenchmark" -p rows=1000000 -prof gc
java -jar benchmarks/build/libs/benchmarks-*-jmh.jar "MappedSelectBenchmark"    -p rows=1000000 -prof gc
java -jar benchmarks/build/libs/benchmarks-*-jmh.jar "StringSelectBenchmark"    -p rows=1000000 -prof gc

# Client-vs-server split (narrow + wide), reads system.query_log
java -cp benchmarks/build/libs/benchmarks-*-jmh.jar \
  io.github.danielbunting.clickhouse.bench.InsertBreakdown
```

## See also

- [README — Performance](../README.md#performance)
- [Cross-client compatibility](cross-client-compatibility.md) — correctness, not speed
- [Benchmark sources](../benchmarks/src/jmh/java/io/github/danielbunting/clickhouse/bench/)
