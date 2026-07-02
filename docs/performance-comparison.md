# Performance Comparison

Full benchmark matrix comparing **jvm-clickhouse-native** against the official
[ClickHouse Java client](https://github.com/ClickHouse/clickhouse-java) `0.9.0` —
in both its **v2 JDBC** driver and its idiomatic **`client-v2`** API, plus the
legacy **v1** JDBC driver where it is still instructive — and the community
[HousePower `clickhouse-native-jdbc`](https://github.com/housepower/ClickHouse-Native-JDBC)
(native TCP).

Each row shows **time / allocation** (average wall-clock from JMH `AverageTime`
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
| jvm-clickhouse-native | built from `DB/further-test-porting`, July 2026 (post bug-fix pass, bugs 35–47) |
| `com.clickhouse:clickhouse-jdbc` | `0.9.0` — v2 driver (`ClickHouseDriver`, HTTP 8123); legacy v1 via the bundled `DriverV1` |
| `com.clickhouse:client-v2` | `0.9.0` (HTTP 8123) — idiomatic non-JDBC API |
| `com.github.housepower:clickhouse-native-jdbc` | `2.7.1` (native TCP, port 9000) |
| JMH config | `Fork=1`, `Warmup=2`, `Measurement=3`, `-prof gc`; 1M-row table |

All numbers are a single July 2026 session. Docker-on-macOS wall clock is noisy
(see the per-section caveats) and shifts ±30% between sessions even for
unchanged code; treat the **ratios and the allocation column** as the durable
findings, not absolute milliseconds.

## Streaming reads — `id + value` (`StreamingSelectBenchmark`)

Fully consuming `SELECT id, value FROM bench` (1M rows), summing primitives with
no boxing — measures wire/decode throughput and allocation.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (columnar blocks) | **14.0 ms** / **15.3 MB** |
| client-v2 (binary reader) | 47.7 ms / 249.3 MB |
| clickhouse-jdbc v2 (HTTP) | 53.1 ms / 249.4 MB |
| housepower (native) | 59.5 ms / 366.5 MB |

The headline is **16× less allocation** (249.3 → 15.3 MB): v2's binary reader
deserialises every cell into a boxed `Object[]` per row (primitives included) and
hands back a per-row wrapper object, so the positional getters read from that
boxed array rather than avoiding the per-cell boxing — while our columnar path
touches only the primitive backing arrays. That allocation gap *is* the ~3.4×
speed gap — the boxing and the GC pressure it creates are client-side decode
costs, not a loopback artifact, so unlike the *insert* wall-clock numbers (where
removing the network flattens single-client time to a tie) this read speedup is a
real, structural difference.

## Materialised typed reads — 1M rows → objects (`MappedSelectBenchmark`)

Each driver builds one `record (long id, double value)` per row, so all produce
the same materialised objects.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (`query(sql, Class)`) | **28.8 ms** / **137.3 MB** |
| client-v2 (binary reader → record) | 53.0 ms / 279.9 MB |
| clickhouse-jdbc v2 (HTTP) | 54.7 ms / 279.9 MB |
| housepower (native) | 60.9 ms / 397.0 MB |

## String-heavy reads — 1M rows × 8 wide String columns (`StringSelectBenchmark`)

Lazy String materialisation. `all` reads every cell of all 8 columns;
`projected` reads only 1 of the 8 (the unread columns are never decoded into
`String`).

| Library | All 8 columns | Projected (1 of 8) |
|---|---|---|
| jvm-clickhouse-native | 279.7 ms / 1.51 GB | 34.8 ms / 188.4 MB |
| client-v2 (binary reader) | 239.2 ms / **0.78 GB** | 32.4 ms / 168.8 MB |
| clickhouse-jdbc v2 (HTTP) | 254.3 ms / **0.78 GB** | 32.7 ms / **168.7 MB** |
| housepower (native) | **231.9 ms** / 2.31 GB | **31.3 ms** / 289.0 MB |

This is the one matrix row the official stack wins. Full materialisation of
every String cell costs ~1.9× the official driver's allocation (1.51 GB vs
0.78 GB) and, this session, ~10–20% wall clock against the field (June measured
the same shape as a statistical time-tie — treat the time delta as session
noise around parity, and the allocation column as the durable loss). Lazy
String decode pays off only when you skip cells; the full-materialisation path
is being worked on. Projection is a near-tie on time, with the unread 7 columns
never decoded.

## Bulk insert — 1M rows × 5 columns (`BulkInsertBenchmark`)

`(id UInt64, ts DateTime, user String, value Float64, status UInt8)`.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (none) | **0.107 s** / **59.7 MB** |
| jvm-clickhouse-native (LZ4) | 0.120 s / 97.0 MB |
| client-v2 (POJO insert) | 0.121 s / 337.0 MB |
| jvm-clickhouse-native (Zstd) | 0.124 s / 78.9 MB |
| housepower (native) | 0.180 s / 734.9 MB |
| clickhouse-jdbc v1 (legacy, HTTP) | 0.250 s / 926.1 MB |
| clickhouse-jdbc v2 (HTTP) | 1.029 s¹ / 2.24 GB |

Against `client-v2` — the official driver's fastest insert path — the
compressed variants are a single-client wall-clock tie and the uncompressed
variant leads outright; the durable wins are **~3.5–5.6× less allocation** and
lower server-side CPU (§ *Where the insert time goes*).

¹ The v2 JDBC batch path is a regression over both v1 and its own `client-v2`
underpinnings, and needs the `user` column backtick-quoted to avoid an even
slower client-side parser fallback (see `BulkInsertBenchmark` javadoc).

## Wide bulk insert — 1M rows × 30 fixed-width columns (`WideBulkInsertBenchmark`)

`bench_wide`: 15 × `UInt64` + 15 × `Float64`, 240 raw B/row, no strings. Per-cell
costs (the server's row→column transpose, per-cell driver dispatch) scale with
column count, so this separates the drivers far more than the 5-column table.

| Library | Time / Alloc |
|---|---|
| **jvm-clickhouse-native** (none) | **0.62 s** / **47.5 MB** |
| clickhouse-jdbc v1 (legacy, HTTP) | 0.99 s / 932.0 MB |
| jvm-clickhouse-native (LZ4) | 1.02 s² / 360.7 MB |
| client-v2 (POJO insert) | 1.17 s / 1.83 GB |
| housepower (native) | 1.37 s / 4.76 GB |
| clickhouse-jdbc v2 (HTTP) | 3.06 s² / 3.79 GB |

² Wall-clock CIs are wide at this payload size (±0.6–1.8 s over 3 iterations
for the LZ4 and v2-JDBC variants) — treat time ordering in the middle of the
table as approximate; the allocation column (tight CIs) is the reliable signal.
On a pure-numeric schema our column buffers produce almost no garbage:
**47.5 MB vs 0.9–4.8 GB** for the row-API drivers (~20–100×).

## JDBC layer head-to-head — 200k rows (`JdbcInsertBenchmark`, `JdbcSelectBenchmark`)

Same JDBC API on both sides — ours over native TCP, the official v2 driver over
HTTP — so this measures what an unmodified JDBC application actually feels.

| Scenario | jvm-clickhouse-native | clickhouse-jdbc v2 |
|---|---|---|
| Batch `INSERT` (`PreparedStatement.executeBatch`) | **119.2 ms** / **367.7 MB** | 187.7 ms / 430.9 MB |
| `getString` scan | **21.0 ms** / 185.3 MB | 31.6 ms / **86.9 MB** |
| `getLong` over UInt64 | **3.6 ms** / **4.2 MB** | 9.3 ms / 35.3 MB |
| `getObject` over UInt64 | **4.2 ms** / **21.0 MB** | 8.5 ms / 35.3 MB |

Ours is 1.5–2.6× faster on every pairing. One caveat mirrors the string-heavy
section above: the `getString` scan allocates ~2.1× more than the official
driver (every accessed cell materialises a `String` from the columnar buffer) —
the primitive accessors show the opposite, up to **8× less** allocation.

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
| ours (native, none) | 121 ms | 120 ms | **58 ms** | 68 ms | **1 ms** |
| ours (native, LZ4) | 164 ms | 162 ms | 109 ms | 62 ms | 2 ms |
| client-v2 (POJO) | 137 ms | 133 ms | 70 ms | 72 ms | 4 ms |
| jdbc v1 (legacy, HTTP) | 286 ms | 78 ms | 79 ms | 4 ms | 208 ms |
| jdbc v2 (HTTP) | 1,056 ms | 266 ms | 308 ms | 46 ms | 790 ms |
| housepower (native) | 345 ms | 341 ms | 81 ms | 266 ms | 4 ms |

**Wide (30 fixed-width columns):**

| Variant | Client wall | Server duration | Server CPU | Net recv wait | Wall − server |
|---|---|---|---|---|---|
| ours (native, none) | 745 ms | 743 ms | **540 ms** | 245 ms | **2 ms** |
| ours (native, LZ4) | 1,309 ms | 1,307 ms | 823 ms | 522 ms | 2 ms |
| client-v2 (POJO) | 1,440 ms | 1,426 ms | 729 ms | 837 ms | 14 ms |
| jdbc v1 (legacy, HTTP) | 1,539 ms | 836 ms | 814 ms | 59 ms | 703 ms |
| jdbc v2 (HTTP) | 3,972 ms | 1,082 ms | 1,414 ms | 73 ms | 2,890 ms |
| housepower (native) | 1,656 ms | 1,645 ms | 647 ms | 1,071 ms | 11 ms |

Two structural facts the wide table makes loud:

- **Native blocks cost the server less CPU.** 540 ms vs client-v2's 729 ms per 1M
  wide rows — a +35% row→column transpose tax for RowBinary (up from ~20% on the
  narrow table), and +162% for v2 JDBC. This lands on the one resource you
  can't scale horizontally.
- **Native clients pipeline; HTTP JDBC stacks.** For our client `wall − server ≈
  2 ms` — production and ingestion fully overlap. The JDBC paths spend hundreds
  of ms (v2: thousands) serialising before the server sees a byte, so client and
  server time add instead of overlapping.

LZ4 is a **net loss on loopback** for wide numeric data (+~280 ms server
decompression CPU for bytes that never cross a real network) — compression
defaults are a network-bandwidth trade, not a universal win.

## Kotlin Flow overhead — 1M-row read (`KotlinStreamingSelectBenchmark`, ours only)

| API | Time / Alloc |
|---|---|
| Java columnar blocks | **13.2 ms** / **15.3 MB** |
| Kotlin `queryBatched` (100k batches) | 19.3 ms / 72.9 MB |
| Java `query(sql, Class)` | 28.6 ms / 137.4 MB |
| Kotlin `queryFlow` (per-row) | 181.8 ms / 79.3 MB |

Per-row `Flow` overhead is almost entirely the per-element `flowOn` channel
handoff, not decoding or allocation (the per-row lane allocates *less* than the Java
mapped `Stream`, yet runs 6× slower). `queryBatched(sql, batchSize) { … }` emits
fixed-size `List<T>` chunks (partial final chunk), crossing that channel once per
batch instead of once per row — **~9× faster** than per-row `queryFlow` at 100k.
For raw columnar throughput, `queryBlocks(sql)` emits whole blocks.

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
