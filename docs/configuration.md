# Configuration

`ClickHouseConfig` is the immutable configuration object every connection is opened from. Build it with the fluent builder, the Kotlin DSL, or parse it from a `chnative://` URL.

- [Builder reference](#builder-reference)
- [URL format](#url-format)
- [Kotlin DSL](#kotlin-dsl)
- [Multiple endpoints and failover](#multiple-endpoints-and-failover)
- [Compression](#compression)
- [Server settings](#server-settings)
- [Timeouts](#timeouts)

## Builder reference

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("ch.example.com")
    .port(9000)
    .database("analytics")
    .username("reader")
    .password("secret")
    .compression(CompressionMethod.LZ4)
    .connectTimeout(Duration.ofSeconds(10))
    .socketTimeout(Duration.ofSeconds(30))
    .setting("max_execution_time", "30")
    .build();
```

#### Kotlin

```kotlin
val config = clickHouseConfig {
    host = "ch.example.com"
    port = 9000
    database = "analytics"
    username = "reader"
    password = "secret"
    compression = CompressionMethod.LZ4
    connectTimeout = Duration.ofSeconds(10)
    socketTimeout = Duration.ofSeconds(30)
    settings["max_execution_time"] = "30"
}
```

All builder methods, with defaults:

| Method | Type | Default | Description |
|---|---|---|---|
| `host(String)` | `String` | `"localhost"` | Primary endpoint host |
| `port(int)` | `Int` | `9000` | Primary endpoint port (native TCP) |
| `endpoint(String, int)` | host + port | — | Adds a candidate endpoint; an explicit endpoint list takes precedence over `host`/`port` |
| `endpoints(List<Endpoint>)` | `List<Endpoint>` | — | Replaces the endpoint list |
| `loadBalancingPolicy(LoadBalancingPolicy)` | enum | `FIRST_ALIVE` | Endpoint ordering: `FIRST_ALIVE`, `ROUND_ROBIN`, `RANDOM` |
| `database(String)` | `String` | `"default"` | Default database |
| `username(String)` | `String` | `"default"` | Authentication username |
| `password(String)` | `String` | `""` | Plaintext password (empty is valid) |
| `accessToken(String)` | `String` | `null` | Bearer/JWT credential — see [authentication.md](authentication.md) |
| `clientCertPath(String)` / `clientKeyPath(String)` | `String` | `null` | PEM client certificate identity (mTLS) — both required together |
| `compression(CompressionMethod)` | enum | `LZ4` | Block compression: `NONE`, `LZ4`, `ZSTD` |
| `insertBatchSize(int)` | `Int` | `65536` | Rows buffered per block by `BulkInserter` before flushing |
| `connectTimeout(Duration)` | `Duration` | 10 s | TCP connect timeout |
| `socketTimeout(Duration)` | `Duration` | 30 s | Socket read/write timeout |
| `queryTimeout(Duration)` | `Duration` | `Duration.ZERO` (disabled) | Client-side deadline per operation |
| `tls(boolean)` | `Boolean` | `false` | Enable TLS (ClickHouse's TLS-native port is conventionally 9440 — set `port` accordingly) |
| `trustStorePath(Path)` / `trustStorePassword(String)` | `Path` / `String` | `null` | JKS/PKCS12 trust store for server certificate verification; platform default trust when unset |
| `keyStorePath(Path)` / `keyStorePassword(String)` | `Path` / `String` | `null` | JKS/PKCS12 key store holding the client certificate + key for mTLS transport |
| `verifyHostname(boolean)` | `Boolean` | `true` | RFC 2818 hostname verification against the server certificate |
| `insecureSkipVerify(boolean)` | `Boolean` | `false` | **Dev/test only** — trust any certificate, skip hostname verification |
| `setting(String, String)` / `settings(Map)` | — | — | Per-connection ClickHouse server settings, sent with every query |

`build()` validates the configuration — see [authentication.md](authentication.md) for the credential mutual-exclusion rules.

## URL format

`ClickHouseConfig.fromUrl(...)` parses a connection URL:

```
chnative://[user[:password]@]host[:port][,host2[:port2]...]/[database][?param=value&...]
```

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.fromUrl(
    "chnative://reader:secret@ch1.example.com:9000,ch2.example.com:9000/analytics"
        + "?compression=zstd&sslmode=strict&settings.max_threads=4");

// JDBC-style: Properties override URL credentials
Properties props = new Properties();
props.setProperty("user", "admin");
props.setProperty("password", "other");
ClickHouseConfig fromProps = ClickHouseConfig.fromUrl(url, props);
```

#### Kotlin

```kotlin
val config = ClickHouseConfig.fromUrl(
    "chnative://reader:secret@ch1.example.com:9000,ch2.example.com:9000/analytics" +
        "?compression=zstd&sslmode=strict&settings.max_threads=4")
```

Accepted query parameters (unrecognized parameters throw `ClickHouseException`):

| Parameter | Values | Maps to |
|---|---|---|
| `compression` | `lz4`, `zstd`, `none` | `compression(...)` |
| `insertBatchSize` | positive integer | `insertBatchSize(...)` |
| `connectTimeout` | seconds | `connectTimeout(...)` |
| `socketTimeout` | seconds | `socketTimeout(...)` |
| `queryTimeout` | seconds | `queryTimeout(...)` |
| `ssl` / `secure` | `true`/`false`, `1`/`0`, `yes`/`no` | `tls(...)` |
| `sslmode` | `none`/`disable` → TLS off; `strict`/`verify-full`/`require`/`true` → TLS on; `none-verify`/`insecure` → TLS on + skip verification | `tls(...)` + `insecureSkipVerify(...)` |
| `loadBalancingPolicy` | `first_alive`, `round_robin`, `random` | `loadBalancingPolicy(...)` |
| `settings.<name>` | any string | `setting(name, value)` — e.g. `settings.max_threads=4` |

The `fromUrl(url, Properties)` overload applies JDBC-style overrides: a `user`/`username` key replaces the URL username and a `password` key replaces the URL password.

## Kotlin DSL

The `-kotlin` module adds `clickHouseConfig {}` with mutable properties (`host`, `port`, `database`, `username`, `password`, `accessToken`, `compression`, `insertBatchSize`, `connectTimeout`, `socketTimeout`, `queryTimeout`, `tls`), a `settings` map, and an `endpoint(host, port)` function for multi-endpoint setups. Unset properties fall through to the builder defaults above.

```kotlin
val config = clickHouseConfig {
    endpoint("ch1.example.com", 9000)
    endpoint("ch2.example.com", 9000)
    database = "analytics"
    compression = CompressionMethod.ZSTD
}
val conn = config.connect()   // suspend; runs the handshake on Dispatchers.IO
```

## Multiple endpoints and failover

When more than one endpoint is configured, the driver tries them in the order produced by the `LoadBalancingPolicy`, failing over to the next endpoint on a connect-time `ConnectionException`:

- **`FIRST_ALIVE`** (default) — always start at the first endpoint; later endpoints are used only while earlier ones are down. Keeps all traffic on the primary.
- **`ROUND_ROBIN`** — rotate the starting endpoint across connection opens, spreading new connections over the cluster.
- **`RANDOM`** — pick a random starting endpoint per open.

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .endpoint("ch1.example.com", 9000)
    .endpoint("ch2.example.com", 9000)
    .endpoint("ch3.example.com", 9000)
    .loadBalancingPolicy(LoadBalancingPolicy.ROUND_ROBIN)
    .build();
```

## Compression

Blocks are compressed on the wire with CityHash128 checksums. `LZ4` is the default and the usual best choice; `ZSTD` trades CPU for a better ratio (useful over slow links); `NONE` skips compression entirely (fastest on loopback). See the [benchmarks](../benchmarks/) for measured trade-offs.

## Server settings

Settings configured on the connection are sent in the settings slot of every query packet. Per-call settings override them key-by-key:

#### Java

```java
// connection-wide
ClickHouseConfig config = ClickHouseConfig.builder()
    .setting("max_execution_time", "30")
    .build();

// per query
conn.execute("INSERT INTO t SELECT * FROM staging",
    Map.of("max_insert_threads", "4"));
try (QueryResult r = conn.query("SELECT ...", Map.of("max_threads", "8"))) { ... }
```

#### Kotlin

```kotlin
val config = clickHouseConfig {
    settings["max_execution_time"] = "30"
}

conn.command("INSERT INTO t SELECT * FROM staging",
    settings = mapOf("max_insert_threads" to "4"))
```

Common settings: `max_execution_time`, `max_memory_usage`, `max_threads`, `async_insert`.

## Timeouts

- `connectTimeout` — TCP connect + handshake.
- `socketTimeout` — individual socket reads/writes; a stalled server surfaces as an exception rather than a hang.
- `queryTimeout` — client-side deadline for a whole operation (`Duration.ZERO` disables it). This is independent from the server-side `max_execution_time` setting; use both for defense in depth.

## See also

- [Authentication & TLS](authentication.md)
- [Connection pooling](connection-pooling.md)
- [Quickstart](quickstart.md)
