# Documentation

| Guide | Description |
|-------|-------------|
| [Quickstart](quickstart.md) | Connect, create a table, insert, and query in minutes |
| [Configuration](configuration.md) | Builder reference, `chnative://` URLs, endpoints/failover, compression, timeouts, settings |
| [Authentication & TLS](authentication.md) | Passwords, access tokens, TLS, mutual TLS |
| [Data types](data-types.md) | Every ClickHouse ↔ JVM type mapping, accessors, null handling, insert binding |
| [Bulk insert](bulk-insert.md) | High-throughput columnar writes: lifecycle, mapping, batch sizing, failure semantics |
| [Connection pooling](connection-pooling.md) | Concurrency, validation, self-healing, sizing |
| [Kotlin extensions](kotlin.md) | Coroutines, `Flow` streaming, `queryAs`, batched/block-granular flows, config DSL |
| [JDBC driver](jdbc.md) | `jdbc:chnative://` usage, prepared statements, batches, limitations |
| [ADBC driver](adbc.md) | Arrow Database Connectivity: results as Arrow `VectorSchemaRoot` batches, ingest, metadata, type mapping |
| [Cross-client compatibility](cross-client-compatibility.md) | Verified round-trip behavior against the official JDBC driver across ClickHouse 25.8–26.5 |
| [Performance comparison](performance-comparison.md) | Full benchmark matrix vs the official driver (v1/v2 JDBC + client-v2) and HousePower, with a client-vs-server time split |

Also see the [runnable samples](../samples/) and the [JMH benchmarks](../benchmarks/).
