# JDBC Driver

`clickhouse-native-client-jdbc` is a JDBC 4.3 driver over the native client — standard `java.sql` API, native TCP transport underneath. It is forward-only and read-optimized, and honest about what ClickHouse doesn't support (no transactions, no updatable result sets).

- [URL and registration](#url-and-registration)
- [Queries](#queries)
- [Prepared statements](#prepared-statements)
- [Batch inserts](#batch-inserts)
- [Metadata](#metadata)
- [Pools and ORMs](#pools-and-orms)
- [Limitations](#limitations)

## URL and registration

```
jdbc:chnative://[user[:password]@]host[:port][,host2[:port2]...]/[database][?param=value&...]
```

The driver (`io.github.danielbunting.clickhouse.jdbc.ClickHouseDriver`) registers itself via `META-INF/services` — no `Class.forName()` needed. Everything after `jdbc:` is the native [`chnative://` URL](configuration.md#url-format), so all its query parameters (compression, TLS, timeouts, `settings.*`, …) work here too.

```java
Properties props = new Properties();
props.setProperty("user", "default");
props.setProperty("password", "");

try (Connection conn = DriverManager.getConnection(
        "jdbc:chnative://localhost:9000/default?compression=lz4", props)) {
    // ...
}
```

`user`/`username` and `password` keys in `Properties` override credentials in the URL.

## Queries

```java
try (Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery("SELECT id, name, score FROM events ORDER BY id")) {
    while (rs.next()) {
        long id      = rs.getLong("id");
        String name  = rs.getString("name");
        double score = rs.getDouble("score");
    }
}
```

- All the standard accessors are implemented: `getString`, `getBoolean`, `getByte/Short/Int/Long`, `getFloat/Double`, `getBigDecimal`, `getBytes`, `getDate`, `getTimestamp`, `getObject` (raw boxed value, e.g. `java.util.List` for `Array(T)`), and `getObject(col, Class<T>)` with coercion.
- Integer- and float-backed columns are read through an **unboxed fast path** (`column.longAt`/`doubleAt`) — `getLong`/`getDouble` in a tight loop do not allocate.
- `wasNull()` works as specified; `findColumn` is case-insensitive.
- `Statement.cancel()` sends a real Cancel packet to the server for the in-flight query.

## Prepared statements

```java
try (PreparedStatement ps = conn.prepareStatement(
        "SELECT * FROM events WHERE score > ? AND name = ?")) {
    ps.setDouble(1, 8.0);
    ps.setString(2, "alpha");
    try (ResultSet rs = ps.executeQuery()) { /* ... */ }
}
```

`?` placeholders bind in one of two modes, chosen by the `server_side_params` connection property:

| Mode | Behavior |
|---|---|
| **Client-side interpolation** (default) | Each `?` is replaced with a safely quoted/escaped SQL literal before the query is sent. Placeholders inside string literals are left alone. |
| **Server-side parameters** (`server_side_params=true`) | Each `?` is rewritten to a named `{_pN:String}` placeholder and the values travel in the query packet; the server casts the textual form to the column type. |

Supported setters: `setNull`, `setBoolean`, `setByte/Short/Int/Long`, `setFloat/Double`, `setBigDecimal`, `setString`/`setNString`, `setBytes`, `setDate`/`setTime`/`setTimestamp` (with or without `Calendar`), `setObject`, `setURL`. Stream and LOB setters (`setAsciiStream`, `setBlob`, `setClob`, `setArray`, …) throw `SQLFeatureNotSupportedException`.

## Batch inserts

```java
try (PreparedStatement ps = conn.prepareStatement(
        "INSERT INTO events (id, name, score) VALUES (?, ?, ?)")) {
    for (Event e : events) {
        ps.setLong(1, e.id());
        ps.setString(2, e.name());
        ps.setDouble(3, e.score());
        ps.addBatch();
    }
    ps.executeBatch();
}
```

In the default client-side mode, a batched INSERT is **collapsed into a single multi-row `INSERT ... VALUES (...),(...),...`** — one round trip. For maximum ingestion throughput, prefer the native [`BulkInserter`](bulk-insert.md), which skips SQL entirely.

## Metadata

- `DatabaseMetaData.getTables` / `getColumns` query `system.tables` / `system.columns`; `getCatalogs` lists databases; `getTableTypes` returns `TABLE`/`VIEW`; `getTypeInfo` describes the major ClickHouse types.
- `ResultSetMetaData` reports the raw ClickHouse type string (`getColumnTypeName`, e.g. `"Nullable(UInt32)"`), the mapped `java.sql.Types` code, nullability, signedness, precision/scale.
- ClickHouse has no schema layer: `getSchemas` returns an empty result and databases surface as JDBC **catalogs**. `getPrimaryKeys` is empty (no enforced PK constraints).

Type mapping highlights: `String`/`FixedString`/`Enum*` → `VARCHAR`, `Int32` → `INTEGER`, `UInt32`/`Int64`/`UInt64` → `BIGINT`, `Int128+` and `Decimal*` → `DECIMAL`, `Float32` → `REAL`, `Float64` → `DOUBLE`, `Date*` → `DATE`, `DateTime*` → `TIMESTAMP`, `Array(...)` → `ARRAY`, `UUID` → `OTHER`. `Nullable(...)`/`LowCardinality(...)` wrappers are unwrapped recursively.

## Pools and ORMs

- `Connection.isValid(timeout)` runs `SELECT 1` — works with HikariCP-style validation.
- `setAutoCommit`/`getAutoCommit` are accepted for compatibility; autocommit is effectively always on (statements execute eagerly), and `commit()`/`rollback()` are no-ops.
- `supportsBatchUpdates()` is `true`; `jdbcCompliant()` is honestly `false` (ClickHouse lacks full entry-level SQL92).
- One connection = one in-flight statement (the core contract); use a pool for concurrency.

## Limitations

Things that throw `SQLFeatureNotSupportedException` (by design — ClickHouse has no equivalent):

- Transactions: savepoints, isolation levels other than `TRANSACTION_NONE`.
- Stored procedures (`prepareCall`), generated keys, named cursors.
- Scrollable or updatable result sets — only `TYPE_FORWARD_ONLY` + `CONCUR_READ_ONLY`.
- LOB/stream accessors (`getBlob`, `getClob`, `getBinaryStream`, …) and `getArray()` (use `getObject`, which returns a `java.util.List`).
- Catalog calls with no ClickHouse counterpart: `getImportedKeys`, `getIndexInfo`, `getProcedures`, etc. (query the `system` tables directly instead).

## See also

- [Configuration / URL parameters](configuration.md#url-format)
- [Bulk insert](bulk-insert.md) — the faster native write path
- [Data types](data-types.md)
