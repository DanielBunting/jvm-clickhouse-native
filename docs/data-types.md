# Data Types

Complete reference for how ClickHouse types map to JVM types in `clickhouse-native-client`.

Values read from a [`QueryResult`](../clickhouse-native-client/src/main/kotlin/io/github/danielbunting/clickhouse/QueryResult.kt) block via `column.value(row)` are returned as the boxed JVM type listed below. The same JVM types (plus the documented widening alternatives) are accepted when writing through a `BulkInserter` or the reflection-based row mappers.

- [Integers](#integers)
- [Floating point](#floating-point)
- [Decimal and Bool](#decimal-and-bool)
- [Strings](#strings)
- [UUID and IP addresses](#uuid-and-ip-addresses)
- [Date and time](#date-and-time)
- [Intervals](#intervals)
- [Enums](#enums)
- [Composite types](#composite-types)
- [Geo types](#geo-types)
- [Variant, Dynamic and JSON](#variant-dynamic-and-json)
- [Unsupported types](#unsupported-types)
- [Column accessors](#column-accessors)
- [Null handling](#null-handling)
- [Insert binding](#insert-binding)

## Integers

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Int8` | `Byte` | Backed by `byte[]` |
| `Int16` | `Short` | Backed by `short[]` |
| `Int32` | `Integer` | Backed by `int[]` |
| `Int64` | `Long` | Backed by `long[]` |
| `Int128` | `BigInteger` | Two's-complement, range ±2¹²⁷ |
| `Int256` | `BigInteger` | Two's-complement, range ±2²⁵⁵ |
| `UInt8` | `Integer` | Widened to avoid sign artifacts; range [0, 255] |
| `UInt16` | `Integer` | Widened; range [0, 65 535] |
| `UInt32` | `Long` | Widened; range [0, 2³²−1] |
| `UInt64` | `Long` | **Raw 64-bit pattern** — values above `Long.MAX_VALUE` read as negative; use `Long.toUnsignedString(value)` for the decimal form |
| `UInt128` | `BigInteger` | Unsigned, range [0, 2¹²⁸−1] |
| `UInt256` | `BigInteger` | Unsigned, range [0, 2²⁵⁶−1] |

## Floating point

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Float32` | `Float` | IEEE 754 single precision |
| `Float64` | `Double` | IEEE 754 double precision |
| `BFloat16` | `Float` | High 16 bits of a float32; widened to `Float` on read |

## Decimal and Bool

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Decimal(P, S)` | `BigDecimal` | P ∈ [1, 76], S ∈ [0, P]. Scale preserved on read; inserts are rescaled with `HALF_UP` rounding |
| `Bool` | `Boolean` | One byte per row on the wire |

## Strings

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `String` | `String` | **Lazy UTF-8 decode** — bytes stay in a column-owned buffer; each cell is decoded (and cached) only when accessed |
| `FixedString(N)` | `String` | NUL-padded to N bytes on the wire; trailing zero bytes stripped on read |

## UUID and IP addresses

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `UUID` | `java.util.UUID` | |
| `IPv4` | `java.net.Inet4Address` | Stored as UInt32 |
| `IPv6` | `java.net.Inet6Address` | 16 raw bytes, network order; IPv4-mapped addresses use the `::ffff:a.b.c.d` form |

## Date and time

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Date` | `java.time.LocalDate` | UInt16 day offset from 1970-01-01 |
| `Date32` | `java.time.LocalDate` | Signed day offset; supports pre-1970 dates |
| `DateTime` / `DateTime(tz)` | `java.time.Instant` | Epoch seconds. The column's timezone (or UTC when unspecified) is available from the codec for building zoned views — the `Instant` itself is zone-independent |
| `DateTime64(P [, tz])` | `java.time.Instant` | P ∈ [0, 9] (3 = ms, 6 = µs, 9 = ns). Sub-tick nanoseconds are truncated toward zero on insert |
| `Time` | `java.time.Duration` | Signed seconds within a day |
| `Time64(P)` | `java.time.Duration` | P ∈ [0, 9]; sub-tick precision truncated on insert |

## Intervals

Interval columns appear in query results (e.g. `SELECT INTERVAL 1 DAY`) but are not storable in tables.

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `IntervalNanosecond` … `IntervalWeek` | `java.time.Duration` | Fixed-length units (ns, µs, ms, s, min, h, day, week) |
| `IntervalMonth`, `IntervalQuarter`, `IntervalYear` | `java.time.Period` | Calendar units with no fixed duration |

## Enums

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Enum8` | `String` | Reads return the enum **name**; inserts accept the name (`String`) or the ordinal (`Number`) |
| `Enum16` | `String` | Same contract as `Enum8` |

## Composite types

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Array(T)` | `java.util.List` | One `List` per row; nests recursively (`Array(Array(T))` → `List<List<…>>`) |
| `Tuple(T1, T2, …)` | `java.util.List` | Heterogeneous, fixed arity. Named tuples (`Tuple(a T1, b T2)`) parse but read as a positional `List` |
| `Map(K, V)` | `java.util.LinkedHashMap` | Insertion order preserved; wire layout is `Array(Tuple(K, V))` |
| `Nullable(T)` | `T` or `null` | Null map kept alongside values; see [Null handling](#null-handling) |
| `LowCardinality(T)` | same as `T` | Transparent — dictionary encoding is a wire-level detail. `LowCardinality(Nullable(T))` supported |
| `SimpleAggregateFunction(f, T)` | same as `T` | Wire format identical to `T`; the function name is parsed and ignored |
| `Nothing` | `null` | Only appears as `Array(Nothing)` for empty array literals |

## Geo types

Geo types decode as their underlying tuple/array structure with `Float64` coordinates.

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Point` | `List<Double>` | Two elements: x, y |
| `Ring`, `LineString` | `List<List<Double>>` | Array of points |
| `Polygon`, `MultiLineString` | `List<List<List<Double>>>` | Array of rings/linestrings |
| `MultiPolygon` | `List<List<List<List<Double>>>>` | Array of polygons |

## Variant, Dynamic and JSON

| ClickHouse Type | JVM Type | Notes |
|---|---|---|
| `Variant(T1, T2, …)` | active member's JVM type, or `null` | Up to 254 member types. On insert, the value's Java type selects the member (server's canonical sorted order) |
| `Dynamic` | active member's JVM type, or `null` | Member types discovered per block at read time. Inserts accept `Long`/`Integer`/`Short`/`Byte`/`Double`/`Float`/`Boolean`/`CharSequence`, with the type inferred from the runtime class |
| `JSON` | `String` (flat JSON object) | Paths are discovered at read time and reconstructed into a JSON string; flat scalar paths only. Inserts accept a flat JSON object string |

See [cross-client-compatibility.md](cross-client-compatibility.md) for round-trip behavior of `Variant`/`Dynamic`/`JSON` against the official drivers — there are several caveats (e.g. Variant NULL representation, JSON read modes).

## Unsupported types

- **`AggregateFunction(f, T)`** is rejected at type-parse time: its wire format carries opaque, function-specific state with no length framing, so generic decoding is impossible. Finalize the state in SQL instead — e.g. `SELECT sumMerge(col)` — or use `SimpleAggregateFunction`.

## Column accessors

Each `Block` column exposes both boxed and unboxed access:

#### Java

```java
try (QueryResult result = conn.query("SELECT id, name, score FROM events")) {
    Iterator<Block> blocks = result.blocks();
    while (blocks.hasNext()) {
        Block block = blocks.next();
        for (int row = 0; row < block.rowCount(); row++) {
            Object id     = block.column(0).value(row);     // boxed, null-safe
            long rawId    = block.column(0).longAt(row);    // primitive, no boxing
            String name   = block.column(1).stringAt(row);  // lazy UTF-8 decode
            double score  = block.column(2).doubleAt(row);  // primitive, no boxing
        }
    }
}
```

#### Kotlin

```kotlin
conn.queryFlow("SELECT id, name, score FROM events").collect { row ->
    val id: Long = row.long("id")
    val name: String? = row.string("name")
    val score: Double = row.double("score")
}
```

| Accessor | Returns | Null behavior |
|---|---|---|
| `value(row)` | boxed value | returns `null` for SQL NULL |
| `longAt(row)` | primitive `long` | **throws `NullPointerException`** on SQL NULL |
| `doubleAt(row)` | primitive `double` | **throws `NullPointerException`** on SQL NULL |
| `stringAt(row)` | `String` | returns `null` for SQL NULL; lazy decode for `String` columns, `value().toString()` otherwise |
| `values()` | raw backing array (`long[]`, `int[]`, `Object[]`, …) | cast before use; ignores the null map |
| `nulls()` | `boolean[]` null map, or `null` for non-nullable columns | `true` = SQL NULL at that row |

The numeric value from `longAt` matches `value()` semantics: day offset for `Date`, epoch seconds for `DateTime`, raw bits for `UInt64`, and so on.

## Null handling

- `Nullable(T)` columns keep a null map parallel to the values; `value(row)` and `stringAt(row)` check it automatically and return `null`.
- Primitive accessors (`longAt`, `doubleAt`) cannot represent SQL NULL and throw `NullPointerException` — guard with `nulls()` or use `value()`.
- When mapping to objects (`conn.query(sql, MyType.class)` / `queryAs<T>`), SQL NULL binding into a non-nullable parameter throws `ClickHouseException`; nullable Kotlin parameters and reference-typed Java fields accept `null`.

## Insert binding

`BulkInserter.add(...)`, the row mappers, and `column.set(...)` accept the read-side JVM type plus convenient widenings:

| ClickHouse Type | Accepted insert inputs |
|---|---|
| Integer types | any `Number` (range-checked); `UInt64` also accepts `BigInteger` in [0, 2⁶⁴−1]; Int128/256 and UInt128/256 also accept numeric `String` |
| `Float32/64`, `BFloat16` | any `Number` |
| `Bool` | `Boolean`, `Number` (non-zero = true), `String` (`"true"`/`"false"`) |
| `String` / `FixedString(N)` | `String`, `CharSequence` (FixedString is padded/validated to N bytes) |
| `UUID` | `UUID`, `String` |
| `Date` / `Date32` | `LocalDate`, `Number` (day offset); `Date32` also accepts ISO `String` |
| `DateTime` / `DateTime64(P)` | `Instant` (sub-precision truncated toward zero) |
| `Time` / `Time64(P)` | `Duration`, `Number` (seconds), `String` (`"HH:MM:SS[.fraction]"`) |
| `Decimal(P, S)` | `BigDecimal` (rescaled `HALF_UP`), `Number`, `String` |
| `Enum8/16` | `String` name or `Number` ordinal |
| `IPv4` / `IPv6` | `InetAddress`, `String`; `IPv4` also `Number`, `IPv6` also `byte[16]` |
| `Array(T)` | `java.util.List` |
| `Tuple(...)` | `java.util.List` or `Object[]` |
| `Map(K, V)` | `java.util.Map` |
| `Nullable(T)` | `null` or any input `T` accepts |
| `Variant(...)` | a value whose Java type matches a member type, or `null` |
| `Dynamic` | `Long`/`Integer`/`Short`/`Byte`/`Double`/`Float`/`Boolean`/`CharSequence` |
| `JSON` | flat JSON object `String` |

One asymmetry to know about: `Decimal` inserts round (`HALF_UP`), while `DateTime64`/`Time64` inserts truncate sub-precision toward zero.

## See also

- [Cross-client compatibility](cross-client-compatibility.md) — verified round-trip behavior against the official JDBC driver across ClickHouse 25.8–26.5
- [Bulk insert](bulk-insert.md) — how typed rows map onto columns
- [Quickstart](quickstart.md)
