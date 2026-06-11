# Cross-Client Compatibility Testing

The cross-client integration suite (`CrossClient*IT` in
`clickhouse-native-client/src/test/java/io/github/danielbunting/clickhouse/integration/`)
validates this library against the **official ClickHouse Java client**
(`com.clickhouse:clickhouse-jdbc` 0.9.0, built on client-v2, HTTP port 8123) as
the reference implementation. Every test round-trips anchored values through
both clients against the same live server (Testcontainers, version matrix
25.8/26.3/26.4/26.5):

- **Direction A** — official client inserts, native client reads: validates
  native **decode** against reference-produced data.
- **Direction B** — native `BulkInserter` inserts, official client reads:
  validates native **encode** as consumed by the reference client.
- **Neutral leg** — where one side cannot participate, raw
  `INSERT ... VALUES` literals (server parses; neither binary encoder
  involved), then **both** clients read and are asserted against the same
  anchors — still cross-validating the two decoders.

Scope note: both clients talk to the same server, which normalizes storage, so
the suite validates **semantic compatibility** (type mapping, precision,
timezone handling, null handling), not byte-for-byte wire equality. The
official Java client cannot speak the native TCP protocol at all.

Run with:

```bash
./gradlew :clickhouse-native-client:integrationTest --tests "*.integration.CrossClient*"
./gradlew :clickhouse-native-client:integrationTestAllVersions   # full 25.8–26.5 matrix
```

One structural fact frames several findings below: **the official JDBC driver
inserts bound parameters as SQL literal text**, not binary (its binary writer
exists but is gated behind an off-by-default beta property). So Direction A
inherently exercises the server's *text parsing* path, while the native
`BulkInserter` ships exact binary columns.

---

## Findings

Every failure the suite surfaced was investigated to attribution with a
direct-probe diagnostic before a workaround was added. None turned out to be a
bug in this library; each is pinned in code at the point a reader would trip
over it, and summarized here.

| # | Finding | Whose behavior | Test workaround |
|---|---------|----------------|-----------------|
| 1 | Float text parsing truncates extremes (1 ULP) and flushes denormal doubles to 0 | ClickHouse server (intentional default) | Extremes pinned via binary encode only; `@Disabled` test documents the text path |
| 2 | DateTime reads collapse ambiguous DST wall-clock times | clickhouse-jdbc 0.9.0 | Fall-back tests read DateTime as `toUnixTimestamp(dt)` on the official leg |
| 3 | Reading a Variant NULL row crashes | clickhouse-jdbc 0.9.0 | Official leg reads `WHERE v IS NOT NULL`; native leg asserts the NULL |
| 4 | JSON-as-string read mode hits EOF | clickhouse-jdbc 0.9.0 | Official leg uses the driver's binary-JSON Map mode |
| 5 | Bare NULL coerced to `Int64 0` in mixed-cast VALUES list (Dynamic column) | ClickHouse server VALUES inference | NULL kept in a cast-free INSERT statement |
| 6 | Time/Time64 and BFloat16 unreadable by the official driver | clickhouse-jdbc 0.9.0 (missing type support) | Official legs read `toString(t)` / `CAST(b AS Float32)` |
| 7 | Nested arrays fail the driver's `java.sql.Array` conversion | clickhouse-jdbc 0.9.0 | Typed `getObject(i, Object.class)` reads + `ArrayValue.asList()` normalization |

### 1. Server float text parsing is not correctly rounded

**Symptom.** Direction A insert of `-Float.MAX_VALUE` (`-3.4028235E38`) read
back as `-3.4028233E38` — exactly 1 ULP low. Follow-up probing showed the
smallest normal Float32 (`1.17549435E-38`) also stores 1 ULP low, and the
denormal Double `4.9E-324` flushes to `0.0`.

**How we validated.** A three-path diagnostic inserted the same value into the
same table through three channels, then read the raw stored bytes server-side
via `hex(reinterpretAsString(f32))`:

| Insert path | Stored bits (expected `0xFF7FFFFF`) |
|---|---|
| Official JDBC `PreparedStatement` | `0xFF7FFFFE` — 1 ULP low |
| Raw SQL literal `-3.4028235E38` (no Java client in the parse) | `0xFF7FFFFE` — same wrong bits |
| Native `BulkInserter` (binary) | `0xFF7FFFFF` — exact |

The raw-literal row is the attribution: the server's own text-to-float parse
produces the wrong bits with no client encoder involved. Both clients read all
three rows identically, so decode agreement holds everywhere.

We then confirmed the documented escape hatch on the same image
(`clickhouse-local`, 25.8): with `SETTINGS precise_float_parsing = 1` the same
literals parse bit-exact and the denormal survives as `5e-324`.

**Status.** Known, intentional upstream behavior (fast parser by default):
ClickHouse/ClickHouse [#1665](https://github.com/ClickHouse/ClickHouse/issues/1665),
[#4819](https://github.com/ClickHouse/ClickHouse/issues/4819),
[#17933](https://github.com/ClickHouse/ClickHouse/issues/17933) (which added
`precise_float_parsing`). Pinned by
`CrossClientScalarTypesIT.floatExtremesViaBinaryEncode` (passing, binary path)
and `floatExtremesViaTextInsertKnownServerBug` (`@Disabled`, documents the
text path and re-enable conditions).

### 2. Official driver collapses ambiguous DST times on DateTime reads

**Symptom.** `CrossClientTimezoneDstIT` inserts the load-bearing fall-back
pair: two **distinct instants** sharing the same local wall clock
(`2024-10-27T00:30:00Z` and `01:30:00Z` are both 01:30 in `Europe/London`).
The official driver returned the same instant for both rows.

**How we validated.** A probe table with the two epoch literals
(`1729989000`, `1729992600`) in `DateTime('Europe/London')` and
`DateTime64(3, 'Europe/London')` columns, read three ways through the
official driver:

| Read path | Row 1 (00:30Z) | Row 2 (01:30Z) |
|---|---|---|
| Typed `getObject(i, OffsetDateTime.class)`, DateTime | 00:30Z | **00:30Z** (collapsed, earlier offset) |
| Untyped `getObject(i)`, DateTime | **01:30Z** | 01:30Z (collapsed, later offset!) |
| Typed read, DateTime64 | 00:30Z | 01:30Z ✓ correct |
| Server-side `toUnixTimestamp(dt)` | 1729989000 | 1729992600 ✓ correct |

The epoch row proves storage is correct; the driver's DateTime read converts
through the local wall clock and loses the offset (and its typed vs untyped
paths even resolve the ambiguity in opposite directions). DateTime64 reads are
instant-based and unaffected. The native client returns correct distinct
`Instant`s for the same rows because it never round-trips through local time.

**Workaround.** The two fall-back tests (`fallBackOverlapLondon`,
`southernHemisphereAndHalfHourDst`) read the DateTime column as
`toUnixTimestamp(dt)` on the official leg; DateTime64 keeps the typed read.
Documented on the `roundTrip(..., ambiguousWallClock)` overload. Candidate for
an upstream report against `ClickHouse/clickhouse-java`.

### 3. Official driver crashes reading Variant NULL

**Symptom.** Direction B official read of `Variant(UInt32, String)` containing
a NULL row throws `IndexOutOfBoundsException: Index -1 out of bounds` inside
`BinaryStreamReader.readVariant` — the NULL discriminator (`0xFF`, read as
−1) is used directly as a type-list index.

**How we validated.** Native decode of the identical bulk-inserted rows
(including the NULL) passes immediately before the official read in the same
test, so the stored data is well-formed; the exception stack is entirely
inside the driver.

**Workaround.** The official leg in `CrossClientExperimentalTypesIT.variant()`
reads `WHERE v IS NOT NULL`; the native legs assert the NULL row. Candidate
for an upstream report.

### 4. Official driver JSON-as-string read mode is broken

**Symptom.** With the server setting
`output_format_binary_write_json_as_string = 1` (passed via the driver's own
`clickhouse_setting_` property prefix), reading a JSON column dies with
`EOFException` inside `BinaryStreamReader.readJsonData` — the server honors
the setting and sends strings, but the 0.9.0 reader still parses binary JSON.

**How we validated.** A probe read the same table both ways: without the
setting, the driver's binary-JSON mode works and returns
`LinkedHashMap` values (`{a=1, b=x}`, absent paths omitted); with the setting,
it fails as above. A server-side `toJSONString(j)` read also works.

**Workaround.** `CrossClientExperimentalTypesIT.json()` asserts the native
legs against reconstructed JSON strings and the official leg against
structurally equivalent Map anchors (binary-JSON mode).

### 5. Server VALUES inference coerces NULL inside a Dynamic column

**Symptom.** The neutral insert
`INSERT INTO t (id, d) VALUES (1, 42::Int64), (2, 'hello'::String), (3, NULL), (4, -1::Int64)`
into a `Dynamic` column stored row 3 as `Int64 0`, not `Dynamic NULL` — the
native client read back `0`, which initially looked like a native codec bug.

**How we validated.** Four INSERT shapes probed against `dynamicType(d)` (the
server's own per-row type report):

| VALUES list | NULL row stored as |
|---|---|
| `42::Int64, 'hello', NULL` (bare literals) | `None` (true NULL) ✓ |
| `42::Int64, 'hello'::String, NULL` | `Int64`, value 0 |
| `…, NULL, -1::Int64` | `Int64`, value 0 |
| `42::Int64, NULL, -1::Int64` | `Int64`, value 0 |

The server itself reports the NULL row as `Int64` whenever cast expressions
appear alongside it — VALUES type inference coerces the NULL through the cast
type before it reaches the Dynamic column. Both clients agree on what is
stored; nothing to fix client-side.

**Workaround.** `dynamic()` keeps the NULL in a cast-free INSERT statement,
with the quirk pinned in a comment.

### 6. Time/Time64 and BFloat16 are unreadable by the official driver

`ClickHouseDataType` in 0.9.0 has no `Time`/`Time64` constants (column-type
parsing throws before any value is read), and `BinaryStreamReader` has no
BFloat16 case. Inserts work (they are literal text), so Direction A is
unaffected. Direction B official legs read through server-side conversions
that still validate the native-encoded stored bytes:
`SELECT toString(t)` (parsed back to `Duration` by
`CrossClientRoundTripBase.parseClickHouseTime`) and
`SELECT CAST(b AS Float32)`.

### 7. Nested arrays surface the driver's internal `ArrayValue` carrier

`getObject()` on `Array(Array(T))` fails inside the driver
(`Failed to convert ... BinaryStreamReader$ArrayValue to java.sql.Array`), and
arrays nested inside tuples surface the raw `ArrayValue` object. Both are
handled in test infrastructure: such columns are read with the typed
`getObject(i, Object.class)` accessor (bypassing the broken conversion) and
`CrossClientRoundTripBase.normalizeOfficial` unwraps `ArrayValue.asList()`
recursively.

---

## Coverage matrix

Directions: **A** official→native, **B** native→official, **N** neutral
insert + both decoders.

| Family | Tests | Directions |
|---|---|---|
| Int8–64, UInt8–32, UInt64 (full range), Int/UInt 128/256 | `CrossClientScalarTypesIT` | A + B |
| Float32/64 ordinary | `CrossClientScalarTypesIT` | A + B |
| Float32/64 IEEE extremes | `floatExtremesViaBinaryEncode` | B (see finding 1) |
| Bool, String, UUID, IPv4/IPv6, Enum8/16 | `CrossClientScalarTypesIT` | A + B |
| Decimal(9,2)/(18,4)/(38,8)/(76,2) | `CrossClientDecimalTemporalIT` | A + B |
| Date, Date32, DateTime(tz), DateTime64(3/6/9) | `CrossClientDecimalTemporalIT` | A + B |
| Nullable, Array, Map, LowCardinality(±Nullable) | `CrossClientNestedTypesIT` | A + B |
| FixedString(±Nullable), SimpleAggregateFunction, Tuple, Array(Array) | `CrossClientCompositeTypesIT` | A + B |
| Nested | `CrossClientCompositeTypesIT` | A + N (no native Nested encode) |
| Geo Point | `CrossClientGeoTypesIT` | A + B |
| Geo Ring/Polygon/MultiPolygon | `CrossClientGeoTypesIT` | A + N (no native deep-geo encode) |
| Variant, Dynamic, JSON | `CrossClientExperimentalTypesIT` | A/N + B (findings 3–5) |
| Time, Time64(3/9), BFloat16 | `CrossClientExperimentalTypesIT` | A + B via casts (finding 6) |
| Timezones: DST gap/overlap, ±30/45-min offsets, ±14h date boundaries | `CrossClientTimezoneDstIT` | A + B (finding 2) |

Deferred: Interval (not a storable column type) and AggregateFunction states
(unreadable without `-Merge`) — tracked in the `CrossClientRoundTripBase`
Javadoc.
