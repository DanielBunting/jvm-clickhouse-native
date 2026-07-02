# Contributing

## Prerequisites

- JDK 17 (the build uses Gradle toolchains, so any 17+ launcher JDK works)
- Docker, for the integration tests (Testcontainers spins up real ClickHouse
  servers)

## Building and testing

```bash
./gradlew build                     # compile + unit tests (no Docker needed)
./gradlew integrationTest           # integration suite against the default ClickHouse image
./gradlew integrationTestAllVersions  # same suite against every supported ClickHouse version
./gradlew :benchmarks:jmh           # JMH benchmarks vs the official and housepower clients
./gradlew jacocoTestReport          # unit-test coverage (HTML + XML under each module's build/reports/jacoco)
```

Unit-test coverage is uploaded to [Codecov](https://codecov.io/gh/DanielBunting/jvm-clickhouse-native)
on every CI run; integration tests don't count toward it.

Integration tests are JUnit tests tagged `integration`; the plain `test` task
excludes them so the default build stays fast and offline. The supported
ClickHouse version matrix has a single source of truth:
`ClickHouseImages.SUPPORTED_VERSIONS` in
[`clickhouse-native-client/src/testFixtures`](clickhouse-native-client/src/testFixtures/java/io/github/danielbunting/clickhouse/test/ClickHouseImages.java)
— the per-version Gradle tasks are generated from it.

The benchmarks also need Docker (the harness starts ClickHouse via
Testcontainers) and benchmark against a real server, so expect a run to take a
while; point them at an existing server with `-Dch.host=...` to skip the
container. They include the GC profiler, so results report allocation
rate/bytes-per-op alongside throughput. Benchmarks are not run in CI — they're
a local tool for validating performance claims.

## Module layout

| Module | What it is |
|---|---|
| `clickhouse-native-client` | core client (native TCP protocol, column-major data plane) |
| `clickhouse-native-client-jdbc` | JDBC driver on top of the core client |
| `clickhouse-native-client-kotlin` | coroutine-native Kotlin surface |
| `clickhouse-native-client-adbc` | ADBC (Arrow Database Connectivity) driver on top of the core client |
| `benchmarks`, `samples` | internal, never published |

## Code conventions

- The core module is mid-port from Java to Kotlin (Java under `src/main/java`,
  Kotlin under `src/main/kotlin`). Its public surface must stay directly
  Java-callable — no Kotlin types in public signatures.
- Kotlin code compiles in explicit-API mode: public declarations need explicit
  visibility and return types.

## Pull requests

CI runs the build (unit tests) and the default-image integration suite on
every PR; the full ClickHouse version matrix runs after merge to `main`.

Every merge to `main` is automatically published as a `-SNAPSHOT` to Maven
Central, and PR titles become the release notes — write them as a user-facing
sentence (see [RELEASING.md](RELEASING.md)).
