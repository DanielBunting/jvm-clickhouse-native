plugins {
    java
    // Kotlin is needed to compile the src/jmh/kotlin benchmarks for the coroutine/Flow layer.
    kotlin("jvm")
    id("me.champeau.jmh") version "0.7.2"
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    // Driver under test.
    jmh(project(":clickhouse-native-client"))
    // Kotlin coroutine/Flow layer under test (transitively brings kotlin-stdlib + coroutines).
    jmh(project(":clickhouse-native-client-kotlin"))
    jmh("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    // Shared test-only helpers (e.g. ClickHouseImages) from clickhouse-native-client's test fixtures.
    jmh(testFixtures(project(":clickhouse-native-client")))
    // Competitors for the head-to-head comparison table.
    // Official driver, HTTP (port 8123). The ':all' shaded jar omits the client SPI
    // (com.clickhouse.client.ClickHouseClient), so use the modular artifacts: jdbc +
    // client (the SPI) + http-client (the transport impl, found via ServiceLoader).
    jmh("com.clickhouse:clickhouse-jdbc:0.6.5")
    jmh("com.clickhouse:clickhouse-client:0.6.5")
    jmh("com.clickhouse:clickhouse-http-client:0.6.5")
    jmh("com.github.housepower:clickhouse-native-jdbc:2.7.1")    // community, native TCP (port 9000)
    // Spin a real ClickHouse for the benchmark harness (override with -Dch.host).
    jmh("org.testcontainers:testcontainers:1.21.4")
}

jmh {
    warmupIterations.set(2)
    iterations.set(3)
    fork.set(1)
    // Report allocation rate / bytes-per-op so the low-allocation claim is measurable.
    profilers.add("gc")
    // ch.host is read inside the forked benchmark JVM (ClickHouseResource), which
    // doesn't inherit the Gradle invocation's -D flags — forward it explicitly.
    providers.systemProperty("ch.host").orNull?.let { jvmArgsAppend.add("-Dch.host=$it") }
}
