plugins {
    application
    // The samples.kotlin sources demonstrate the clickhouse-native-client-kotlin coroutine surface.
    kotlin("jvm")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

kotlin {
    jvmToolchain(17)
}

tasks.withType<JavaCompile> { options.release.set(17) }

dependencies {
    implementation(project(":clickhouse-native-client"))
    // Coroutine/Flow extensions used by the Kotlin sample. The coroutines runtime is an
    // implementation detail of that module, so the sample declares its own dependency.
    implementation(project(":clickhouse-native-client-kotlin"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    // JSON parsing for the live-stream samples. Streams themselves are read with the
    // JDK's java.net.http HttpClient/WebSocket — no extra client SDK is needed.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    // ADBC sample: discover the driver the way an external Arrow consumer would.
    implementation(project(":clickhouse-native-client-adbc"))
    implementation("org.apache.arrow.adbc:adbc-driver-manager:0.23.0")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:18.3.0")
}

// The module hosts several runnable demos; register one run task per sample.
// Each inherits the Gradle process environment, so CH_HOST/CH_USER/... pass through.
val sampleMains = mapOf(
    // Java samples (native API directly).
    "runQuickStart" to "io.github.danielbunting.clickhouse.samples.QuickStart",
    "runAdbcQuickStart" to "io.github.danielbunting.clickhouse.samples.AdbcQuickStart",
    "runQueries" to "io.github.danielbunting.clickhouse.samples.Queries",
    "runBulkInserts" to "io.github.danielbunting.clickhouse.samples.BulkInserts",
    "runPooling" to "io.github.danielbunting.clickhouse.samples.Pooling",
    // Live-stream samples (need internet access) live in the `live` subpackage.
    "runWikimedia" to "io.github.danielbunting.clickhouse.samples.live.WikimediaEditsStream",
    "runCoinbase" to "io.github.danielbunting.clickhouse.samples.live.CoinbaseTradesStream",
    "runBluesky" to "io.github.danielbunting.clickhouse.samples.live.BlueskyJetstream",
    // Kotlin samples (coroutine surface of clickhouse-native-client-kotlin), in the
    // parallel samples.kotlin package — file names match the Java set 1:1.
    "runKotlinQuickStart" to "io.github.danielbunting.clickhouse.samples.kotlin.QuickStartKt",
    "runKotlinQueries" to "io.github.danielbunting.clickhouse.samples.kotlin.QueriesKt",
    "runKotlinBulkInserts" to "io.github.danielbunting.clickhouse.samples.kotlin.BulkInsertsKt",
    "runKotlinPooling" to "io.github.danielbunting.clickhouse.samples.kotlin.PoolingKt",
    "runKotlinWikimedia" to "io.github.danielbunting.clickhouse.samples.kotlin.live.WikimediaEditsStreamKt",
    "runKotlinCoinbase" to "io.github.danielbunting.clickhouse.samples.kotlin.live.CoinbaseTradesStreamKt",
    "runKotlinBluesky" to "io.github.danielbunting.clickhouse.samples.kotlin.live.BlueskyJetstreamKt",
)
sampleMains.forEach { (taskName, fqcn) ->
    tasks.register<JavaExec>(taskName) {
        group = "samples"
        description = "Runs the $fqcn sample"
        mainClass.set(fqcn)
        classpath = sourceSets["main"].runtimeClasspath
        // Harmless for the non-Arrow samples; required by the ADBC sample's off-heap allocator.
        jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
    }
}

application {
    // Default `run` target; the per-sample tasks above are the usual entry points.
    mainClass.set("io.github.danielbunting.clickhouse.samples.QuickStart")
}
