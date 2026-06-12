import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm

plugins {
    `java-library`
    // Test fixtures expose shared test-only helpers (e.g. ClickHouseImages) to
    // the JDBC module's tests and the benchmarks' JMH source set.
    `java-test-fixtures`
    // Joint Java+Kotlin compilation: the core is being ported to Kotlin file by
    // file (remaining Java under src/main/java, Kotlin under src/main/kotlin).
    // The public surface must stay directly Java-callable — no Kotlin types in
    // public signatures.
    kotlin("jvm")
    // Maven Central publishing (Central Portal): builds the sources/javadoc jars,
    // signs all artifacts, and uploads the bundle. Version pinned in the root build.
    id("com.vanniktech.maven.publish")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

kotlin {
    jvmToolchain(17)
    // Strict explicit-API mode: every public Kotlin declaration needs an explicit
    // visibility modifier and return type, so nothing leaks into the Java-facing
    // surface by accident.
    explicitApi()
    compilerOptions {
        // Interface default bodies compile to true JVM default methods (no
        // DefaultImpls stubs), so Java callers and Java implementors of e.g.
        // ColumnCodec see the same binary shape as the original Java interfaces.
        freeCompilerArgs.add("-Xjvm-default=all")
    }
}

tasks.withType<JavaCompile> { options.release.set(17) }

// Test fixtures (ClickHouseImages etc.) are build-internal — shared with the other
// modules' tests and the JMH source set, but not part of the published library.
(components["java"] as AdhocComponentWithVariants).run {
    withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
    withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
    // The fixtures *sources* variant only joins the component once the publish
    // plugin wires up withSourcesJar() in its own afterEvaluate — ours runs later.
    afterEvaluate {
        withVariantsFromConfiguration(configurations["testFixturesSourcesElements"]) { skip() }
    }
}

mavenPublishing {
    // Kotlin-JVM project: sources jar from the Kotlin sources, empty javadoc jar
    // (Central only requires the artifact to exist; Dokka can replace it later).
    configure(KotlinJvm(javadocJar = JavadocJar.Empty(), sourcesJar = true))
    publishToMavenCentral()
    signAllPublications()
    coordinates(group.toString(), "clickhouse-native-client", version.toString())
    pom {
        name.set("clickhouse-native-client")
        description.set(
            "High-performance native TCP (port 9000) ClickHouse client for the JVM " +
                "with column-major data plane and LZ4/Zstd compression."
        )
        url.set("https://github.com/DanielBunting/jvm-clickhouse-native")
        licenses {
            license {
                name.set("Apache-2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("DanielBunting")
                name.set("Daniel Bunting")
                url.set("https://github.com/DanielBunting")
            }
        }
        scm {
            url.set("https://github.com/DanielBunting/jvm-clickhouse-native")
            connection.set("scm:git:git://github.com/DanielBunting/jvm-clickhouse-native.git")
            developerConnection.set("scm:git:ssh://git@github.com/DanielBunting/jvm-clickhouse-native.git")
        }
    }
}

dependencies {
    // Compression codecs (Spec 3). lz4 is part of the public surface (default
    // method), so api; zstd is an implementation detail behind the Compressor SPI.
    api("org.lz4:lz4-java:1.8.0")
    implementation("com.github.luben:zstd-jni:1.5.6-3")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // Integration harness (W1.F1) spins a real ClickHouse via a generic container.
    testImplementation("org.testcontainers:testcontainers:1.21.4")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    // Network-chaos integration tests (ToxiproxyChaosIT): the testcontainers
    // Toxiproxy module pulls in the eclipse/rekawek toxiproxy-java client transitively.
    testImplementation("org.testcontainers:toxiproxy:1.21.4")
    // Cross-client round-trip ITs (CrossClient*IT): the official ClickHouse JDBC
    // driver (0.8+ is built on client-v2; HTTP, port 8123) acts as the reference
    // implementation. Test-only — never leaks into the published library. The
    // benchmarks module pins the old 0.6.5 line separately for comparability.
    testImplementation("com.clickhouse:clickhouse-jdbc:0.9.0")
}

// Default test task: run unit tests only (exclude integration tag so the build stays offline/fast).
tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

// Extracts the version matrix from ClickHouseImages.SUPPORTED_VERSIONS (the single
// source of truth) by reading the fixture's `List.of("…", "…")` literal. Keeping the
// list in the Java file means the fixture and the Gradle task matrix can't drift.
fun parseSupportedClickHouseVersions(root: Project): List<String> {
    val fixture = root.file(
        "clickhouse-native-client/src/testFixtures/java/io/github/danielbunting/clickhouse/test/ClickHouseImages.java"
    )
    val literal = Regex("""SUPPORTED_VERSIONS\s*=\s*List\.of\(([^)]*)\)""")
        .find(fixture.readText())?.groupValues?.get(1)
        ?: error("Could not find SUPPORTED_VERSIONS literal in $fixture")
    return Regex(""""([^"]+)"""").findAll(literal)
        .map { it.groupValues[1] }
        .toList()
        .ifEmpty { error("SUPPORTED_VERSIONS is empty in $fixture") }
}

// Single source of truth for the supported-version matrix is
// ClickHouseImages.SUPPORTED_VERSIONS — parse it here so the task list and the
// test fixture can never drift apart.
val supportedClickHouseVersions: List<String> = parseSupportedClickHouseVersions(rootProject)

// Registers an integration-test task. `version == null` uses the default image baked
// into ClickHouseImages; otherwise the image is pinned via -Dch.image so the task runs
// in its own JVM against that one server version (each task gets its own report dir).
fun registerIntegrationTest(name: String, version: String?) =
    tasks.register<Test>(name) {
        description = version
            ?.let { "Runs integration tests against ClickHouse $it (requires Docker)." }
            ?: "Runs integration tests tagged 'integration' (requires Docker)."
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        useJUnitPlatform {
            includeTags("integration")
        }
        // Share the compiled test classes and classpath with the main test task.
        testClassesDirs = sourceSets["test"].output.classesDirs
        classpath = sourceSets["test"].runtimeClasspath
        if (version != null) {
            systemProperty("ch.image", version)
        }
        // Do not run integration tests as part of the regular check lifecycle.
        shouldRunAfter(tasks.test)
    }

// Default task: runs against ClickHouseImages.DEFAULT_SERVER.
// Run with: ./gradlew :clickhouse-native-client:integrationTest
val integrationTest = registerIntegrationTest("integrationTest", null)

// Coverage from the default-version integration run. CI uploads this XML to
// Codecov alongside the unit-test report; Codecov merges per-commit uploads.
tasks.register<JacocoReport>("jacocoIntegrationTestReport") {
    description = "Generates a coverage report from integrationTest execution data."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    dependsOn(integrationTest)
    executionData(integrationTest.get())
    sourceSets(sourceSets["main"])
}

// One task per supported version, e.g. integrationTest_26_4.
val versionedIntegrationTests = supportedClickHouseVersions.map { version ->
    registerIntegrationTest("integrationTest_${version.replace('.', '_')}", version)
}

// Aggregate: runs the integration suite against every supported version.
// Run with: ./gradlew :clickhouse-native-client:integrationTestAllVersions
tasks.register("integrationTestAllVersions") {
    description = "Runs integration tests against all supported ClickHouse versions " +
        "$supportedClickHouseVersions (requires Docker)."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    dependsOn(versionedIntegrationTests)
}
