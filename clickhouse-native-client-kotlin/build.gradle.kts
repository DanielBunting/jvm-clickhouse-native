import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm

plugins {
    kotlin("jvm")
    id("org.jetbrains.dokka")
    // Maven Central publishing (Central Portal): builds the sources/javadoc jars,
    // signs all artifacts, and uploads the bundle. Version pinned in the root build.
    id("com.vanniktech.maven.publish")
    // Uber-jar for the downloadable kotlin-query-flow bundle (Kotlin/Flow layer +
    // core + coroutines + reflect + stdlib + codecs).
    id("com.gradleup.shadow")
}

// Self-contained Kotlin coroutines/Flow client jar (this layer + core + coroutines +
// kotlin-reflect + stdlib + lz4/zstd codecs) for the kotlin-query-flow.zip release
// bundle. Codecs bundled but not relocated. Packaged by the root :releaseBundles task.
tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("clickhouse-native-client-kotlin")
    archiveClassifier.set("all")
    mergeServiceFiles()
}

// Keep the uber-jar OUT of the Maven Central publication (GitHub Release asset only).
(components["java"] as org.gradle.api.component.AdhocComponentWithVariants)
    .withVariantsFromConfiguration(configurations["shadowRuntimeElements"]) { skip() }

val kotlinFlowBundleReadme = tasks.register("kotlinFlowBundleReadme") {
    description = "README shipped inside kotlin-query-flow.zip."
    val readme = layout.buildDirectory.file("bundle-readme/README.txt")
    val v = project.version.toString()
    outputs.file(readme)
    doLast {
        readme.get().asFile.writeText(
            """
            ClickHouse Native Client — Kotlin coroutines/Flow $v — single-jar bundle
            =======================================================================

            Kotlin coroutines/Flow/DSL query support over the native TCP protocol,
            bundled with the core client, kotlinx-coroutines, kotlin-reflect, the
            Kotlin stdlib and lz4/zstd codecs. Self-contained: add
            clickhouse-native-client-kotlin-$v-all.jar to the classpath.
            """.trimIndent() + "\n"
        )
    }
}

// Packages the Kotlin/Flow uber-jar (+ README) into kotlin-query-flow.zip.
tasks.register<Zip>("kotlinFlowBundle") {
    group = "distribution"
    description = "Packages the Kotlin coroutines/Flow uber-jar into kotlin-query-flow.zip."
    archiveFileName.set("kotlin-query-flow.zip")
    destinationDirectory.set(rootProject.layout.buildDirectory.dir("release-assets"))
    from(tasks.named("shadowJar"))
    from(kotlinFlowBundleReadme)
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

kotlin {
    jvmToolchain(17)
}

mavenPublishing {
    // Dokka is applied, so publish its output as the javadoc jar.
    configure(KotlinJvm(javadocJar = JavadocJar.Dokka("dokkaHtml"), sourcesJar = true))
    publishToMavenCentral()
    signAllPublications()
    coordinates(group.toString(), "clickhouse-native-client-kotlin", version.toString())
    pom {
        name.set("clickhouse-native-client-kotlin")
        description.set(
            "Kotlin coroutines/Flow/DSL extensions for clickhouse-native-client — " +
                "ClickHouse via the native TCP protocol."
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
    api(project(":clickhouse-native-client"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    // Primary-constructor row binding for immutable data classes (reads @Metadata parameter
    // names). Quarantined here on purpose — the core must stay reflection-light and Java-shaped.
    implementation(kotlin("reflect"))

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")

    // Shared test-only helpers (e.g. ClickHouseImages) from clickhouse-native-client's test fixtures.
    testImplementation(testFixtures(project(":clickhouse-native-client")))

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // Integration harness spins a real ClickHouse via a generic container.
    testImplementation("org.testcontainers:testcontainers:1.21.4")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
}

// Default test task: run unit tests only (exclude integration tag so the build stays offline/fast).
tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

// Extracts the version matrix from ClickHouseImages.SUPPORTED_VERSIONS (the single
// source of truth, in the core module's test fixtures) by reading its
// `List.of("…", "…")` literal — so the fixture and this task matrix can't drift.
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
// Run with: ./gradlew :clickhouse-native-client-kotlin:integrationTest
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
// Run with: ./gradlew :clickhouse-native-client-kotlin:integrationTestAllVersions
tasks.register("integrationTestAllVersions") {
    description = "Runs integration tests against all supported ClickHouse versions " +
        "$supportedClickHouseVersions (requires Docker)."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    dependsOn(versionedIntegrationTests)
}
