import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.JavaLibrary

plugins {
    `java-library`
    // Maven Central publishing (Central Portal): builds the sources/javadoc jars,
    // signs all artifacts, and uploads the bundle. Version pinned in the root build.
    id("com.vanniktech.maven.publish")
    // Uber-jar for the downloadable driver bundle (JDBC + core + Kotlin stdlib + codecs).
    id("com.gradleup.shadow")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

tasks.withType<JavaCompile> { options.release.set(17) }

mavenPublishing {
    configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = true))
    publishToMavenCentral()
    signAllPublications()
    coordinates(group.toString(), "clickhouse-native-client-jdbc", version.toString())
    pom {
        name.set("clickhouse-native-client-jdbc")
        description.set(
            "JDBC driver over clickhouse-native-client — ClickHouse via the native " +
                "TCP protocol (jdbc:chnative:// URLs)."
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

// ---------------------------------------------------------------------------
// Uber-jar: clickhouse-native-jdbc-all
// ---------------------------------------------------------------------------
// A single self-contained driver jar (JDBC layer + core client + Kotlin stdlib +
// lz4/zstd codecs) for dropping into any JDBC tool (DataGrip, DBeaver, ...) with
// no Maven resolution. mergeServiceFiles() preserves the META-INF/services
// java.sql.Driver registration. The native-lib deps (lz4/zstd) are bundled but
// NOT relocated — relocation would break their fixed native-resource paths.
// Packaged into jdbc.zip by the root :releaseBundles task.
tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("clickhouse-native-jdbc")
    archiveClassifier.set("all")
    mergeServiceFiles()
}

// Keep the uber-jar OUT of the Maven Central publication — it ships only as a
// GitHub Release asset. Without this, the shadow plugin adds its -all.jar to the
// published java component (via the shadowRuntimeElements variant).
(components["java"] as org.gradle.api.component.AdhocComponentWithVariants)
    .withVariantsFromConfiguration(configurations["shadowRuntimeElements"]) { skip() }

val jdbcBundleReadme = tasks.register("jdbcBundleReadme") {
    description = "README shipped inside jdbc.zip."
    val readme = layout.buildDirectory.file("bundle-readme/README.txt")
    val v = project.version.toString()
    outputs.file(readme)
    doLast {
        readme.get().asFile.writeText(
            """
            ClickHouse Native JDBC Driver $v (single-jar bundle)
            ====================================================

            Setup (DataGrip / DBeaver / any JDBC tool):
              1. Add clickhouse-native-jdbc-$v-all.jar as the only driver file.
              2. Driver class:  io.github.danielbunting.clickhouse.jdbc.ClickHouseDriver
              3. JDBC URL:      jdbc:chnative://<host>:9000/<database>
                                e.g. jdbc:chnative://localhost:9000/default
              4. Supply user / password as connection properties.

            This is the native-protocol driver (port 9000), NOT the HTTP driver (8123).
            The jar is self-contained — no other files are needed.
            """.trimIndent() + "\n"
        )
    }
}

// Packages the JDBC uber-jar (+ README) into jdbc.zip under the shared release-assets dir.
tasks.register<Zip>("jdbcBundle") {
    group = "distribution"
    description = "Packages the JDBC uber-jar into jdbc.zip."
    archiveFileName.set("jdbc.zip")
    destinationDirectory.set(rootProject.layout.buildDirectory.dir("release-assets"))
    from(tasks.named("shadowJar"))
    from(jdbcBundleReadme)
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
// Run with: ./gradlew :clickhouse-native-client-jdbc:integrationTest
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
// Run with: ./gradlew :clickhouse-native-client-jdbc:integrationTestAllVersions
tasks.register("integrationTestAllVersions") {
    description = "Runs integration tests against all supported ClickHouse versions " +
        "$supportedClickHouseVersions (requires Docker)."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    dependsOn(versionedIntegrationTests)
}
