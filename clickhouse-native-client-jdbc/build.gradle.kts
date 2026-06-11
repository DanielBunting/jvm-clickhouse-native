import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.JavaLibrary

plugins {
    `java-library`
    // Maven Central publishing (Central Portal): builds the sources/javadoc jars,
    // signs all artifacts, and uploads the bundle. Version pinned in the root build.
    id("com.vanniktech.maven.publish")
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
