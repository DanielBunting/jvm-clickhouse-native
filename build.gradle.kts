// Root build. Shared coordinates plus the Maven Central publishing plugin version.
// Publishing is configured per publishable module (`mavenPublishing` block in
// clickhouse-native-client, -jdbc, and -kotlin); benchmarks and samples stay unpublished.
plugins {
    // Plugin versions are pinned here once (apply false) so the publish plugin and
    // the Kotlin plugin share one classpath — vanniktech needs to see the Kotlin
    // plugin classes, which fails if they live in sibling module classloaders.
    // Modules apply these by id only, without a version.
    kotlin("jvm") version "2.0.21" apply false
    id("org.jetbrains.dokka") version "1.9.20" apply false
    id("com.vanniktech.maven.publish") version "0.35.0" apply false
    // Uber-jar assembly for the downloadable release bundles (single self-contained
    // jar per module). Applied by id in the publishable modules.
    id("com.gradleup.shadow") version "8.3.5" apply false
}

// Version scheme: the VERSION file holds major.minor; CI computes the patch as the
// commit count on main since VERSION last changed and passes it via -PbuildVersion
// (snapshot publishes append -SNAPSHOT, releases don't). Local builds default to
// <major.minor>.0-SNAPSHOT so the build itself never has to consult git.
val baseVersion = rootProject.file("VERSION").readText().trim()
val buildVersion = providers.gradleProperty("buildVersion").orNull ?: "$baseVersion.0-SNAPSHOT"
require(buildVersion.startsWith("$baseVersion.")) {
    "buildVersion '$buildVersion' does not match VERSION file '$baseVersion' — " +
        "the major.minor in -PbuildVersion must come from this checkout's VERSION file."
}

subprojects {
    group = "io.github.danielbunting.clickhouse"
    version = buildVersion

    // Generated -javadoc.jar shouldn't fail the build on doc-comment nitpicks
    // (broken @link refs, missing @param). Disable doclint; keep generation.
    tasks.withType<Javadoc>().configureEach {
        (options as org.gradle.external.javadoc.StandardJavadocDocletOptions)
            .addStringOption("Xdoclint:none", "-quiet")
    }

    // Coverage: every Java module gets JaCoCo with an XML report, uploaded to
    // Codecov by CI. Two sources, merged per-commit by Codecov: the unit `test`
    // task (jacocoTestReport, build job) and the default integrationTest run
    // (jacocoIntegrationTestReport, integration job).
    plugins.withId("java") {
        apply(plugin = "jacoco")
        tasks.withType<JacocoReport>().configureEach {
            reports.xml.required.set(true)
        }
    }
}

// ---------------------------------------------------------------------------
// Release distribution bundles
// ---------------------------------------------------------------------------
// Four downloadable zips, all landing in build/release-assets/ and attached to
// each GitHub Release (see .github/workflows/release.yml):
//   jdbc.zip              — single JDBC uber-jar          (:...-jdbc:jdbcBundle)
//   native-client.zip     — single core-client uber-jar   (:...-client:nativeClientBundle)
//   kotlin-query-flow.zip — single Kotlin/Flow uber-jar   (:...-kotlin:kotlinFlowBundle)
//   all-jars.zip          — every module jar + all shared deps as LOOSE jars (below)
// The three single-jar bundles are defined in their modules (next to their
// shadowJar); all-jars is assembled here because it spans every module.
val releaseAssetsDir = layout.buildDirectory.dir("release-assets")

// Resolves to the full runtime closure of the publishable modules — every module
// jar plus every shared third-party dependency, deduplicated by dependency
// resolution — for the loose-jar all-jars.zip bundle.
val allModulesRuntime: Configuration by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
}
dependencies {
    allModulesRuntime(project(":clickhouse-native-client-jdbc"))
    allModulesRuntime(project(":clickhouse-native-client-kotlin"))
    // core is pulled transitively by both, but list it so the intent is explicit.
    allModulesRuntime(project(":clickhouse-native-client"))
}

val allJarsBundleReadme = tasks.register("allJarsBundleReadme") {
    description = "README shipped inside all-jars.zip."
    val readme = layout.buildDirectory.file("bundle-readme/README.txt")
    val v = buildVersion
    outputs.file(readme)
    doLast {
        readme.get().asFile.writeText(
            """
            ClickHouse Native — all jars $v (loose bundle)
            ==============================================

            Every module jar plus all shared runtime dependencies as separate jars,
            for hand-picking a classpath. For a ready-to-use single file, prefer the
            per-module bundles instead:
              jdbc.zip              — JDBC driver (DataGrip/DBeaver)
              native-client.zip     — core native client
              kotlin-query-flow.zip — Kotlin coroutines/Flow client
            """.trimIndent() + "\n"
        )
    }
}

// all-jars.zip: the loose kitchen-sink set under one folder.
val allJarsBundle = tasks.register<Zip>("allJarsBundle") {
    group = "distribution"
    description = "Every module jar + all shared dependencies as loose jars (all-jars.zip)."
    archiveFileName.set("all-jars.zip")
    destinationDirectory.set(releaseAssetsDir)
    into("all-jars-$buildVersion")
    from(allModulesRuntime)
    from(allJarsBundleReadme)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Umbrella: builds all four release-asset zips into build/release-assets/.
// Run with: ./gradlew releaseBundles
tasks.register("releaseBundles") {
    group = "distribution"
    description = "Builds all downloadable release-asset zips into build/release-assets/."
    dependsOn(
        allJarsBundle,
        ":clickhouse-native-client-jdbc:jdbcBundle",
        ":clickhouse-native-client:nativeClientBundle",
        ":clickhouse-native-client-kotlin:kotlinFlowBundle",
    )
}
