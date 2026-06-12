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
