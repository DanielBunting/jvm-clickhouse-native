// Root build. Shared coordinates plus the Maven Central publishing plugin version.
// Publishing is configured per publishable module (`mavenPublishing` block in
// clickhouse-native-client, -jdbc, and -kotlin); benchmarks and samples stay unpublished.
// Version stays a snapshot until the first real release.
plugins {
    // Plugin versions are pinned here once (apply false) so the publish plugin and
    // the Kotlin plugin share one classpath — vanniktech needs to see the Kotlin
    // plugin classes, which fails if they live in sibling module classloaders.
    // Modules apply these by id only, without a version.
    kotlin("jvm") version "2.0.21" apply false
    id("org.jetbrains.dokka") version "1.9.20" apply false
    id("com.vanniktech.maven.publish") version "0.35.0" apply false
}

subprojects {
    group = "io.github.danielbunting.clickhouse"
    version = "0.1.0-SNAPSHOT"

    // Generated -javadoc.jar shouldn't fail the build on doc-comment nitpicks
    // (broken @link refs, missing @param). Disable doclint; keep generation.
    tasks.withType<Javadoc>().configureEach {
        (options as org.gradle.external.javadoc.StandardJavadocDocletOptions)
            .addStringOption("Xdoclint:none", "-quiet")
    }
}
