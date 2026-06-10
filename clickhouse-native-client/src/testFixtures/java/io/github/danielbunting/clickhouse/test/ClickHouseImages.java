package io.github.danielbunting.clickhouse.test;

import java.util.List;

/**
 * Centralized ClickHouse Docker image coordinates shared by every test and
 * benchmark that spins a real server via Testcontainers.
 *
 * <p>This lives in the {@code clickhouse-native-client} {@code testFixtures} source set
 * so it can be reused across module boundaries: the JDBC module's tests and the
 * {@code benchmarks} JMH source set both depend on these test fixtures, which is
 * the only way a test-only constant can be genuinely shared (plain test sources
 * are not exported to other modules' test/jmh source sets).
 *
 * <p>{@link #SUPPORTED_VERSIONS} is the single source of truth for the version
 * matrix: each module's {@code build.gradle.kts} parses this list to register one
 * integration-test task per version (plus an {@code integrationTestAllVersions}
 * aggregate), so adding or removing a version is a one-line edit here.
 *
 * <p>The image is resolved once per JVM. By default it is {@link #DEFAULT_SERVER}
 * (the floor of the support window), but it can be overridden with the
 * {@value #IMAGE_PROPERTY} system property so a single test run targets one
 * specific server version. The override accepts either a full image reference
 * (containing a {@code /}, e.g. {@code clickhouse/clickhouse-server:26.4} or a
 * private-registry mirror) or a bare tag/version (e.g. {@code 26.4}), in which
 * case it is expanded against the official {@value #REPOSITORY} repository.
 *
 * <pre>{@code
 * ./gradlew :clickhouse-native-client:integrationTest -Dch.image=26.4
 * ./gradlew :clickhouse-native-client:integrationTest -Dch.image=clickhouse/clickhouse-server:26.4
 * }</pre>
 */
public final class ClickHouseImages {

    /** Official ClickHouse server image repository (used to expand bare-tag values). */
    public static final String REPOSITORY = "clickhouse/clickhouse-server";

    /**
     * Currently-supported ClickHouse versions the integration suites run against,
     * as bare {@code major.minor} tags, oldest first (so element 0 is the floor).
     *
     * <p><strong>Single source of truth.</strong> Each module's
     * {@code build.gradle.kts} parses this declaration to build its per-version
     * task matrix — keep it as a simple {@code List.of("…", "…")} literal so that
     * parsing stays trivial.
     */
    public static final List<String> SUPPORTED_VERSIONS = List.of("25.8", "26.3", "26.4", "26.5");

    /**
     * System property that overrides the server image for a test run. The value
     * is either a full image reference or a bare tag/version (see class docs).
     */
    public static final String IMAGE_PROPERTY = "ch.image";

    /**
     * Server image used when {@value #IMAGE_PROPERTY} is not set — the lowest
     * currently-supported version, so the default run reflects the support floor.
     */
    public static final String DEFAULT_SERVER = imageFor(SUPPORTED_VERSIONS.get(0));

    /**
     * Resolved ClickHouse server image for this JVM, honouring the
     * {@value #IMAGE_PROPERTY} override and falling back to {@link #DEFAULT_SERVER}.
     */
    public static final String SERVER = resolveServer();

    /**
     * Expands a bare version/tag (e.g. {@code "26.4"}) into a full image reference
     * against the official {@value #REPOSITORY} repository, or returns the input
     * unchanged if it already looks like a full reference (contains a {@code /}).
     *
     * @param versionOrImage a bare tag/version or a full image reference
     * @return a full Docker image reference
     */
    public static String imageFor(String versionOrImage) {
        return versionOrImage.indexOf('/') >= 0
                ? versionOrImage
                : REPOSITORY + ":" + versionOrImage;
    }

    private static String resolveServer() {
        String override = System.getProperty(IMAGE_PROPERTY);
        if (override == null || override.isBlank()) {
            return DEFAULT_SERVER;
        }
        return imageFor(override.trim());
    }

    private ClickHouseImages() {
        // Constants holder; not instantiable.
    }
}
