rootProject.name = "clickhouse-native"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include(
    "clickhouse-native-client",
    "clickhouse-native-client-jdbc",
    "clickhouse-native-client-kotlin",
    "clickhouse-native-client-adbc",
    "benchmarks",
    "samples",
)
