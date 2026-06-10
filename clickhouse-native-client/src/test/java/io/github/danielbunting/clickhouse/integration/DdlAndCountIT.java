package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for DDL ({@code CREATE TABLE}) and basic DML via
 * {@link ClickHouseConnection#execute(String)} followed by a scalar count.
 *
 * <p>Each test uses a unique table name to avoid cross-test interference
 * when tests run in parallel or are retried.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class DdlAndCountIT extends IntegrationTestBase {

    /**
     * Builds a default config pointing at the test container.
     *
     * @return a fresh {@link ClickHouseConfig}
     */
    private ClickHouseConfig config() {
        return ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .build();
    }

    /**
     * Creates a MergeTree table, inserts rows via a VALUES literal in
     * {@link ClickHouseConnection#execute(String)}, then counts them back.
     *
     * <p>Validates the full DDL → DML → query lifecycle using only the
     * simplest API surface. A count mismatch means either the INSERT was
     * not sent/acknowledged, or the SELECT path is broken.
     */
    @Test
    void createInsertAndCountRows() {
        String table = "ddl_count_basic_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id   UInt32,"
                + "  name String"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, name) VALUES"
                + " (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(3L, count,
                    "Expected 3 rows after inserting 3 VALUES tuples into " + table
                    + " — check INSERT packet framing or acknowledgement handling");

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Verifies that a second INSERT appends rows correctly (tests that the
     * connection is reusable across multiple round-trips).
     */
    @Test
    void multipleInsertsAccumulateRows() {
        String table = "ddl_count_multi_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  val UInt64"
                + ") ENGINE = MergeTree() ORDER BY val");

            conn.execute("INSERT INTO " + table + " (val) VALUES (10), (20)");
            conn.execute("INSERT INTO " + table + " (val) VALUES (30), (40), (50)");

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(5L, count,
                    "Expected 5 rows after two separate INSERTs into " + table
                    + " — connection must be reusable (no half-consumed packet state)");

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Asserts that creating the same table twice with {@code IF NOT EXISTS}
     * succeeds without throwing, and that the table still contains zero rows.
     */
    @Test
    void ifNotExistsIsIdempotent() {
        String table = "ddl_idempotent_" + System.nanoTime();
        String ddl =
            "CREATE TABLE IF NOT EXISTS " + table + " ("
            + "  id UInt32"
            + ") ENGINE = MergeTree() ORDER BY id";
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(ddl);
            conn.execute(ddl); // second time must not throw

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(0L, count,
                    "Freshly-created empty table must have 0 rows in " + table);

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
