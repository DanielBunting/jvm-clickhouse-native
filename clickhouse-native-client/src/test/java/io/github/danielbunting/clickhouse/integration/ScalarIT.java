package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for {@link ClickHouseConnection#executeScalar(String)}.
 *
 * <p>These tests verify that a simple literal SELECT round-trips through the
 * native protocol and returns the correct numeric value. They act as a
 * baseline sanity check for the connection handshake and query/response path.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class ScalarIT extends IntegrationTestBase {

    /**
     * Builds a default config pointing at the test container.
     *
     * @return a fresh {@link ClickHouseConfig} with default db/user/compression
     */
    private ClickHouseConfig config() {
        return ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .build();
    }

    /**
     * Asserts that {@code SELECT 1} returns the scalar value {@code 1}.
     *
     * <p>This exercises the minimum viable path: connect, handshake, send a
     * Query + empty Data block, receive header Data block, receive data block
     * with one row, receive END_OF_STREAM, close.
     */
    @Test
    void selectOneLiteralReturnsOne() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            long result = conn.executeScalar("SELECT 1");
            assertEquals(1L, result,
                    "SELECT 1 must return scalar 1 — wire-format or handshake bug if it does not");
        }
    }

    /**
     * Asserts that {@code SELECT 42} returns the scalar value {@code 42}.
     *
     * <p>Guards against an off-by-one or constant-return implementation that
     * passes {@code SELECT 1} by coincidence.
     */
    @Test
    void selectFortyTwoReturnsFortyTwo() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            long result = conn.executeScalar("SELECT 42");
            assertEquals(42L, result,
                    "SELECT 42 must return scalar 42 — check value extraction from UInt8/Int8 column");
        }
    }

    /**
     * Asserts that a large literal ({@code SELECT 9999999}) survives encoding
     * and decoding without truncation.
     */
    @Test
    void selectLargeLiteralReturnsCorrectValue() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            long result = conn.executeScalar("SELECT 9999999");
            assertEquals(9_999_999L, result,
                    "SELECT 9999999 must return scalar 9999999 — check integer column codec width");
        }
    }

    /**
     * Asserts that {@code SELECT toUInt64(1000000000000)} returns the expected
     * value, exercising a 64-bit integer column on the wire.
     */
    @Test
    void selectUInt64LiteralRoundTrips() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            long result = conn.executeScalar("SELECT toUInt64(1000000000000)");
            assertEquals(1_000_000_000_000L, result,
                    "toUInt64(1000000000000) must return 1000000000000 — check UInt64 / Int64 codec");
        }
    }
}
