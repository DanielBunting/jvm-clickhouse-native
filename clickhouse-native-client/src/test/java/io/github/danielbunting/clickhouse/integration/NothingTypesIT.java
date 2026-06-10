package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Decodes the ClickHouse {@code Nothing} bottom type, which surfaces only as the
 * element type of an empty / NULL-only array: {@code SELECT []} produces an
 * {@code Array(Nothing)} column. With no {@code Nothing} codec the parser would
 * throw on the unknown inner type; the {@link io.github.danielbunting.clickhouse.types.codec.NothingCodec}
 * lets {@code Array(Nothing)} resolve and decode to an empty {@link List}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class NothingTypesIT extends TypeRoundTripBase {

    /** {@code SELECT []} -> {@code Array(Nothing)} decodes to an empty List. */
    @Test
    void emptyArrayLiteralDecodesToEmptyList() {
        try (var conn = io.github.danielbunting.clickhouse.ClickHouseConnection.open(config())) {
            List<Object[]> rows = decode(conn, "SELECT [] AS a");
            assertEquals(1, rows.size(), "expected exactly 1 row");
            List<?> arr = assertInstanceOf(List.class, rows.get(0)[0],
                    "Array(Nothing) must decode to a java.util.List");
            assertTrue(arr.isEmpty(), "empty array literal must decode to an empty List");
        }
    }

    /** Explicit {@code CAST([] AS Array(Nothing))} likewise decodes to an empty List. */
    @Test
    void castEmptyArrayNothingDecodesToEmptyList() {
        try (var conn = io.github.danielbunting.clickhouse.ClickHouseConnection.open(config())) {
            List<Object[]> rows = decode(conn, "SELECT CAST([] AS Array(Nothing)) AS a");
            assertEquals(1, rows.size(), "expected exactly 1 row");
            List<?> arr = assertInstanceOf(List.class, rows.get(0)[0],
                    "Array(Nothing) must decode to a java.util.List");
            assertTrue(arr.isEmpty(), "CAST([] AS Array(Nothing)) must decode to an empty List");
        }
    }
}
