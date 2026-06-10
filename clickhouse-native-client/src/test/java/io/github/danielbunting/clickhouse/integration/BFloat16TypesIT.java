package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips the experimental ClickHouse {@code BFloat16} type through a real
 * server in both directions — DECODE (raw {@code INSERT ... VALUES}, server
 * encodes) and ENCODE (bulk insert of a mapped record with a {@link Float}
 * field).
 *
 * <p>{@code BFloat16} keeps only the high 16 bits of a {@code Float32} (8 exponent
 * + 7 mantissa bits), giving ~3 significant decimal digits. To assert exact
 * equality the test uses values that are exactly representable in bfloat16 (small
 * integers and simple binary fractions: 1.5, 2.0, -3.0, 0.5, 0.0) so no rounding
 * occurs and {@code .equals} holds.
 *
 * <p>May require {@code SET allow_experimental_bfloat16_type = 1}; the flag is SET
 * on the connection that runs CREATE / INSERT / SELECT.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class BFloat16TypesIT extends TypeRoundTripBase {

    private static final String GATE = "SET allow_experimental_bfloat16_type = 1";

    /** Exactly-representable bfloat16 values; equality must hold with no tolerance. */
    private static final float[] VALUES = {1.5f, 2.0f, -3.0f, 0.5f, 0.0f};

    record BF16Row(int id, Float v) {}

    @Test
    void bfloat16RoundTrip() {
        // DECODE: literal INSERT, client decodes to Float.
        withTable("bf16_dec", (conn, table) -> {
            conn.execute(GATE);
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  v  BFloat16"
                    + ") ENGINE = MergeTree() ORDER BY id");
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < VALUES.length; i++) {
                if (i > 0) {
                    values.append(", ");
                }
                values.append("(").append(i + 1).append(", ").append(VALUES[i]).append(")");
            }
            conn.execute("INSERT INTO " + table + " (id, v) VALUES " + values);

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(VALUES.length, rows.size(), "Expected " + VALUES.length + " BFloat16 rows");
            for (int i = 0; i < VALUES.length; i++) {
                assertBFloat(rows.get(i)[0], VALUES[i]);
            }
        });

        // ENCODE + MAPPED-READ.
        withTable("bf16_enc", (conn, table) -> {
            conn.execute(GATE);
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  v  BFloat16"
                    + ") ENGINE = MergeTree() ORDER BY id");
            List<BF16Row> input = new java.util.ArrayList<>();
            for (int i = 0; i < VALUES.length; i++) {
                input.add(new BF16Row(i + 1, VALUES[i]));
            }
            try (BulkInserter<BF16Row> inserter =
                    conn.createBulkInserter(table, BF16Row.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(VALUES.length, rows.size(), "Expected " + VALUES.length + " bulk BFloat16 rows");
            for (int i = 0; i < VALUES.length; i++) {
                assertBFloat(rows.get(i)[0], VALUES[i]);
            }

            List<BF16Row> mapped;
            try (var stream = conn.query(
                    "SELECT id, v FROM " + table + " ORDER BY id", BF16Row.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, "Mapped BFloat16 records must equal the inserted records");
        });
    }

    private static void assertBFloat(Object actual, float expected) {
        assertInstanceOf(Float.class, actual,
                "BFloat16 must decode to Float, got "
                + (actual == null ? "null" : actual.getClass().getName()));
        // Exactly-representable values round-trip without loss; assert with zero tolerance.
        assertEquals(expected, (Float) actual, 0.0f, "BFloat16 value");
    }
}
