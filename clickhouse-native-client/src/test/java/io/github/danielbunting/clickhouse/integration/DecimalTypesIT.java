package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips {@code Decimal} at all four wire widths through a real server in
 * both directions — DECODE (raw {@code INSERT ... VALUES} with
 * {@link BigDecimal#toPlainString()} literals) and ENCODE (bulk insert of a
 * mapped {@link BigDecimal} record) — plus the {@code query(sql, Class)}
 * mapped-read path:
 * <ul>
 *   <li>{@code Decimal(9,2)}  — Decimal32, 4-byte backing integer</li>
 *   <li>{@code Decimal(18,4)} — Decimal64, 8-byte backing integer</li>
 *   <li>{@code Decimal(38,8)} — Decimal128, 16-byte backing integer</li>
 *   <li>{@code Decimal(76,2)} — Decimal256, 32-byte backing integer</li>
 * </ul>
 *
 * <p>Each width covers zero, a negative value, and a near-max-precision positive
 * value. There is also a server-side rounding case: inserting the literal
 * {@code 1.239} into {@code Decimal(9,2)} (decode path) pins ClickHouse's
 * half-up rounding to {@code 1.24}.
 *
 * <p>All equality is asserted with {@link BigDecimal#compareTo} {@code == 0}
 * (scale-insensitive), never {@code equals}, so {@code 1.50} vs {@code 1.5}
 * never produces a spurious failure.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class DecimalTypesIT extends TypeRoundTripBase {

    /** Decimal(9,2) — Decimal32 (4-byte). Max magnitude 9999999.99. */
    @Test
    void decimal32RoundTrip() {
        BigDecimal[] inputs = {
            new BigDecimal("0.00"),
            new BigDecimal("-12345.67"),
            new BigDecimal("9999999.99")
        };
        roundTrip("dec32", "Decimal(9, 2)", "d", inputs);
    }

    /** Decimal(18,4) — Decimal64 (8-byte). Up to 14 int + 4 frac digits. */
    @Test
    void decimal64RoundTrip() {
        BigDecimal[] inputs = {
            new BigDecimal("0.0000"),
            new BigDecimal("-271828.1828"),
            new BigDecimal("99999999999999.9999")
        };
        roundTrip("dec64", "Decimal(18, 4)", "d", inputs);
    }

    /** Decimal(38,8) — Decimal128 (16-byte, exceeds long range). */
    @Test
    void decimal128RoundTrip() {
        BigDecimal[] inputs = {
            new BigDecimal("0.00000000"),
            new BigDecimal("-123456789012345678901234.56789012"),
            new BigDecimal("999999999999999999999999999999.99999999")
        };
        roundTrip("dec128", "Decimal(38, 8)", "d", inputs);
    }

    /** Decimal(76,2) — Decimal256 (32-byte, widest decimal). */
    @Test
    void decimal256RoundTrip() {
        BigDecimal[] inputs = {
            new BigDecimal("0.00"),
            new BigDecimal("-9876543210987654321098765432109876543210.12"),
            new BigDecimal(
                "99999999999999999999999999999999999999999999999999999999999999999999999999.99")
        };
        roundTrip("dec256", "Decimal(76, 2)", "d", inputs);
    }

    /**
     * ROUNDING: inserting literal {@code 1.239} into {@code Decimal(9,2)} (decode
     * path) pins ClickHouse's server-side behavior. Empirically (image 25.6) the
     * server TRUNCATES the excess fractional digit toward zero, yielding
     * {@code 1.23} — it does NOT round half-up to {@code 1.24}.
     */
    @Test
    void decimal32LiteralRoundingIsTruncation() {
        withTable("dec_round", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  d  Decimal(9, 2)"
                    + ") ENGINE = MergeTree() ORDER BY id");
            // 1.239 has 3 fractional digits; the column scale is 2.
            conn.execute("INSERT INTO " + table + " (id, d) VALUES (1, 1.239)");

            List<Object[]> rows = decode(conn,
                    "SELECT d FROM " + table + " ORDER BY id");
            assertEquals(1, rows.size(), "Expected 1 row from " + table);

            Object actual = rows.get(0)[0];
            assertInstanceOf(BigDecimal.class, actual,
                    "Decimal(9,2) must decode to BigDecimal");
            // Pinned actual behavior: ClickHouse truncates 1.239 -> 1.23 (toward zero),
            // not half-up rounding to 1.24.
            assertEquals(0, new BigDecimal("1.23").compareTo((BigDecimal) actual),
                    "ClickHouse truncates literal 1.239 toward zero to 1.23 at Decimal(9,2)"
                    + " (NOT half-up to 1.24); got " + actual);
        });
    }

    /**
     * Runs the DECODE + ENCODE + MAPPED-READ round-trip for one decimal width.
     *
     * @param prefix   table-name prefix
     * @param chType   the ClickHouse decimal type, e.g. {@code "Decimal(18, 4)"}
     * @param column   the decimal column name
     * @param inputs   the values to round-trip, in order
     */
    private void roundTrip(String prefix, String chType, String column, BigDecimal[] inputs) {
        // DECODE: literal INSERT, client decodes back.
        withTable(prefix + "_dec", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  " + column + " " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");

            StringBuilder sql = new StringBuilder("INSERT INTO ").append(table)
                    .append(" (id, ").append(column).append(") VALUES ");
            for (int i = 0; i < inputs.length; i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                // toPlainString() avoids scientific notation ClickHouse would reject.
                sql.append('(').append(i + 1).append(", ")
                   .append(inputs[i].toPlainString()).append(')');
            }
            conn.execute(sql.toString());

            List<Object[]> rows = decode(conn,
                    "SELECT " + column + " FROM " + table + " ORDER BY id");
            assertDecimals(chType + " decode", inputs, rows);
        });

        // ENCODE + MAPPED-READ: bulk insert a mapped BigDecimal record.
        withTable(prefix + "_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  d  " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<DecimalRow> input = new java.util.ArrayList<>();
            for (int i = 0; i < inputs.length; i++) {
                input.add(new DecimalRow(i + 1, inputs[i]));
            }

            try (BulkInserter<DecimalRow> inserter =
                    conn.createBulkInserter(table, DecimalRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT d FROM " + table + " ORDER BY id");
            assertDecimals(chType + " encode", inputs, rows);

            // (b) mapped-read via query(sql, Class)
            List<DecimalRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, d FROM " + table + " ORDER BY id", DecimalRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(inputs.length, mapped.size(),
                    chType + " mapped: expected " + inputs.length + " rows");
            for (int i = 0; i < inputs.length; i++) {
                assertEquals(0, inputs[i].compareTo(mapped.get(i).d()),
                        chType + " mapped row " + i + ": expected " + inputs[i]
                        + " but got " + mapped.get(i).d());
            }
        });
    }

    private static void assertDecimals(String label, BigDecimal[] inputs, List<Object[]> rows) {
        assertEquals(inputs.length, rows.size(),
                label + ": expected " + inputs.length + " rows");
        for (int i = 0; i < inputs.length; i++) {
            Object actual = rows.get(i)[0];
            assertInstanceOf(BigDecimal.class, actual,
                    label + " row " + i + " must decode to BigDecimal, got "
                    + (actual == null ? "null" : actual.getClass().getName()));
            assertEquals(0, inputs[i].compareTo((BigDecimal) actual),
                    label + " row " + i + ": expected " + inputs[i]
                    + " but got " + actual);
        }
    }

    /**
     * Row type for the bulk-insert / mapped-read paths. Field names {@code id}
     * and {@code d} match the column names; {@code d} is the natural
     * {@link BigDecimal} type for every Decimal width.
     */
    record DecimalRow(int id, BigDecimal d) {}
}
