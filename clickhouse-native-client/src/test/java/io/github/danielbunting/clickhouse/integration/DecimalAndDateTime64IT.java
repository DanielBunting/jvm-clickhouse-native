package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests that round-trip wide {@code Decimal} types
 * ({@code Decimal32} / {@code Decimal64} / {@code Decimal128} / {@code Decimal256})
 * and high-precision {@code DateTime64} columns against a live ClickHouse server.
 *
 * <p>These exercise the {@code DecimalCodec} and {@code DateTime64} codecs end to
 * end (block framing, wire encoding, and decode). To keep assertions robust the
 * tests rely on <em>round-trip equality</em>: a known {@link BigDecimal} or
 * {@link Instant} is inserted and the value SELECTed back is compared against the
 * original input. No hand-computed scaled-integer or epoch constants are
 * hard-coded, so a wrong constant can never mask (or fabricate) a codec bug.
 *
 * <p>Decimal equality is asserted with {@link BigDecimal#compareTo(BigDecimal)}
 * {@code == 0} so that a differing {@code scale} (e.g. {@code 1.50} vs
 * {@code 1.5}) does not cause a spurious failure — only the numeric value
 * matters. Each {@code DateTime64} column pins the {@code 'UTC'} timezone so the
 * decoded {@link Instant} is deterministic regardless of the container's session
 * timezone.
 *
 * <p>Both the READ/decode path (literal {@code INSERT ... VALUES}) and, where the
 * boxed type is simple enough to map from a Java record field, the WRITE/encode
 * path (via {@link BulkInserter}) are covered.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class DecimalAndDateTime64IT extends IntegrationTestBase {

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
     * Reads all blocks from a {@link QueryResult} and materialises every
     * {@code (column, row)} cell into a row-major {@code List<Object[]>} using
     * the null-aware {@link Column#value(int)} accessor.
     *
     * @param result the query result (iterated by this method)
     * @return list of rows, each a {@code Object[]} of boxed, null-aware values
     */
    private List<Object[]> materialize(QueryResult result) {
        List<Object[]> rows = new ArrayList<>();
        Iterator<Block> blocks = result.blocks();
        while (blocks.hasNext()) {
            Block block = blocks.next();
            if (block.isEmpty()) {
                continue;
            }
            int colCount = block.columnCount();
            int rowCount = block.rowCount();
            for (int r = 0; r < rowCount; r++) {
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    Column col = block.column(c);
                    row[c] = col.value(r);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Round-trips a {@code Decimal(9,2)} ({@code Decimal32}) column using literal
     * inserts: insert several values within the precision, SELECT them back, and
     * assert each returned {@link BigDecimal} is numerically equal to the input.
     */
    @Test
    void decimal32RoundTrips() {
        String table = "dec_d32_" + System.nanoTime();
        // Values within Decimal(9,2): max magnitude is 9999999.99.
        BigDecimal[] inputs = {
            new BigDecimal("0.00"),
            new BigDecimal("1.50"),
            new BigDecimal("-12345.67"),
            new BigDecimal("9999999.99"),
            new BigDecimal("-9999999.99")
        };

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  d32 Decimal(9, 2)"
                + ") ENGINE = MergeTree() ORDER BY id");

            insertDecimals(conn, table, inputs);
            assertDecimalsRoundTrip(conn, table, "Decimal(9,2)", inputs);

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a {@code Decimal(18,4)} ({@code Decimal64}) column via literal
     * inserts, asserting numeric round-trip equality of each value.
     */
    @Test
    void decimal64RoundTrips() {
        String table = "dec_d64_" + System.nanoTime();
        // Decimal(18,4): up to 14 integer digits + 4 fractional digits.
        BigDecimal[] inputs = {
            new BigDecimal("0.0000"),
            new BigDecimal("3.1416"),
            new BigDecimal("-271828.1828"),
            new BigDecimal("99999999999999.9999"),
            new BigDecimal("-99999999999999.9999")
        };

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  d64 Decimal(18, 4)"
                + ") ENGINE = MergeTree() ORDER BY id");

            insertDecimals(conn, table, inputs);
            assertDecimalsRoundTrip(conn, table, "Decimal(18,4)", inputs);

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a {@code Decimal(38,8)} ({@code Decimal128}) column. The
     * 128-bit backing integer exceeds {@code long} range, so this exercises the
     * codec's wide little-endian integer encode/decode path.
     */
    @Test
    void decimal128RoundTrips() {
        String table = "dec_d128_" + System.nanoTime();
        // Decimal(38,8): up to 30 integer digits + 8 fractional digits.
        BigDecimal[] inputs = {
            new BigDecimal("0.00000000"),
            new BigDecimal("123456789012345678901234.56789012"),
            new BigDecimal("-123456789012345678901234.56789012"),
            // Largest magnitude representable with 38 significant digits, scale 8.
            new BigDecimal("999999999999999999999999999999.99999999"),
            new BigDecimal("-999999999999999999999999999999.99999999")
        };

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id   UInt32,"
                + "  d128 Decimal(38, 8)"
                + ") ENGINE = MergeTree() ORDER BY id");

            insertDecimals(conn, table, inputs);
            assertDecimalsRoundTrip(conn, table, "Decimal(38,8)", inputs);

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a {@code Decimal(76,2)} ({@code Decimal256}) column. This is
     * the widest ClickHouse decimal (256-bit backing integer) and is the primary
     * stress test for the wide-integer codec path.
     */
    @Test
    void decimal256RoundTrips() {
        String table = "dec_d256_" + System.nanoTime();
        // Decimal(76,2): up to 74 integer digits + 2 fractional digits.
        BigDecimal[] inputs = {
            new BigDecimal("0.00"),
            new BigDecimal("9876543210987654321098765432109876543210.12"),
            new BigDecimal("-9876543210987654321098765432109876543210.12"),
            // 74 nines + ".99" → 76 significant digits.
            new BigDecimal(
                "99999999999999999999999999999999999999999999999999999999999999999999999999.99"),
            new BigDecimal(
                "-99999999999999999999999999999999999999999999999999999999999999999999999999.99")
        };

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id   UInt32,"
                + "  d256 Decimal(76, 2)"
                + ") ENGINE = MergeTree() ORDER BY id");

            insertDecimals(conn, table, inputs);
            assertDecimalsRoundTrip(conn, table, "Decimal(76,2)", inputs);

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code DateTime64(3,'UTC')} (millisecond precision) using a
     * literal insert, asserting the decoded {@link Instant} equals the input
     * instant (which carries sub-second milliseconds).
     */
    @Test
    void dateTime64MillisRoundTrips() {
        String table = "dt64_ms_" + System.nanoTime();
        // 2024-03-15 12:30:45.123 UTC — millisecond sub-second precision.
        String literal = "2024-03-15 12:30:45.123";
        Instant expected = Instant.parse("2024-03-15T12:30:45.123Z");

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id UInt32,"
                + "  ts DateTime64(3, 'UTC')"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, ts) VALUES (1, '" + literal + "')");

            try (QueryResult result = conn.query(
                    "SELECT ts FROM " + table + " ORDER BY id")) {
                List<Object[]> rows = materialize(result);
                assertEquals(1, rows.size(), "Expected 1 row from " + table);

                Object actual = rows.get(0)[0];
                assertNotNull(actual, "DateTime64(3,'UTC') value must not be null");
                assertInstanceOf(Instant.class, actual,
                        "DateTime64(3) must decode to java.time.Instant, got "
                        + actual.getClass().getName());
                assertEquals(expected, actual,
                        "DateTime64(3,'UTC') '" + literal + "' must round-trip to "
                        + expected + " — check millisecond-scale conversion in DateTime64 codec");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code DateTime64(9,'UTC')} (nanosecond precision) using a
     * literal insert, asserting the decoded {@link Instant} equals the input
     * instant down to nanoseconds.
     */
    @Test
    void dateTime64NanosRoundTrips() {
        String table = "dt64_ns_" + System.nanoTime();
        // 2024-03-15 12:30:45.123456789 UTC — full nanosecond precision.
        String literal = "2024-03-15 12:30:45.123456789";
        Instant expected = Instant.parse("2024-03-15T12:30:45.123456789Z");

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id UInt32,"
                + "  ts DateTime64(9, 'UTC')"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, ts) VALUES (1, '" + literal + "')");

            try (QueryResult result = conn.query(
                    "SELECT ts FROM " + table + " ORDER BY id")) {
                List<Object[]> rows = materialize(result);
                assertEquals(1, rows.size(), "Expected 1 row from " + table);

                Object actual = rows.get(0)[0];
                assertNotNull(actual, "DateTime64(9,'UTC') value must not be null");
                assertInstanceOf(Instant.class, actual,
                        "DateTime64(9) must decode to java.time.Instant, got "
                        + actual.getClass().getName());
                assertEquals(expected, actual,
                        "DateTime64(9,'UTC') '" + literal + "' must round-trip to "
                        + expected + " — check nanosecond-scale conversion in DateTime64 codec");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Exercises the WRITE/encode path for {@code Decimal(18,4)} via a
     * {@link BulkInserter} round-trip: a matching Java record is bulk-inserted,
     * SELECTed back, and each value compared for numeric equality. This confirms
     * the codec encodes a {@link BigDecimal} field correctly on the wire as well
     * as decoding it.
     */
    @Test
    void decimalBulkInsertRoundTrips() {
        String table = "dec_bulk_" + System.nanoTime();
        List<DecimalRow> rows = List.of(
            new DecimalRow(1, new BigDecimal("0.0000")),
            new DecimalRow(2, new BigDecimal("3.1416")),
            new DecimalRow(3, new BigDecimal("-271828.1828")),
            new DecimalRow(4, new BigDecimal("99999999999999.9999")),
            new DecimalRow(5, new BigDecimal("-99999999999999.9999")));

        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id  UInt32,"
                + "  d64 Decimal(18, 4)"
                + ") ENGINE = MergeTree() ORDER BY id");

            try (BulkInserter<DecimalRow> inserter =
                    conn.createBulkInserter(table, DecimalRow.class)) {
                inserter.init();
                inserter.addRange(rows);
                inserter.complete();
            }

            try (QueryResult result = conn.query(
                    "SELECT d64 FROM " + table + " ORDER BY id")) {
                List<Object[]> back = materialize(result);
                assertEquals(rows.size(), back.size(),
                        "Expected " + rows.size() + " bulk-inserted Decimal rows from " + table);

                for (int i = 0; i < rows.size(); i++) {
                    BigDecimal expected = rows.get(i).d64();
                    Object actual = back.get(i)[0];
                    assertInstanceOf(BigDecimal.class, actual,
                            "Bulk Decimal(18,4) row " + i + " must decode to BigDecimal");
                    assertEquals(0, expected.compareTo((BigDecimal) actual),
                            "Bulk Decimal(18,4) row " + i + ": expected " + expected
                            + " but got " + actual
                            + " — check BigDecimal encode path in Decimal codec");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Inserts an array of {@link BigDecimal} values as literal rows into the
     * given table's {@code (id, <decimal>)} schema. The decimal column is
     * assumed to be the second declared column; {@code id} is the 1-based row
     * index so {@code ORDER BY id} returns the values in insertion order.
     *
     * @param conn   the open connection
     * @param table  the target table name
     * @param inputs the decimal values to insert, in order
     */
    private void insertDecimals(ClickHouseConnection conn, String table, BigDecimal[] inputs) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        for (int i = 0; i < inputs.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            // toPlainString() avoids scientific notation that ClickHouse would reject.
            sql.append('(').append(i + 1).append(", ")
               .append(inputs[i].toPlainString()).append(')');
        }
        conn.execute(sql.toString());
    }

    /**
     * SELECTs the decimal column (in insertion order) and asserts each returned
     * value is a {@link BigDecimal} numerically equal to the corresponding input
     * (via {@link BigDecimal#compareTo} so differing scale does not fail).
     *
     * @param conn     the open connection
     * @param table    the table to read from
     * @param typeName the ClickHouse type label, used only in failure messages
     * @param inputs   the expected values, in insertion order
     */
    private void assertDecimalsRoundTrip(ClickHouseConnection conn, String table,
                                         String typeName, BigDecimal[] inputs) {
        try (QueryResult result = conn.query(
                "SELECT " + decimalColumn(table) + " FROM " + table + " ORDER BY id")) {
            List<Object[]> rows = materialize(result);
            assertEquals(inputs.length, rows.size(),
                    "Expected " + inputs.length + " " + typeName + " rows from " + table);

            for (int i = 0; i < inputs.length; i++) {
                Object actual = rows.get(i)[0];
                assertNotNull(actual,
                        typeName + " row " + i + " (" + inputs[i] + ") must not be null");
                assertInstanceOf(BigDecimal.class, actual,
                        typeName + " row " + i + " must decode to BigDecimal, got "
                        + actual.getClass().getName());
                assertEquals(0, inputs[i].compareTo((BigDecimal) actual),
                        typeName + " row " + i + ": expected " + inputs[i]
                        + " but got " + actual
                        + " — check wide-integer encode/decode in Decimal codec");
            }
        }
    }

    /**
     * Resolves the decimal column name for a given table created by this class.
     * The CREATE TABLE statements name the decimal column after the type width
     * ({@code d32}/{@code d64}/{@code d128}/{@code d256}); this maps the table
     * prefix to that column name. Using {@code SELECT *} would also work but an
     * explicit column keeps the result deterministic.
     *
     * @param table the table name (carries the width prefix)
     * @return the decimal column identifier for that table
     */
    private String decimalColumn(String table) {
        if (table.startsWith("dec_d32_")) {
            return "d32";
        }
        if (table.startsWith("dec_d64_")) {
            return "d64";
        }
        if (table.startsWith("dec_d128_")) {
            return "d128";
        }
        if (table.startsWith("dec_d256_")) {
            return "d256";
        }
        throw new IllegalArgumentException("Unknown decimal table: " + table);
    }

    /**
     * Row type for the {@code Decimal(18,4)} bulk-insert round-trip. Field names
     * match the {@code (id, d64)} column names so
     * {@link io.github.danielbunting.clickhouse.mapping.RowMappers#forClass(Class)}
     * can map by name.
     *
     * @param id  the UInt32 primary key / sort column
     * @param d64 the {@code Decimal(18,4)} value
     */
    record DecimalRow(int id, BigDecimal d64) {}
}
