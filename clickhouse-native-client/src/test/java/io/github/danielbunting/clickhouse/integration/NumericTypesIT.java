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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for the numeric ClickHouse types that are <em>not</em>
 * already covered by {@code TypeRoundTripIT} (which exercises UInt32, Int64 and
 * Float64).
 *
 * <p>This class round-trips the remaining numeric types against a live
 * ClickHouse server: {@code UInt8}, {@code UInt16}, {@code UInt64},
 * {@code Int8}, {@code Int16} and {@code Float32}, plus a re-confirmation of a
 * couple of the already-tested types for good measure. Every test:
 * <ol>
 *   <li>CREATEs a uniquely-named MergeTree table (per {@link System#nanoTime()}),</li>
 *   <li>INSERTs boundary + typical literal values (min, max, 0, and for unsigned
 *       types the high-bit-set extreme),</li>
 *   <li>SELECTs the rows back and asserts <em>round-trip equality</em> against the
 *       inserted input rather than against hand-computed magic numbers,</li>
 *   <li>DROPs the table.</li>
 * </ol>
 *
 * <p>Boxing reminder (per the core codec contract):
 * UInt8/UInt16/Int16/Int32 decode to {@code Integer}, Int8 to {@code Byte},
 * UInt32/UInt64/Int64 to {@code Long}, Float32 to {@code Float}. In particular a
 * {@code UInt64} holds the <em>raw 64-bit pattern</em> in a signed {@code Long},
 * so values above {@link Long#MAX_VALUE} come back negative; those are compared
 * via {@link Long#toUnsignedString(long)} / the matching raw-bit {@code Long}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class NumericTypesIT extends IntegrationTestBase {

    /**
     * Record with one field per supported numeric ClickHouse type, used for the
     * {@link BulkInserter} write-path round-trip.
     *
     * <p>Field names must match the ClickHouse column names exactly (the
     * {@link io.github.danielbunting.clickhouse.mapping.RowMappers#forClass(Class)}
     * mapper matches by name), and the declaration order matches the
     * {@code CREATE TABLE} column order.
     *
     * @param u8  value for a {@code UInt8}  column (0..255)
     * @param u16 value for a {@code UInt16} column (0..65535)
     * @param u64 value for a {@code UInt64} column (raw 64-bit pattern in a long)
     * @param i8  value for an {@code Int8}  column (-128..127)
     * @param i16 value for an {@code Int16} column (-32768..32767)
     * @param f32 value for a {@code Float32} column
     */
    record NumericRow(int u8, int u16, long u64, byte i8, short i16, float f32) {}

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
     * {@code (column, row)} value into a row-major {@code List<Object[]>} using
     * the null-aware {@link Column#value(int)} accessor.
     *
     * @param result the query result (iterated by this method)
     * @return list of rows, each an {@code Object[]} of boxed column values
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
     * Round-trips {@code UInt8} with its boundary values: 0, a typical value,
     * and the high-bit-set maximum 255 (which a sign-extension bug would turn
     * into -1 or 255-as-negative-byte).
     */
    @Test
    void uint8RoundTrip() {
        String table = "num_uint8_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  v   UInt8"
                + ") ENGINE = MergeTree() ORDER BY rid");

            conn.execute(
                "INSERT INTO " + table + " (rid, v) VALUES"
                + " (1, 0), (2, 1), (3, 127), (4, 128), (5, 255)");

            try (QueryResult result = conn.query(
                    "SELECT rid, v FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                long[] expected = {0, 1, 127, 128, 255};
                assertEquals(expected.length, rows.size(),
                        "Expected " + expected.length + " UInt8 rows from " + table);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(expected[i], ((Number) rows.get(i)[1]).longValue(),
                            "UInt8 value " + expected[i]
                            + " round-tripped wrong — check unsigned-byte zero extension");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code UInt16} with 0, a typical value, the signed-short edge
     * (32768, high bit set) and the unsigned maximum 65535.
     */
    @Test
    void uint16RoundTrip() {
        String table = "num_uint16_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  v   UInt16"
                + ") ENGINE = MergeTree() ORDER BY rid");

            conn.execute(
                "INSERT INTO " + table + " (rid, v) VALUES"
                + " (1, 0), (2, 12345), (3, 32767), (4, 32768), (5, 65535)");

            try (QueryResult result = conn.query(
                    "SELECT rid, v FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                long[] expected = {0, 12345, 32767, 32768, 65535};
                assertEquals(expected.length, rows.size(),
                        "Expected " + expected.length + " UInt16 rows from " + table);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(expected[i], ((Number) rows.get(i)[1]).longValue(),
                            "UInt16 value " + expected[i]
                            + " round-tripped wrong — check unsigned-short zero extension");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code UInt64}, including the maximum value
     * {@code 18446744073709551615} whose raw 64-bit pattern is
     * {@code 0xFFFFFFFFFFFFFFFF}. That comes back as the signed {@code Long}
     * {@code -1}, so it is compared both via {@link Long#toUnsignedString(long)}
     * and via the matching raw-bit {@code Long} ({@code -1L}).
     */
    @Test
    void uint64RoundTrip() {
        String table = "num_uint64_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  v   UInt64"
                + ") ENGINE = MergeTree() ORDER BY rid");

            // Decimal literals: 0, a typical value, Long.MAX_VALUE, Long.MAX+1
            // (high bit set), and UInt64 max (all bits set).
            conn.execute(
                "INSERT INTO " + table + " (rid, v) VALUES"
                + " (1, 0),"
                + " (2, 42),"
                + " (3, 9223372036854775807),"
                + " (4, 9223372036854775808),"
                + " (5, 18446744073709551615)");

            try (QueryResult result = conn.query(
                    "SELECT rid, v FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                assertEquals(5, rows.size(), "Expected 5 UInt64 rows from " + table);

                // Expected raw 64-bit patterns held in a signed long.
                long[] expectedRaw = {
                        0L,
                        42L,
                        Long.MAX_VALUE,               // 9223372036854775807
                        Long.MIN_VALUE,               // 9223372036854775808 (high bit set)
                        -1L                           // 18446744073709551615 (all bits set)
                };
                String[] expectedUnsigned = {
                        "0",
                        "42",
                        "9223372036854775807",
                        "9223372036854775808",
                        "18446744073709551615"
                };
                for (int i = 0; i < expectedRaw.length; i++) {
                    long actual = ((Number) rows.get(i)[1]).longValue();
                    assertEquals(expectedRaw[i], actual,
                            "UInt64 row " + i + " raw-bit pattern mismatch (expected unsigned "
                            + expectedUnsigned[i] + ")");
                    assertEquals(expectedUnsigned[i], Long.toUnsignedString(actual),
                            "UInt64 row " + i + " unsigned decimal mismatch"
                            + " — check unsigned 64-bit decode");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code Int8} across its full signed range: minimum -128, -1,
     * 0, a typical positive value, and maximum 127.
     */
    @Test
    void int8RoundTrip() {
        String table = "num_int8_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  v   Int8"
                + ") ENGINE = MergeTree() ORDER BY rid");

            conn.execute(
                "INSERT INTO " + table + " (rid, v) VALUES"
                + " (1, -128), (2, -1), (3, 0), (4, 42), (5, 127)");

            try (QueryResult result = conn.query(
                    "SELECT rid, v FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                long[] expected = {-128, -1, 0, 42, 127};
                assertEquals(expected.length, rows.size(),
                        "Expected " + expected.length + " Int8 rows from " + table);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(expected[i], ((Number) rows.get(i)[1]).longValue(),
                            "Int8 value " + expected[i]
                            + " round-tripped wrong — check signed-byte decode");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code Int16} across its full signed range: minimum -32768,
     * -1, 0, a typical positive value, and maximum 32767.
     */
    @Test
    void int16RoundTrip() {
        String table = "num_int16_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  v   Int16"
                + ") ENGINE = MergeTree() ORDER BY rid");

            conn.execute(
                "INSERT INTO " + table + " (rid, v) VALUES"
                + " (1, -32768), (2, -1), (3, 0), (4, 12345), (5, 32767)");

            try (QueryResult result = conn.query(
                    "SELECT rid, v FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                long[] expected = {-32768, -1, 0, 12345, 32767};
                assertEquals(expected.length, rows.size(),
                        "Expected " + expected.length + " Int16 rows from " + table);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(expected[i], ((Number) rows.get(i)[1]).longValue(),
                            "Int16 value " + expected[i]
                            + " round-tripped wrong — check signed-short decode");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips {@code Float32}, asserting against exactly-representable
     * binary fractions (so no precision slop is needed) plus a negative value.
     *
     * <p>The values 0.5, -1.5 and 0.0 are exact in IEEE-754 single precision, so
     * the comparison uses a zero delta. A non-power-of-two value (0.1f) is also
     * checked with a small float ULP delta to confirm the 4-byte little-endian
     * decode rather than a Float64 mis-read.
     */
    @Test
    void float32RoundTrip() {
        String table = "num_float32_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  v   Float32"
                + ") ENGINE = MergeTree() ORDER BY rid");

            conn.execute(
                "INSERT INTO " + table + " (rid, v) VALUES"
                + " (1, 0), (2, 0.5), (3, -1.5), (4, 0.1)");

            try (QueryResult result = conn.query(
                    "SELECT rid, v FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                assertEquals(4, rows.size(), "Expected 4 Float32 rows from " + table);

                assertEquals(0.0f, ((Number) rows.get(0)[1]).floatValue(), 0.0f,
                        "Float32 0.0 round-tripped wrong");
                assertEquals(0.5f, ((Number) rows.get(1)[1]).floatValue(), 0.0f,
                        "Float32 0.5 (exact) round-tripped wrong");
                assertEquals(-1.5f, ((Number) rows.get(2)[1]).floatValue(), 0.0f,
                        "Float32 -1.5 (exact) round-tripped wrong");
                assertEquals(0.1f, ((Number) rows.get(3)[1]).floatValue(), 1e-7f,
                        "Float32 0.1 round-tripped wrong — check 4-byte LE Float32 decode");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Re-confirms a couple of the already-covered numeric types (UInt32, Int64,
     * Float64) here too, so this class is self-contained and a regression in any
     * of those still surfaces if {@code TypeRoundTripIT} is skipped.
     */
    @Test
    void uint32Int64Float64Reconfirm() {
        String table = "num_reconfirm_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  rid UInt32,"
                + "  u32 UInt32,"
                + "  i64 Int64,"
                + "  f64 Float64"
                + ") ENGINE = MergeTree() ORDER BY rid");

            conn.execute(
                "INSERT INTO " + table + " (rid, u32, i64, f64) VALUES"
                + " (1, 0, -9223372036854775808, -2.5),"
                + " (2, 4294967295, 9223372036854775807, 2.5)");

            try (QueryResult result = conn.query(
                    "SELECT rid, u32, i64, f64 FROM " + table + " ORDER BY rid")) {
                List<Object[]> rows = materialize(result);
                assertEquals(2, rows.size(), "Expected 2 rows from " + table);

                Object[] r0 = rows.get(0);
                assertEquals(0L, ((Number) r0[1]).longValue(), "r0.u32");
                assertEquals(Long.MIN_VALUE, ((Number) r0[2]).longValue(), "r0.i64 min");
                assertEquals(-2.5, ((Number) r0[3]).doubleValue(), 0.0, "r0.f64");

                Object[] r1 = rows.get(1);
                assertEquals(4_294_967_295L, ((Number) r1[1]).longValue(), "r1.u32 max");
                assertEquals(Long.MAX_VALUE, ((Number) r1[2]).longValue(), "r1.i64 max");
                assertEquals(2.5, ((Number) r1[3]).doubleValue(), 0.0, "r1.f64");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Exercises the WRITE/encode path: bulk-inserts a {@link NumericRow} whose
     * fields span every supported numeric type (including the UInt8/UInt16/
     * UInt64 high-bit-set extremes and the Int8/Int16 minima), then SELECTs the
     * rows back and asserts each field round-tripped, mirroring the unsigned
     * raw-bit handling of {@link #uint64RoundTrip()}.
     */
    @Test
    void bulkInsertMixedNumericRoundTrip() {
        String table = "num_bulk_mixed_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  u8  UInt8,"
                + "  u16 UInt16,"
                + "  u64 UInt64,"
                + "  i8  Int8,"
                + "  i16 Int16,"
                + "  f32 Float32"
                + ") ENGINE = MergeTree() ORDER BY u8");

            // Three rows covering low, mid and high-bit-set extremes. The u64
            // field holds the raw 64-bit pattern: -1L == UInt64 max.
            List<NumericRow> input = List.of(
                    new NumericRow(0, 0, 0L, (byte) -128, (short) -32768, 0.0f),
                    new NumericRow(127, 32768, 42L, (byte) 0, (short) 12345, 0.5f),
                    new NumericRow(255, 65535, -1L, (byte) 127, (short) 32767, -1.5f));

            try (BulkInserter<NumericRow> inserter =
                    conn.createBulkInserter(table, NumericRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            long count = conn.executeScalar("SELECT count() FROM " + table);
            assertEquals(input.size(), count,
                    "Expected " + input.size() + " bulk-inserted rows in " + table);

            try (QueryResult result = conn.query(
                    "SELECT u8, u16, u64, i8, i16, f32 FROM " + table + " ORDER BY u8")) {
                List<Object[]> rows = materialize(result);
                assertEquals(input.size(), rows.size(),
                        "Expected " + input.size() + " rows back from " + table);

                for (int i = 0; i < input.size(); i++) {
                    NumericRow in = input.get(i);
                    Object[] out = rows.get(i);
                    assertEquals((long) in.u8(), ((Number) out[0]).longValue(),
                            "bulk row " + i + " u8 — UInt8 write/read mismatch");
                    assertEquals((long) in.u16(), ((Number) out[1]).longValue(),
                            "bulk row " + i + " u16 — UInt16 write/read mismatch");
                    long u64Actual = ((Number) out[2]).longValue();
                    assertEquals(in.u64(), u64Actual,
                            "bulk row " + i + " u64 raw-bit mismatch (expected unsigned "
                            + Long.toUnsignedString(in.u64()) + ")");
                    assertEquals(Long.toUnsignedString(in.u64()),
                            Long.toUnsignedString(u64Actual),
                            "bulk row " + i + " u64 unsigned decimal mismatch");
                    assertEquals((long) in.i8(), ((Number) out[3]).longValue(),
                            "bulk row " + i + " i8 — Int8 write/read mismatch");
                    assertEquals((long) in.i16(), ((Number) out[4]).longValue(),
                            "bulk row " + i + " i16 — Int16 write/read mismatch");
                    assertEquals(in.f32(), ((Number) out[5]).floatValue(), 0.0f,
                            "bulk row " + i + " f32 — Float32 write/read mismatch");
                }
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
