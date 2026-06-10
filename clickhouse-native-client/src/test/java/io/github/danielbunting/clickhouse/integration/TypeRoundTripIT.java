package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests that INSERT known literal rows, SELECT them back through
 * {@link QueryResult}, and assert each column value round-tripped exactly.
 *
 * <p>Column access is done through the low-level
 * {@link Column#codec()}{@link io.github.danielbunting.clickhouse.types.ColumnCodec#get(Object, int) .get(values, row)}
 * API so that bugs in any layer (codec, compression, block framing) surface
 * with a clear column+row label.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class TypeRoundTripIT extends TypeRoundTripBase {

    /**
     * Round-trips UInt32, Int64, and Float64 through a MergeTree table using
     * literal INSERT VALUES.
     *
     * <p>UInt32 is returned as a {@code long} (unsigned 32-bit fits in long),
     * Int64 as {@code long}, Float64 as {@code double}. The assertions use
     * delta-free {@link org.junit.jupiter.api.Assertions#assertEquals(double, double, double)}
     * for the float column.
     */
    @Test
    void numericColumnsRoundTrip() {
        String table = "type_rt_numeric_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  u32  UInt32,"
                + "  i64  Int64,"
                + "  f64  Float64"
                + ") ENGINE = MergeTree() ORDER BY u32");

            conn.execute(
                "INSERT INTO " + table + " (u32, i64, f64) VALUES"
                + " (0, -9223372036854775808, -1.5),"
                + " (4294967295, 9223372036854775807, 3.14159265358979)");

            try (QueryResult result = conn.query(
                    "SELECT u32, i64, f64 FROM " + table + " ORDER BY u32")) {

                List<Object[]> rows = materialize(result);

                assertEquals(2, rows.size(),
                        "Expected 2 rows from " + table + " — check MergeTree ORDER BY / INSERT");

                // Row 0: u32=0, i64=MIN_LONG, f64=-1.5
                Object[] r0 = rows.get(0);
                assertEquals(0L, ((Number) r0[0]).longValue(),
                        "Row 0 u32: expected 0 — UInt32 codec may have sign-extension bug");
                assertEquals(Long.MIN_VALUE, ((Number) r0[1]).longValue(),
                        "Row 0 i64: expected Long.MIN_VALUE");
                assertEquals(-1.5, ((Number) r0[2]).doubleValue(), 1e-12,
                        "Row 0 f64: expected -1.5 — Float64 codec precision bug");

                // Row 1: u32=4294967295 (UInt32 max), i64=MAX_LONG, f64=pi
                Object[] r1 = rows.get(1);
                assertEquals(4_294_967_295L, ((Number) r1[0]).longValue(),
                        "Row 1 u32: expected 4294967295 (UInt32 max) — check unsigned handling");
                assertEquals(Long.MAX_VALUE, ((Number) r1[1]).longValue(),
                        "Row 1 i64: expected Long.MAX_VALUE");
                assertEquals(3.14159265358979, ((Number) r1[2]).doubleValue(), 1e-12,
                        "Row 1 f64: expected 3.14159265358979 — Float64 precision bug");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a String column through the wire protocol.
     *
     * <p>Tests empty string, ASCII, and a UTF-8 multi-byte value to catch
     * VarInt length-prefix bugs in {@code StringColumnCodec}.
     */
    @Test
    void stringColumnRoundTrips() {
        String table = "type_rt_string_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id UInt32,"
                + "  s  String"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, s) VALUES"
                + " (1, ''),"
                + " (2, 'hello world'),"
                + " (3, 'unicode: éàü')");

            try (QueryResult result = conn.query(
                    "SELECT id, s FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);

                assertEquals(3, rows.size(), "Expected 3 rows from " + table);

                assertEquals("", rows.get(0)[1],
                        "Row 1 s: expected empty string — check zero-length VarInt string codec");
                assertEquals("hello world", rows.get(1)[1],
                        "Row 2 s: expected 'hello world'");
                assertEquals("unicode: éàü", rows.get(2)[1],
                        "Row 3 s: expected UTF-8 multi-byte string — check byte count vs char count");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips Date and DateTime columns.
     *
     * <p>Date is stored on the wire as days-since-epoch (UInt16), DateTime as
     * Unix epoch-seconds (UInt32). Values are returned by the codec as
     * {@code Integer} (Date) and {@code Long} (DateTime).
     *
     * <p>VERIFY: the codec's {@code get()} return types match whatever
     * {@code DateCodec} and {@code DateTimeCodec} produce — this test assumes
     * {@code Number} for both. // VERIFY against CH.Native
     */
    @Test
    void dateAndDateTimeRoundTrip() {
        String table = "type_rt_dates_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id UInt32,"
                + "  d  Date,"
                // Pin the column timezone so the literal below parses in UTC
                // regardless of the container's session timezone.
                + "  dt DateTime('UTC')"
                + ") ENGINE = MergeTree() ORDER BY id");

            // 2024-03-15 = days since epoch 19797
            // 2024-03-15 12:30:00 UTC = 1710460800 (midnight) + 45000 = 1710505800
            conn.execute(
                "INSERT INTO " + table + " (id, d, dt) VALUES"
                + " (1, '2024-03-15', '2024-03-15 12:30:00')");

            try (QueryResult result = conn.query(
                    "SELECT id, toUnixTimestamp(d) AS d_epoch,"
                    + "       toUnixTimestamp(dt) AS dt_epoch"
                    + " FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);

                assertEquals(1, rows.size(), "Expected 1 row from " + table);

                Object[] r0 = rows.get(0);
                // d_epoch: midnight UTC for 2024-03-15
                long expectedDateEpoch = 1710460800L; // 2024-03-15T00:00:00Z
                assertEquals(expectedDateEpoch, ((Number) r0[1]).longValue(),
                        "Date '2024-03-15' epoch must be " + expectedDateEpoch
                        + " — check DateCodec days-to-epoch conversion");

                long expectedDtEpoch = 1710505800L; // 2024-03-15T12:30:00Z
                assertEquals(expectedDtEpoch, ((Number) r0[2]).longValue(),
                        "DateTime '2024-03-15 12:30:00' epoch must be " + expectedDtEpoch
                        + " — check DateTimeCodec epoch-seconds encoding");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Round-trips a {@code Nullable(String)} column, inserting both non-null
     * and null values, then asserting that null-map is decoded correctly.
     *
     * <p>A null-map decoding bug would manifest as either a non-null value
     * appearing null, or a null value appearing as empty-string or garbage.
     */
    @Test
    void nullableStringRoundTrips() {
        String table = "type_rt_nullable_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  id UInt32,"
                + "  ns Nullable(String)"
                + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute(
                "INSERT INTO " + table + " (id, ns) VALUES"
                + " (1, 'present'),"
                + " (2, NULL),"
                + " (3, 'also present')");

            try (QueryResult result = conn.query(
                    "SELECT id, ns FROM " + table + " ORDER BY id")) {

                List<Object[]> rows = materialize(result);

                assertEquals(3, rows.size(), "Expected 3 rows from " + table);

                assertEquals("present", rows.get(0)[1],
                        "Row 1 ns: expected 'present' — null-map codec bug");
                assertNull(rows.get(1)[1],
                        "Row 2 ns: expected null — null-map bit not set or not read");
                assertEquals("also present", rows.get(2)[1],
                        "Row 3 ns: expected 'also present' — null-map codec corrupted adjacent value");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Comprehensive multi-type table: UInt32, Int64, Float64, String, Date,
     * DateTime, Nullable(String).  All columns in a single table to verify
     * that column ordering and block framing handle mixed type widths correctly.
     *
     * <p>VERIFY: DateTime timezone handling — the container's ClickHouse server
     * may use a default timezone; if values come back in local time rather than
     * UTC the epoch comparison will fail. // VERIFY against CH.Native
     */
    @Test
    void allTypeColumnsRoundTrip() {
        String table = "type_rt_all_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "  u32  UInt32,"
                + "  i64  Int64,"
                + "  f64  Float64,"
                + "  s    String,"
                + "  d    Date,"
                + "  dt   DateTime('UTC'),"
                + "  ns   Nullable(String)"
                + ") ENGINE = MergeTree() ORDER BY u32");

            conn.execute(
                "INSERT INTO " + table + " (u32, i64, f64, s, d, dt, ns) VALUES"
                + " (1, 100, 1.1, 'row1', '2024-01-01', '2024-01-01 00:00:01', 'nullable1'),"
                + " (2, 200, 2.2, 'row2', '2024-06-15', '2024-06-15 12:00:00', NULL)");

            try (QueryResult result = conn.query(
                    "SELECT u32, i64, f64, s,"
                    + "       toUnixTimestamp(d)  AS d_ts,"
                    + "       toUnixTimestamp(dt) AS dt_ts,"
                    + "       ns"
                    + " FROM " + table + " ORDER BY u32")) {

                List<String> colNames = result.columnNames();
                assertNotNull(colNames, "columnNames() must not be null");
                assertEquals(7, colNames.size(),
                        "Expected 7 columns in result: " + colNames);

                List<Object[]> rows = materialize(result);
                assertEquals(2, rows.size(), "Expected 2 rows from " + table);

                // Row 0
                Object[] r0 = rows.get(0);
                assertEquals(1L, ((Number) r0[0]).longValue(), "r0.u32");
                assertEquals(100L, ((Number) r0[1]).longValue(), "r0.i64");
                assertEquals(1.1, ((Number) r0[2]).doubleValue(), 1e-9, "r0.f64");
                assertEquals("row1", r0[3], "r0.s");
                // 2024-01-01 UTC midnight = 1704067200
                assertEquals(1704067200L, ((Number) r0[4]).longValue(),
                        "r0.d_ts — Date 2024-01-01 midnight UTC epoch mismatch");
                // 2024-01-01 00:00:01 UTC = 1704067201
                assertEquals(1704067201L, ((Number) r0[5]).longValue(),
                        "r0.dt_ts — DateTime 2024-01-01 00:00:01 UTC epoch mismatch");
                assertEquals("nullable1", r0[6], "r0.ns");

                // Row 1
                Object[] r1 = rows.get(1);
                assertEquals(2L, ((Number) r1[0]).longValue(), "r1.u32");
                assertEquals(200L, ((Number) r1[1]).longValue(), "r1.i64");
                assertEquals(2.2, ((Number) r1[2]).doubleValue(), 1e-9, "r1.f64");
                assertEquals("row2", r1[3], "r1.s");
                // 2024-06-15 UTC midnight = 1718409600
                assertEquals(1718409600L, ((Number) r1[4]).longValue(),
                        "r1.d_ts — Date 2024-06-15 midnight UTC epoch mismatch");
                // 2024-06-15 12:00:00 UTC = 1718452800
                assertEquals(1718452800L, ((Number) r1[5]).longValue(),
                        "r1.dt_ts — DateTime 2024-06-15 12:00:00 UTC epoch mismatch");
                assertNull(r1[6], "r1.ns — expected null, null-map not decoded correctly");
            }

            conn.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
