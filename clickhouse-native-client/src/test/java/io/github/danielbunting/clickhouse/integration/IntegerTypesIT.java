package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips every signed and unsigned integer width through a real server in
 * both directions: DECODE (raw {@code INSERT ... VALUES}) and ENCODE (bulk
 * insert of a mapped record), plus the {@code query(sql, Class)} mapped-read
 * path. Boundary values (min/max per width, and the {@code UInt64} high-bit-set
 * raw-bits case) are the focus.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class IntegerTypesIT extends TypeRoundTripBase {

    /**
     * Row mirroring the all-integer table. Field types match each column's
     * natural Java type so both the bulk binder (encode) and the object mapper
     * (mapped-read) bind without coercion surprises: signed widths to
     * {@code byte/short/int/long}, unsigned widths widened
     * ({@code UInt8/16 -> int}, {@code UInt32/64 -> long}; {@code UInt64} carries
     * raw bits, so its max is {@code -1L}).
     */
    record IntRow(int id, byte i8, short i16, int i32, long i64,
                  int u8, int u16, long u32, long u64) {}

    private static final String COLUMNS =
            "  id  UInt32,"
            + "  i8  Int8,"
            + "  i16 Int16,"
            + "  i32 Int32,"
            + "  i64 Int64,"
            + "  u8  UInt8,"
            + "  u16 UInt16,"
            + "  u32 UInt32,"
            + "  u64 UInt64";

    private static final String SELECT_COLS =
            "id, i8, i16, i32, i64, u8, u16, u32, u64";

    /** UInt64 max (18446744073709551615) is stored as the raw bit pattern -1L. */
    private static final long U64_MAX_BITS = -1L;

    /**
     * DECODE: server encodes literal min/max rows, client decodes them back.
     */
    @Test
    void integerDecodeRoundTrip() {
        withTable("int_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (" + SELECT_COLS + ") VALUES"
                    // row 1: minimums / zero
                    + " (1, -128, -32768, -2147483648, -9223372036854775808, 0, 0, 0, 0),"
                    // row 2: maximums (UInt64 max as decimal literal)
                    + " (2, 127, 32767, 2147483647, 9223372036854775807,"
                    + "  255, 65535, 4294967295, 18446744073709551615)");

            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows from " + table);

            assertRowMin(rows.get(0));
            assertRowMax(rows.get(1));
        });
    }

    /**
     * ENCODE + MAPPED-READ: client encodes the same boundary rows via a mapped
     * record, then they are read back through both the block API and
     * {@code query(sql, IntRow.class)}.
     */
    @Test
    void integerEncodeAndMappedReadRoundTrip() {
        withTable("int_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<IntRow> input = List.of(
                    new IntRow(1, (byte) -128, (short) -32768, Integer.MIN_VALUE,
                            Long.MIN_VALUE, 0, 0, 0L, 0L),
                    new IntRow(2, (byte) 127, (short) 32767, Integer.MAX_VALUE,
                            Long.MAX_VALUE, 255, 65535, 4_294_967_295L, U64_MAX_BITS));

            try (BulkInserter<IntRow> inserter =
                    conn.createBulkInserter(table, IntRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 bulk-inserted rows");
            assertRowMin(rows.get(0));
            assertRowMax(rows.get(1));

            // (b) mapped-read via query(sql, Class)
            List<IntRow> mapped;
            try (var stream = conn.query(
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id",
                    IntRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped,
                    "Mapped-read records must equal the inserted records exactly "
                    + "(incl. UInt64 raw-bit max as -1L)");
            assertEquals("18446744073709551615",
                    Long.toUnsignedString(mapped.get(1).u64()),
                    "UInt64 max must render as its unsigned decimal string");
        });
    }

    private static void assertRowMin(Object[] r) {
        assertEquals(1L, ((Number) r[0]).longValue(), "id");
        assertEquals(-128L, ((Number) r[1]).longValue(), "Int8 min");
        assertEquals(-32768L, ((Number) r[2]).longValue(), "Int16 min");
        assertEquals(Integer.MIN_VALUE, ((Number) r[3]).intValue(), "Int32 min");
        assertEquals(Long.MIN_VALUE, ((Number) r[4]).longValue(), "Int64 min");
        assertEquals(0L, ((Number) r[5]).longValue(), "UInt8 zero");
        assertEquals(0L, ((Number) r[6]).longValue(), "UInt16 zero");
        assertEquals(0L, ((Number) r[7]).longValue(), "UInt32 zero");
        assertEquals(0L, ((Number) r[8]).longValue(), "UInt64 zero");
    }

    private static void assertRowMax(Object[] r) {
        assertEquals(2L, ((Number) r[0]).longValue(), "id");
        assertEquals(127L, ((Number) r[1]).longValue(), "Int8 max");
        assertEquals(32767L, ((Number) r[2]).longValue(), "Int16 max");
        assertEquals(Integer.MAX_VALUE, ((Number) r[3]).intValue(), "Int32 max");
        assertEquals(Long.MAX_VALUE, ((Number) r[4]).longValue(), "Int64 max");
        assertEquals(255L, ((Number) r[5]).longValue(), "UInt8 max");
        assertEquals(65535L, ((Number) r[6]).longValue(), "UInt16 max");
        assertEquals(4_294_967_295L, ((Number) r[7]).longValue(), "UInt32 max");
        assertEquals(U64_MAX_BITS, ((Number) r[8]).longValue(), "UInt64 max raw bits");
        assertEquals("18446744073709551615",
                Long.toUnsignedString(((Number) r[8]).longValue()),
                "UInt64 max unsigned string");
    }
}
