package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips {@code Array(T)} columns whose inner type {@code T} is richer than the
 * widths exercised by {@link ArrayAndNullableIT} (which covers
 * {@code Array(UInt32/String/Nullable(String)/Array(UInt32))}). The focus here is the
 * inner-codec dispatch inside {@link io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec}
 * for temporal, decimal, uuid, enum, float, and nested-nullable element types.
 *
 * <p>Decode path: each row is inserted via a raw SQL array literal (server encodes), then
 * read back through {@link #decode}; the array cell must materialise as a
 * {@link java.util.List} and is asserted element-wise with the correct per-type comparison
 * (compareTo for {@link BigDecimal}, isNaN/isInfinite for special floats, equals otherwise).
 *
 * <p>One bulk-encode case ({@code Array(Int64)} via a {@code record R(long id, List<Long> arr)})
 * proves the client-side array WRITE path for a numeric inner type.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class ArrayExtraTypesIT extends TypeRoundTripBase {

    /** Returns the single array cell (column index 1) of the single returned row as a List. */
    private List<?> singleArrayCell(List<Object[]> rows, String label) {
        assertEquals(1, rows.size(), label + ": expected exactly 1 row");
        Object cell = rows.get(0)[1];
        return assertInstanceOf(List.class, cell,
                label + ": expected a java.util.List (Array codec) but got "
                        + (cell == null ? "null" : cell.getClass().getName()));
    }

    /**
     * {@code Array(Float64)} including the special literals {@code nan} and {@code inf}.
     * Asserts ordinary doubles by value and the special slots via isNaN / isInfinite.
     */
    @Test
    void arrayFloat64WithSpecials() {
        withTable("arr_f64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Float64)) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES"
                    + " (1, [1.5, 3.5, nan, inf])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(Float64)");
            assertEquals(4, arr.size(), "Array(Float64) length");
            assertEquals(1.5, ((Number) arr.get(0)).doubleValue(), 0.0, "elem 0");
            assertEquals(3.5, ((Number) arr.get(1)).doubleValue(), 0.0, "elem 1");
            assertTrue(Double.isNaN(((Number) arr.get(2)).doubleValue()), "elem 2 must be NaN");
            assertTrue(Double.isInfinite(((Number) arr.get(3)).doubleValue()),
                    "elem 3 must be +Inf");
        });
    }

    /** {@code Array(Decimal(18,4))} — element-wise {@link BigDecimal#compareTo}. */
    @Test
    void arrayDecimal() {
        withTable("arr_dec", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Decimal(18,4))) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES (1, [1.23, -4.5])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(Decimal)");
            assertEquals(2, arr.size(), "Array(Decimal) length");
            assertEquals(0, ((BigDecimal) arr.get(0)).compareTo(new BigDecimal("1.23")), "elem 0");
            assertEquals(0, ((BigDecimal) arr.get(1)).compareTo(new BigDecimal("-4.5")), "elem 1");
        });
    }

    /** {@code Array(Date)} — element-wise {@link LocalDate} equality. */
    @Test
    void arrayDate() {
        withTable("arr_date", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Date)) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table
                    + " (id, arr) VALUES (1, ['1970-01-01', '2024-03-15'])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(Date)");
            assertEquals(List.of(LocalDate.of(1970, 1, 1), LocalDate.of(2024, 3, 15)), arr,
                    "Array(Date) elements");
        });
    }

    /** {@code Array(DateTime('UTC'))} — element-wise {@link Instant} equality. */
    @Test
    void arrayDateTime() {
        withTable("arr_dt", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(DateTime('UTC'))) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES"
                    + " (1, ['2024-03-15 12:00:00', '2024-03-15 13:30:45'])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(DateTime)");
            assertEquals(List.of(
                            Instant.parse("2024-03-15T12:00:00Z"),
                            Instant.parse("2024-03-15T13:30:45Z")),
                    arr, "Array(DateTime) elements");
        });
    }

    /** {@code Array(UUID)} — nil UUID and a known UUID. */
    @Test
    void arrayUuid() {
        withTable("arr_uuid", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(UUID)) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES"
                    + " (1, ['00000000-0000-0000-0000-000000000000',"
                    + " '61f0c404-5cb3-11e7-907b-a6006ad3dba0'])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(UUID)");
            assertEquals(List.of(
                            new UUID(0L, 0L),
                            UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0")),
                    arr, "Array(UUID) elements");
        });
    }

    /** {@code Array(Enum8(...))} — decodes to the String names. */
    @Test
    void arrayEnum8() {
        withTable("arr_enum", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Enum8('a' = 1, 'b' = 2)))"
                    + " ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES (1, ['a', 'b'])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(Enum8)");
            assertEquals(List.of("a", "b"), arr, "Array(Enum8) names");
        });
    }

    /** {@code Array(Nullable(Int64))} — a null in the middle must decode to {@code null}. */
    @Test
    void arrayNullableInt64() {
        withTable("arr_nint", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Nullable(Int64))) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES (1, [1, NULL, 3])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(Nullable(Int64))");
            assertEquals(3, arr.size(), "length");
            assertEquals(1L, ((Number) arr.get(0)).longValue(), "elem 0");
            assertNull(arr.get(1), "elem 1 must be null");
            assertEquals(3L, ((Number) arr.get(2)).longValue(), "elem 2");
        });
    }

    /** Empty {@code Array(Int64)} must decode to an empty (non-null) List. */
    @Test
    void arrayEmpty() {
        withTable("arr_empty", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Int64)) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, arr) VALUES (1, [])");

            List<?> arr = singleArrayCell(
                    decode(conn, "SELECT id, arr FROM " + table + " ORDER BY id"),
                    "Array(Int64) empty");
            assertTrue(arr.isEmpty(), "empty array must decode to an empty List");
        });
    }

    /**
     * Record for the {@code Array(Int64)} bulk-encode round trip. Field {@code arr} is a
     * {@code List<Long>} so the mapper binds it to the array WRITE path.
     */
    record LongArrRow(long id, List<Long> arr) {}

    /**
     * ENCODE: bulk-inserts {@code Array(Int64)} rows defined as a Java record, then reads
     * them back and asserts the list structure round-trips (proves the client-side array
     * encode for a non-UInt32 numeric inner type).
     */
    @Test
    void arrayInt64BulkEncodeRoundTrips() {
        withTable("arr_i64_bulk", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, arr Array(Int64)) ENGINE = MergeTree() ORDER BY id");

            List<LongArrRow> input = List.of(
                    new LongArrRow(1, List.of()),
                    new LongArrRow(2, List.of(-5_000_000_000L)),
                    new LongArrRow(3, Arrays.asList(1L, 2L, 9_223_372_036_854_775_807L)));

            try (BulkInserter<LongArrRow> inserter =
                         conn.createBulkInserter(table, LongArrRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT id, arr FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk rows");

            List<?> r1 = assertInstanceOf(List.class, rows.get(0)[1], "row1");
            assertTrue(r1.isEmpty(), "row1 empty array encode");
            List<?> r2 = assertInstanceOf(List.class, rows.get(1)[1], "row2");
            assertEquals(List.of(-5_000_000_000L), r2, "row2 single element encode");
            List<?> r3 = assertInstanceOf(List.class, rows.get(2)[1], "row3");
            assertEquals(List.of(1L, 2L, 9_223_372_036_854_775_807L), r3,
                    "row3 multi element encode");
        });
    }
}
