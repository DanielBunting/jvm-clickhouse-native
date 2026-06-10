package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips the ClickHouse {@code Date32} type through a real server in both
 * directions: DECODE (raw {@code INSERT ... VALUES}) and ENCODE (bulk insert of
 * a mapped record with a {@link LocalDate} field).
 *
 * <p>{@code Date32} is a signed Int32 day-offset from 1970-01-01 (negative
 * before 1970), supported display range 1900-01-01..2299-12-31. The
 * 1900-01-01 case empirically confirms the epoch base: if the base were wrong,
 * the negative offset would not round-trip to {@code LocalDate.of(1900, 1, 1)}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class Date32TypesIT extends TypeRoundTripBase {

    /** A {@link LocalDate} field exercises the mapper LocalDate coercion path. */
    record DateRow(int id, LocalDate d) {}

    private static final String COLUMNS = "id UInt32, d Date32";
    private static final String SELECT_COLS = "id, d";

    private static final LocalDate D_1900 = LocalDate.of(1900, 1, 1);
    private static final LocalDate D_1970 = LocalDate.of(1970, 1, 1);
    private static final LocalDate D_2024 = LocalDate.of(2024, 3, 15);
    private static final LocalDate D_2299 = LocalDate.of(2299, 1, 1);

    /**
     * DECODE: server encodes literal dates spanning the negative (pre-1970),
     * epoch, and far-future range; client decodes them back.
     */
    @Test
    void date32DecodeRoundTrip() {
        withTable("date32_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (" + SELECT_COLS + ") VALUES "
                    + "(1, '1900-01-01'), (2, '1970-01-01'), "
                    + "(3, '2024-03-15'), (4, '2299-01-01')");

            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size(), "Expected 4 rows from " + table);

            assertEquals(D_1900, rows.get(0)[1], "1900-01-01 (negative offset)");
            assertEquals(D_1970, rows.get(1)[1], "1970-01-01 (epoch base)");
            assertEquals(D_2024, rows.get(2)[1], "2024-03-15");
            assertEquals(D_2299, rows.get(3)[1], "2299-01-01");
        });
    }

    /**
     * ENCODE + MAPPED-READ: client encodes records (LocalDate field), reads back
     * via the block API and {@code query(sql, Class)}.
     */
    @Test
    void date32EncodeAndMappedReadRoundTrip() {
        withTable("date32_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<DateRow> input = List.of(
                    new DateRow(1, D_1900),
                    new DateRow(2, D_1970),
                    new DateRow(3, D_2024),
                    new DateRow(4, D_2299));

            try (BulkInserter<DateRow> inserter =
                    conn.createBulkInserter(table, DateRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(4, rows.size(), "Expected 4 bulk-inserted rows");
            assertEquals(D_1900, rows.get(0)[1], "1900-01-01 (negative offset)");
            assertEquals(D_1970, rows.get(1)[1], "1970-01-01 (epoch base)");
            assertEquals(D_2024, rows.get(2)[1], "2024-03-15");
            assertEquals(D_2299, rows.get(3)[1], "2299-01-01");

            // (b) mapped-read via query(sql, Class)
            List<DateRow> mapped;
            try (var stream = conn.query(
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id",
                    DateRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped,
                    "Mapped-read records must equal the inserted records exactly");
        });
    }
}
