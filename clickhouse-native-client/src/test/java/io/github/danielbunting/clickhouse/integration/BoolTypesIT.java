package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips the ClickHouse {@code Bool} type through a real server in both
 * directions: DECODE (raw {@code INSERT ... VALUES}) and ENCODE (bulk insert of
 * a mapped record with a primitive {@code boolean} field, which exercises the
 * object-mapper path into {@code codec.set(Boolean)}).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class BoolTypesIT extends TypeRoundTripBase {

    /** A primitive {@code boolean} field goes through the mapper OBJECT path. */
    record BoolRow(int id, boolean b) {}

    private static final String COLUMNS = "id UInt32, b Bool";
    private static final String SELECT_COLS = "id, b";

    /**
     * DECODE: server encodes literal {@code true}/{@code false}, client decodes.
     */
    @Test
    void boolDecodeRoundTrip() {
        withTable("bool_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (" + SELECT_COLS + ") VALUES "
                    + "(1, true), (2, false)");

            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows from " + table);

            assertEquals(Boolean.TRUE, rows.get(0)[1], "row 1 Bool true");
            assertEquals(Boolean.FALSE, rows.get(1)[1], "row 2 Bool false");
        });
    }

    /**
     * ENCODE + MAPPED-READ: client encodes records (primitive {@code boolean}
     * field -> mapper object path -> {@code codec.set(Boolean)}), reads back via
     * the block API and {@code query(sql, Class)}.
     */
    @Test
    void boolEncodeAndMappedReadRoundTrip() {
        withTable("bool_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<BoolRow> input = List.of(
                    new BoolRow(1, true),
                    new BoolRow(2, false));

            try (BulkInserter<BoolRow> inserter =
                    conn.createBulkInserter(table, BoolRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 bulk-inserted rows");
            assertEquals(Boolean.TRUE, rows.get(0)[1], "row 1 Bool true");
            assertEquals(Boolean.FALSE, rows.get(1)[1], "row 2 Bool false");

            // (b) mapped-read via query(sql, Class)
            List<BoolRow> mapped;
            try (var stream = conn.query(
                    "SELECT " + SELECT_COLS + " FROM " + table + " ORDER BY id",
                    BoolRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped,
                    "Mapped-read records must equal the inserted records exactly");
        });
    }
}
