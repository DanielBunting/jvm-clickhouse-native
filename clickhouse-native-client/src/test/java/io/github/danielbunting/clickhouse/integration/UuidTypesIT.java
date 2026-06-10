package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips the {@code UUID} type through a real server in both directions,
 * focusing on the nil UUID, multiple known values, and the mapped-read path
 * ({@code query(sql, Class)}) that the existing {@code StringLikeTypesIT} does
 * not cover.
 *
 * <p>ClickHouse stores a UUID as two little-endian 64-bit halves in swapped
 * (high, low) order; correct decode is asserted via {@link UUID#equals} against
 * {@link UUID#fromString(String)} (and {@code new UUID(0,0)} for nil).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class UuidTypesIT extends TypeRoundTripBase {

    /** Row mirroring the UUID table; field names match the column names. */
    record UuidRow(int id, UUID u) {}

    private static final UUID NIL = new UUID(0L, 0L);
    private static final UUID U1 = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
    private static final UUID U2 = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    private static final String COLUMNS =
            "  id UInt32,"
            + "  u  UUID";

    /**
     * DECODE: server encodes literal UUID values (nil + two known), client decodes
     * and the reassembled {@link UUID} must equal {@code UUID.fromString}.
     */
    @Test
    void uuidDecodeRoundTrip() {
        withTable("uuid_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute("INSERT INTO " + table + " (id, u) VALUES"
                    + " (1, '00000000-0000-0000-0000-000000000000'),"
                    + " (2, '61f0c404-5cb3-11e7-907b-a6006ad3dba0'),"
                    + " (3, '123e4567-e89b-12d3-a456-426614174000')");

            List<Object[]> rows = decode(conn,
                    "SELECT id, u FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 rows from " + table);

            assertEquals(NIL, rows.get(0)[1], "nil UUID must decode to new UUID(0,0)");
            assertEquals(U1, rows.get(1)[1], "known UUID U1 must decode exactly (half-ordering)");
            assertEquals(U2, rows.get(2)[1], "known UUID U2 must decode exactly (half-ordering)");
        });
    }

    /**
     * ENCODE + MAPPED-READ: bulk-insert records with {@link UUID} fields (incl.
     * nil), read back via the block API and via {@code query(sql, UuidRow.class)}.
     */
    @Test
    void uuidEncodeAndMappedReadRoundTrip() {
        withTable("uuid_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<UuidRow> input = List.of(
                    new UuidRow(1, NIL),
                    new UuidRow(2, U1),
                    new UuidRow(3, U2));

            try (BulkInserter<UuidRow> inserter =
                    conn.createBulkInserter(table, UuidRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT id, u FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk-inserted rows");
            assertEquals(NIL, rows.get(0)[1], "nil UUID encode round-trip");
            assertEquals(U1, rows.get(1)[1], "U1 encode round-trip");
            assertEquals(U2, rows.get(2)[1], "U2 encode round-trip");

            // (b) mapped-read via query(sql, Class) — the path not covered elsewhere
            List<UuidRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, u FROM " + table + " ORDER BY id",
                    UuidRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped,
                    "Mapped-read UUID records must equal the inserted records exactly");
        });
    }
}
