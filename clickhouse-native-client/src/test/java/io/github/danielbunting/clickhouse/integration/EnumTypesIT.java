package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Round-trips {@code Enum8} and {@code Enum16} through a real server in both
 * directions, focusing on boundary ordinals and the name/number duality of the
 * insert path.
 *
 * <ul>
 *   <li><b>DECODE</b> — literal {@code INSERT}s using both the enum NAME and the
 *       numeric ordinal (ClickHouse maps an integer literal back to its name);
 *       boundary ordinals {@code -128 / 127} (Enum8) and {@code -32768 / 32767}
 *       (Enum16) are exercised. The decoded value is always the String name.</li>
 *   <li><b>ENCODE</b> — a bulk record whose enum fields are {@code String} names;
 *       {@code Enum8Codec.set}/{@code Enum16Codec.set} perform name-&gt;ordinal
 *       reverse lookup. Read back asserts the names.</li>
 *   <li><b>UNKNOWN-NAME ENCODE</b> — binding a {@code String} that is not a
 *       declared enum name throws {@link IllegalArgumentException} (a client-side
 *       codec rejection), NOT a server {@code ClickHouseException}.</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class EnumTypesIT extends TypeRoundTripBase {

    /**
     * Row mirroring the enum table. Enum columns map to {@code String} fields so
     * the bulk binder drives the codec's name-&gt;ordinal reverse lookup (and the
     * mapped-read returns the name back).
     */
    record EnumRow(int id, String e8, String e16) {}

    private static final String COLUMNS =
            "  id  UInt32,"
            + "  e8  Enum8('a' = -128, 'b' = 0, 'c' = 127),"
            + "  e16 Enum16('x' = -32768, 'y' = 0, 'z' = 32767)";

    /**
     * DECODE: server encodes rows inserted by NAME and by numeric ordinal; the
     * client decodes them back to the String name. Boundary ordinals are covered.
     */
    @Test
    void enumDecodeRoundTrip() {
        withTable("enum_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            // row 1: insert by NAME; row 2: insert by numeric ordinal literal
            // (ClickHouse maps the integer back to the declared name).
            conn.execute("INSERT INTO " + table + " (id, e8, e16) VALUES"
                    + " (1, 'a', 'x'),"          // boundary names: -128 / -32768
                    + " (2, 127, 32767),"        // boundary ordinals -> 'c' / 'z'
                    + " (3, 'b', 'y')");          // zero ordinals

            List<Object[]> rows = decode(conn,
                    "SELECT id, e8, e16 FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 rows from " + table);

            assertEquals("a", rows.get(0)[1], "Enum8 row 1: name insert, ordinal -128");
            assertEquals("x", rows.get(0)[2], "Enum16 row 1: name insert, ordinal -32768");

            assertEquals("c", rows.get(1)[1], "Enum8 row 2: numeric insert 127 -> 'c'");
            assertEquals("z", rows.get(1)[2], "Enum16 row 2: numeric insert 32767 -> 'z'");

            assertEquals("b", rows.get(2)[1], "Enum8 row 3: zero ordinal -> 'b'");
            assertEquals("y", rows.get(2)[2], "Enum16 row 3: zero ordinal -> 'y'");
        });
    }

    /**
     * ENCODE + MAPPED-READ: bulk-insert records carrying enum NAMES, then read
     * back via the block API and via {@code query(sql, EnumRow.class)}; both
     * surface the name String. Boundary ordinals included.
     */
    @Test
    void enumEncodeAndMappedReadRoundTrip() {
        withTable("enum_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<EnumRow> input = List.of(
                    new EnumRow(1, "a", "x"),   // boundary minimums
                    new EnumRow(2, "c", "z"),   // boundary maximums
                    new EnumRow(3, "b", "y"));  // zero ordinals

            try (BulkInserter<EnumRow> inserter =
                    conn.createBulkInserter(table, EnumRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT id, e8, e16 FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 bulk-inserted rows");
            assertEquals("a", rows.get(0)[1], "Enum8 encode boundary min name");
            assertEquals("x", rows.get(0)[2], "Enum16 encode boundary min name");
            assertEquals("c", rows.get(1)[1], "Enum8 encode boundary max name");
            assertEquals("z", rows.get(1)[2], "Enum16 encode boundary max name");
            assertEquals("b", rows.get(2)[1], "Enum8 encode zero name");
            assertEquals("y", rows.get(2)[2], "Enum16 encode zero name");

            // (b) mapped-read via query(sql, Class) — enum get() returns the name
            List<EnumRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, e8, e16 FROM " + table + " ORDER BY id",
                    EnumRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped,
                    "Mapped-read enum records must equal the inserted name records");
        });
    }

    /**
     * UNKNOWN-NAME ENCODE: binding a {@code String} that is not a declared enum
     * name must throw {@link IllegalArgumentException} from the codec's reverse
     * lookup (client-side), not a server {@code ClickHouseException}.
     */
    @Test
    void enumEncodeUnknownNameThrows() {
        withTable("enum_bad", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (" + COLUMNS
                    + ") ENGINE = MergeTree() ORDER BY id");

            // Use a DEDICATED connection for the deliberately-failing bulk insert:
            // the codec rejection leaves that connection mid-INSERT (dirty), and we
            // do not want the shared withTable connection's DROP to then fail with a
            // server-side "Unexpected packet" exception that would mask the result.
            assertThrows(IllegalArgumentException.class, () -> {
                try (ClickHouseConnection bad = ClickHouseConnection.open(config());
                        BulkInserter<EnumRow> inserter =
                                bad.createBulkInserter(table, EnumRow.class)) {
                    inserter.init();
                    // 'q' is not a declared Enum8 name -> reverse lookup fails.
                    inserter.add(new EnumRow(1, "q", "x"));
                    inserter.complete();
                }
            }, "Binding an undeclared Enum name must raise IllegalArgumentException "
                    + "from the codec, not a server-side ClickHouseException");
        });
    }
}
