package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Edge-case round-trips for {@code String} and {@code FixedString(N)} that the
 * existing {@code StringLikeTypesIT} does not cover.
 *
 * <ul>
 *   <li><b>String</b> — empty, ascii, multi-byte unicode ({@code éàü}), emoji
 *       (surrogate pairs), an embedded NUL (encode-only, since a NUL in a SQL
 *       literal is awkward), and a &gt;64KB value to exercise the multi-byte
 *       VarInt length prefix.</li>
 *   <li><b>FixedString(N)</b> — {@code N=1}; a value shorter than {@code N}
 *       (server zero-pads, decoder returns trailing NULs trimmed here); a value
 *       exactly {@code N}; and a value longer than {@code N} via the encode path,
 *       which silently truncates to the first {@code N} bytes.</li>
 * </ul>
 *
 * <p>{@code FixedString} {@link #trimNuls} mirrors the helper in
 * {@code StringLikeTypesIT}: trailing NUL padding is stripped before comparing.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class StringExtraTypesIT extends TypeRoundTripBase {

    /** Row mirroring a single {@code String} column; field name matches. */
    record StrRow(int id, String s) {}

    /** Row mirroring a single {@code FixedString} column; field name matches. */
    record FixedRow(int id, String fs) {}

    private static final String UNICODE = "éàü";        // éàü
    private static final String EMOJI = "😀🎉";    // 😀🎉
    private static final String EMBEDDED_NUL = "a\u0000b";             // 3 chars incl. NUL
    private static final String LONG_STR = "x".repeat(70_000);         // >64KB VarInt prefix

    /**
     * Strips trailing NUL / space padding from a decoded {@code FixedString}
     * value so the logical content can be compared to the input.
     */
    private static String trimNuls(String s) {
        int end = s.length();
        while (end > 0) {
            char c = s.charAt(end - 1);
            if (c != '\u0000' && c != ' ') {
                break;
            }
            end--;
        }
        return s.substring(0, end);
    }

    /**
     * DECODE: server encodes the SQL-expressible String values (empty, ascii,
     * unicode, emoji, and a &gt;64KB value), client decodes them back exactly.
     */
    @Test
    void stringDecodeRoundTrip() {
        withTable("str_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, s String)"
                    + " ENGINE = MergeTree() ORDER BY id");

            // Insert via bulk for the long/unicode values is exercised separately;
            // here use literals for the SQL-expressible cases. The long string is
            // SQL-expressible too and exercises the multi-byte VarInt length on read.
            try (BulkInserter<StrRow> inserter =
                    conn.createBulkInserter(table, StrRow.class)) {
                inserter.init();
                inserter.addRange(List.of(
                        new StrRow(1, ""),
                        new StrRow(2, "ascii"),
                        new StrRow(3, UNICODE),
                        new StrRow(4, EMOJI),
                        new StrRow(5, LONG_STR)));
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT id, s FROM " + table + " ORDER BY id");
            assertEquals(5, rows.size(), "Expected 5 rows from " + table);

            assertEquals("", rows.get(0)[1], "empty String decode");
            assertEquals("ascii", rows.get(1)[1], "ascii String decode");
            assertEquals(UNICODE, rows.get(2)[1], "multi-byte unicode decode");
            assertEquals(EMOJI, rows.get(3)[1], "emoji (surrogate pair) decode");
            String back = (String) rows.get(4)[1];
            assertEquals(70_000, back.length(),
                    ">64KB String decode: length must survive multi-byte VarInt prefix");
            assertEquals(LONG_STR, back, ">64KB String decode: content must match");
        });
    }

    /**
     * ENCODE: bulk-insert all String edge cases including the embedded NUL and the
     * &gt;64KB value (the multi-byte VarInt length-prefix WRITE path), then read
     * back and assert exact equality.
     */
    @Test
    void stringEncodeRoundTrip() {
        withTable("str_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, s String)"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<StrRow> input = List.of(
                    new StrRow(1, ""),
                    new StrRow(2, "ascii"),
                    new StrRow(3, UNICODE),
                    new StrRow(4, EMOJI),
                    new StrRow(5, EMBEDDED_NUL),
                    new StrRow(6, LONG_STR));

            try (BulkInserter<StrRow> inserter =
                    conn.createBulkInserter(table, StrRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT id, s FROM " + table + " ORDER BY id");
            assertEquals(6, rows.size(), "Expected 6 rows from " + table);

            assertEquals("", rows.get(0)[1], "empty String encode");
            assertEquals("ascii", rows.get(1)[1], "ascii String encode");
            assertEquals(UNICODE, rows.get(2)[1], "multi-byte unicode encode");
            assertEquals(EMOJI, rows.get(3)[1], "emoji encode");

            String nul = (String) rows.get(4)[1];
            assertEquals(3, nul.length(), "embedded-NUL String must keep all 3 chars");
            assertEquals(EMBEDDED_NUL, nul, "embedded-NUL String must round-trip incl. the NUL");
            assertEquals('\u0000', nul.charAt(1), "the middle char must be the NUL");

            String back = (String) rows.get(5)[1];
            assertEquals(70_000, back.length(),
                    ">64KB String encode: multi-byte VarInt length prefix must be written");
            assertEquals(LONG_STR, back, ">64KB String encode: content must match");
        });
    }

    /**
     * DECODE: literal {@code FixedString(8)} values — exact length, shorter
     * (zero-padded), and {@code N=1} on a separate single-byte column — read back
     * and compared after trimming NUL padding.
     */
    @Test
    void fixedStringDecodeRoundTrip() {
        withTable("fixed_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  fs FixedString(8),"
                    + "  fs1 FixedString(1)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            conn.execute("INSERT INTO " + table + " (id, fs, fs1) VALUES"
                    + " (1, 'exactly8', 'A'),"   // fs exactly N=8, fs1 N=1
                    + " (2, 'abc', 'B')");        // fs shorter -> zero-padded to 8

            List<Object[]> rows = decode(conn,
                    "SELECT id, fs, fs1 FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "Expected 2 rows from " + table);

            assertEquals("exactly8", trimNuls((String) rows.get(0)[1]),
                    "FixedString(8) exact-length value");
            assertEquals("A", trimNuls((String) rows.get(0)[2]),
                    "FixedString(1) single-byte value");

            assertEquals("abc", trimNuls((String) rows.get(1)[1]),
                    "FixedString(8) shorter value, zero-padded then trimmed");
            assertEquals("B", trimNuls((String) rows.get(1)[2]),
                    "FixedString(1) single-byte value");
        });
    }

    /**
     * ENCODE: bulk-insert {@code FixedString(4)} values — shorter than N
     * (zero-padded), exactly N, and longer than N (the encode path silently
     * truncates to the first N bytes). Read back trims NUL padding.
     */
    @Test
    void fixedStringEncodeRoundTrip() {
        withTable("fixed_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, fs FixedString(4))"
                    + " ENGINE = MergeTree() ORDER BY id");

            List<FixedRow> input = List.of(
                    new FixedRow(1, "ab"),       // shorter than N=4 -> zero-padded
                    new FixedRow(2, "abcd"),     // exactly N=4
                    new FixedRow(3, "abcdef"));  // longer than N -> encode truncates to "abcd"

            try (BulkInserter<FixedRow> inserter =
                    conn.createBulkInserter(table, FixedRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT id, fs FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "Expected 3 rows from " + table);

            assertEquals("ab", trimNuls((String) rows.get(0)[1]),
                    "FixedString(4) shorter value zero-padded then trimmed");
            assertEquals("abcd", trimNuls((String) rows.get(1)[1]),
                    "FixedString(4) exact-length value");
            assertEquals("abcd", trimNuls((String) rows.get(2)[1]),
                    "FixedString(4) longer value: encode truncates to first N bytes");
        });
    }
}
