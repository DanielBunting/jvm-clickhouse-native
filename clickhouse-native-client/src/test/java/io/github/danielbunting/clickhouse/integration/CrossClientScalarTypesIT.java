package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

/**
 * Cross-client round-trips for scalar types: integers (including the unsigned
 * extremes and 128/256-bit wide ints), floats, Bool, String, UUID, IPv4/IPv6
 * and Enum8/16. Each test inserts via the official ClickHouse client and
 * decodes via the native client (Direction A), then truncates and inserts via
 * the native {@code BulkInserter} and reads back via the official client
 * (Direction B) — both directions asserted against the same anchored values.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientScalarTypesIT extends CrossClientRoundTripBase {

    /** Signed fixed-width integers at min/zero/max. */
    @Test
    void signedIntegers() {
        record Row(int id, byte i8, short i16, int i32, long i64) {}
        String columns = "id, i8, i16, i32, i64";
        List<Object[]> expected = rowsOf(
                row(1L, (byte) -128, (short) -32768, Integer.MIN_VALUE, Long.MIN_VALUE),
                row(2L, (byte) 0, (short) 0, 0, 0L),
                row(3L, (byte) 127, (short) 32767, Integer.MAX_VALUE, Long.MAX_VALUE));

        withTable("xc_sint", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  i8  Int8,"
                    + "  i16 Int16,"
                    + "  i32 Int32,"
                    + "  i64 Int64"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("signed ints native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, (byte) -128, (short) -32768, Integer.MIN_VALUE, Long.MIN_VALUE),
                    new Row(2, (byte) 0, (short) 0, 0, 0L),
                    new Row(3, (byte) 127, (short) 32767, Integer.MAX_VALUE, Long.MAX_VALUE));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("signed ints official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Unsigned UInt8/16/32 at zero/mid/max (UInt64 has its own test). */
    @Test
    void unsignedIntegers() {
        record Row(int id, int u8, int u16, long u32) {}
        String columns = "id, u8, u16, u32";
        List<Object[]> expected = rowsOf(
                row(1L, 0, 0, 0L),
                row(2L, 200, 40000, 3_000_000_000L),
                row(3L, 255, 65535, 4_294_967_295L));

        withTable("xc_uint", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  u8  UInt8,"
                    + "  u16 UInt16,"
                    + "  u32 UInt32"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("unsigned ints native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 0, 0, 0L),
                    new Row(2, 200, 40000, 3_000_000_000L),
                    new Row(3, 255, 65535, 4_294_967_295L));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("unsigned ints official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * UInt64 across the full unsigned range. The native client materializes
     * UInt64 as a raw-bits {@code long} (2^64-1 reads as {@code -1L}), while
     * the official client returns the logical unsigned value as
     * {@link BigInteger} — so this test anchors each direction separately.
     */
    @Test
    void uint64FullRange() {
        record Row(int id, long u64) {}
        BigInteger maxU64 = BigInteger.TWO.pow(64).subtract(BigInteger.ONE);
        BigInteger highBit = BigInteger.TWO.pow(63);
        List<Object[]> logical = rowsOf(
                row(1L, BigInteger.ZERO),
                row(2L, BigInteger.ONE),
                row(3L, highBit),
                row(4L, maxU64));
        // Same values as the native client's raw-bits longs.
        List<Object[]> rawBits = rowsOf(
                row(1L, 0L),
                row(2L, 1L),
                row(3L, Long.MIN_VALUE),
                row(4L, -1L));

        withTable("xc_u64", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  u64 UInt64"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, u64", logical);
            assertRowsMatch("UInt64 native decode (raw bits)", rawBits,
                    decode(conn, "SELECT id, u64 FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 0L),
                    new Row(2, 1L),
                    new Row(3, Long.MIN_VALUE),
                    new Row(4, -1L));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("UInt64 official read (logical)", logical,
                    officialSelect("SELECT id, u64 FROM " + table + " ORDER BY id"));
        });
    }

    /** Int128/Int256/UInt128/UInt256 extremes as BigInteger on both sides. */
    @Test
    void wideIntegers() {
        record Row(int id, BigInteger i128, BigInteger i256, BigInteger u128, BigInteger u256) {}
        BigInteger i128Min = BigInteger.TWO.pow(127).negate();
        BigInteger i128Max = BigInteger.TWO.pow(127).subtract(BigInteger.ONE);
        BigInteger i256Min = BigInteger.TWO.pow(255).negate();
        BigInteger i256Max = BigInteger.TWO.pow(255).subtract(BigInteger.ONE);
        BigInteger u128Max = BigInteger.TWO.pow(128).subtract(BigInteger.ONE);
        BigInteger u256Max = BigInteger.TWO.pow(256).subtract(BigInteger.ONE);
        String columns = "id, i128, i256, u128, u256";
        List<Object[]> expected = rowsOf(
                row(1L, i128Min, i256Min, BigInteger.ZERO, BigInteger.ZERO),
                row(2L, BigInteger.valueOf(-1), BigInteger.ONE, BigInteger.ONE, BigInteger.ONE),
                row(3L, i128Max, i256Max, u128Max, u256Max));

        withTable("xc_wide", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id   UInt32,"
                    + "  i128 Int128,"
                    + "  i256 Int256,"
                    + "  u128 UInt128,"
                    + "  u256 UInt256"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("wide ints native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, i128Min, i256Min, BigInteger.ZERO, BigInteger.ZERO),
                    new Row(2, BigInteger.valueOf(-1), BigInteger.ONE, BigInteger.ONE, BigInteger.ONE),
                    new Row(3, i128Max, i256Max, u128Max, u256Max));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("wide ints official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * Float32/Float64 with ordinary values and signed zero — values that
     * survive the official client's textual insert path (see
     * {@link #floatExtremesViaBinaryEncode()} for why the IEEE extremes are
     * excluded from Direction A).
     */
    @Test
    void floats() {
        record Row(int id, float f32, double f64) {}
        String columns = "id, f32, f64";
        List<Object[]> expected = rowsOf(
                row(1L, 0.0f, 0.0d),
                row(2L, -0.0f, -0.0d),
                row(3L, 1.5f, 2.718281828459045d),
                row(4L, 1.4E-45f, -1.7976931348623157E308d));

        withTable("xc_float", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  f32 Float32,"
                    + "  f64 Float64"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("floats native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, 0.0f, 0.0d),
                    new Row(2, -0.0f, -0.0d),
                    new Row(3, 1.5f, 2.718281828459045d),
                    new Row(4, 1.4E-45f, -1.7976931348623157E308d));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("floats official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * IEEE boundary floats round-trip exactly through the native binary encode
     * (Direction B only). Direction A is deliberately skipped for these values:
     * the official JDBC driver inserts float parameters as <em>text</em>, and
     * the ClickHouse server's float text parser is not correctly rounded —
     * empirically (image 25.8): {@code -3.4028235E38} (−Float.MAX_VALUE) and
     * {@code 1.17549435E-38} (smallest normal Float32) store 1 ULP low, and
     * the denormal Double {@code 4.9E-324} flushes to {@code 0.0}. The same
     * loss reproduces with a raw SQL literal through any client, so it is a
     * server text-parse behavior, not an official-client (or native) bug; the
     * native binary path is bit-exact, which is what this test pins.
     */
    @Test
    void floatExtremesViaBinaryEncode() {
        record Row(int id, float f32, double f64) {}
        String columns = "id, f32, f64";
        List<Object[]> expected = rowsOf(
                row(1L, -Float.MAX_VALUE, -Double.MAX_VALUE),
                row(2L, Float.MAX_VALUE, Double.MAX_VALUE),
                row(3L, Float.MIN_VALUE, Double.MIN_VALUE),
                row(4L, 1.17549435E-38f, 2.2250738585072014E-308d));

        withTable("xc_fext", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  f32 Float32,"
                    + "  f64 Float64"
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<Row> records = List.of(
                    new Row(1, -Float.MAX_VALUE, -Double.MAX_VALUE),
                    new Row(2, Float.MAX_VALUE, Double.MAX_VALUE),
                    new Row(3, Float.MIN_VALUE, Double.MIN_VALUE),
                    new Row(4, 1.17549435E-38f, 2.2250738585072014E-308d));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("float extremes native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));
            assertRowsMatch("float extremes official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /**
     * Direction A for the IEEE boundary floats — kept disabled to document a
     * ClickHouse <em>server</em> limitation: by default, text-to-float parsing
     * is fast but not correctly rounded (it does not always pick the nearest
     * representable value). Empirically on image 25.8:
     * <ul>
     *   <li>{@code -3.4028235E38} (−Float.MAX_VALUE) stores as bit pattern
     *       {@code 0xFF7FFFFE} — 1 ULP below the correct {@code 0xFF7FFFFF};</li>
     *   <li>{@code 1.17549435E-38} (smallest normal Float32) also stores
     *       1 ULP low;</li>
     *   <li>the denormal Double {@code 4.9E-324} flushes to {@code 0.0}.</li>
     * </ul>
     *
     * <p>This was isolated with a three-path diagnostic against the same
     * table: a raw SQL literal ({@code INSERT ... VALUES (-3.4028235E38)})
     * reproduces the identical wrong bit pattern with no Java client involved
     * in the parse, while the native {@code BulkInserter} (binary wire format)
     * stores the exact bits. Both clients read back whatever was stored
     * identically. So the loss is in ClickHouse's text-to-float parsing — not
     * in this library and not in the official client's own logic (it merely
     * inherits the behavior by choosing the text path).
     *
     * <p>This is known, intentional upstream behavior, not an open bug:
     * <ul>
     *   <li>ClickHouse/ClickHouse#1665 — imprecise float parsing on text
     *       import (2017, closed as a feature request);</li>
     *   <li>ClickHouse/ClickHouse#4819 — {@code toFloat64('4.9e-324')}
     *       returns 0 (closed as documented behavior);</li>
     *   <li>ClickHouse/ClickHouse#17933 — added the opt-in
     *       {@code precise_float_parsing} setting.</li>
     * </ul>
     * Verified on image 25.8: with {@code SETTINGS precise_float_parsing = 1}
     * the same literals parse bit-exact ({@code 0xFF7FFFFF}) and the denormal
     * survives ({@code 5e-324}).
     *
     * <p>Re-enable if the server's default ever becomes correctly rounded, or
     * if the official client starts sending floats in a binary format (or
     * applies {@code precise_float_parsing} itself); the binary-path coverage
     * for the same values lives in {@link #floatExtremesViaBinaryEncode()}.
     */
    @Disabled("KNOWN LIMITATION of ClickHouse server defaults: text float parsing is fast but "
            + "not correctly rounded (Float32 extremes store 1 ULP low, denormal Float64 "
            + "flushes to 0.0), so any text-path insert (official JDBC, raw SQL literals) "
            + "loses the last bit. Tracked upstream as ClickHouse#1665/#4819/#17933; opt-in "
            + "fix exists (SETTINGS precise_float_parsing = 1, verified on 25.8). Native "
            + "binary inserts are unaffected; see floatExtremesViaBinaryEncode().")
    @Test
    void floatExtremesViaTextInsertKnownServerBug() {
        String columns = "id, f32, f64";
        List<Object[]> expected = rowsOf(
                row(1L, -Float.MAX_VALUE, -Double.MAX_VALUE),
                row(2L, Float.MAX_VALUE, Double.MAX_VALUE),
                row(3L, Float.MIN_VALUE, Double.MIN_VALUE),
                row(4L, 1.17549435E-38f, 2.2250738585072014E-308d));

        withTable("xc_fext_txt", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  f32 Float32,"
                    + "  f64 Float64"
                    + ") ENGINE = MergeTree() ORDER BY id");

            // Official client serializes these as text; the server's parser
            // currently mangles the Float32 extremes and the denormal double.
            officialInsert(table, columns, expected);
            assertRowsMatch("float extremes (text insert) native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));
            assertRowsMatch("float extremes (text insert) official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Non-finite floats (NaN, ±Inf) — kept separate from the ordinary cases. */
    @Test
    void nonFiniteFloats() {
        record Row(int id, float f32, double f64) {}
        String columns = "id, f32, f64";
        List<Object[]> expected = rowsOf(
                row(1L, Float.NaN, Double.NaN),
                row(2L, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
                row(3L, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));

        withTable("xc_nonfin", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  f32 Float32,"
                    + "  f64 Float64"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("non-finite floats native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, Float.NaN, Double.NaN),
                    new Row(2, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
                    new Row(3, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("non-finite floats official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Bool round-trips as true/false on both sides. */
    @Test
    void bools() {
        record Row(int id, boolean b) {}
        List<Object[]> expected = rowsOf(
                row(1L, true),
                row(2L, false));

        withTable("xc_bool", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  b  Bool"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, b", expected);
            assertRowsMatch("bool native decode", expected,
                    decode(conn, "SELECT id, b FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(new Row(1, true), new Row(2, false));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("bool official read", expected,
                    officialSelect("SELECT id, b FROM " + table + " ORDER BY id"));
        });
    }

    /** String: empty, ASCII, multibyte UTF-8 (CJK + emoji), embedded NUL. */
    @Test
    void strings() {
        record Row(int id, String s) {}
        List<Object[]> expected = rowsOf(
                row(1L, ""),
                row(2L, "hello"),
                row(3L, "雪が降る ❄️🌨️"),
                row(4L, "nul\0inside"));

        withTable("xc_str", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  s  String"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, s", expected);
            assertRowsMatch("string native decode", expected,
                    decode(conn, "SELECT id, s FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, ""),
                    new Row(2, "hello"),
                    new Row(3, "雪が降る ❄️🌨️"),
                    new Row(4, "nul\0inside"));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("string official read", expected,
                    officialSelect("SELECT id, s FROM " + table + " ORDER BY id"));
        });
    }

    /** UUID: nil plus known values — catches half-swap/byte-order bugs. */
    @Test
    void uuids() {
        record Row(int id, UUID u) {}
        UUID nil = new UUID(0L, 0L);
        UUID u1 = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        UUID u2 = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        List<Object[]> expected = rowsOf(
                row(1L, nil),
                row(2L, u1),
                row(3L, u2));

        withTable("xc_uuid", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  u  UUID"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, u", expected);
            assertRowsMatch("uuid native decode", expected,
                    decode(conn, "SELECT id, u FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(new Row(1, nil), new Row(2, u1), new Row(3, u2));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("uuid official read", expected,
                    officialSelect("SELECT id, u FROM " + table + " ORDER BY id"));
        });
    }

    /** IPv4 and IPv6 boundary addresses (incl. an IPv4-mapped IPv6 value). */
    @Test
    void ipAddresses() throws UnknownHostException {
        record Row(int id, InetAddress v4, InetAddress v6) {}
        InetAddress v4Zero = InetAddress.getByName("0.0.0.0");
        InetAddress v4Mid = InetAddress.getByName("1.2.3.4");
        InetAddress v4Max = InetAddress.getByName("255.255.255.255");
        InetAddress v6Zero = InetAddress.getByName("::");
        InetAddress v6Loop = InetAddress.getByName("::1");
        InetAddress v6Full = InetAddress.getByName("2001:db8:85a3:8d3:1319:8a2e:370:7348");
        String columns = "id, v4, v6";
        List<Object[]> expected = rowsOf(
                row(1L, v4Zero, v6Zero),
                row(2L, v4Mid, v6Loop),
                row(3L, v4Max, v6Full));

        withTable("xc_ip", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  v4 IPv4,"
                    + "  v6 IPv6"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("ip native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, v4Zero, v6Zero),
                    new Row(2, v4Mid, v6Loop),
                    new Row(3, v4Max, v6Full));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("ip official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }

    /** Enum8/Enum16 at boundary ordinals, asserted by name on both sides. */
    @Test
    void enums() {
        record Row(int id, String e8, String e16) {}
        String columns = "id, e8, e16";
        List<Object[]> expected = rowsOf(
                row(1L, "lo8", "lo16"),
                row(2L, "mid", "mid"),
                row(3L, "hi8", "hi16"));

        withTable("xc_enum", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id  UInt32,"
                    + "  e8  Enum8('lo8' = -128, 'mid' = 0, 'hi8' = 127),"
                    + "  e16 Enum16('lo16' = -32768, 'mid' = 0, 'hi16' = 32767)"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, columns, expected);
            assertRowsMatch("enum native decode", expected,
                    decode(conn, "SELECT " + columns + " FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, "lo8", "lo16"),
                    new Row(2, "mid", "mid"),
                    new Row(3, "hi8", "hi16"));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("enum official read", expected,
                    officialSelect("SELECT " + columns + " FROM " + table + " ORDER BY id"));
        });
    }
}
