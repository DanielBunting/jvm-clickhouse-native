package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Round-trips the ClickHouse {@code IPv4} and {@code IPv6} types through a real
 * server in both directions: DECODE (raw {@code INSERT ... VALUES} literals,
 * server encodes) and ENCODE (bulk insert of a mapped record carrying an
 * {@link InetAddress} field, client encodes). Assertions go through
 * {@link InetAddress#equals(Object)} so the address-family details and the byte
 * order of the wire encoding are exercised directly.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class IpTypesIT extends TypeRoundTripBase {

    /** Record whose {@link InetAddress} field drives the mapper OBJECT -> codec.set path. */
    record IpRow(int id, InetAddress addr) {}

    private static InetAddress addr(String s) {
        try {
            return InetAddress.getByName(s);
        } catch (UnknownHostException e) {
            throw new AssertionError("bad test literal: " + s, e);
        }
    }

    // ------------------------------------------------------------------ IPv4

    @Test
    void ipv4DecodeRoundTrip() {
        withTable("ipv4_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, addr IPv4) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, addr) VALUES"
                    + " (1, '0.0.0.0'),"
                    + " (2, '192.168.1.1'),"          // UInt32 = 3232235777 = 0xC0A80101
                    + " (3, '255.255.255.255')");

            List<Object[]> rows = decode(conn,
                    "SELECT id, addr FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            assertInstanceOf(InetAddress.class, rows.get(0)[1]);
            assertEquals(addr("0.0.0.0"), rows.get(0)[1], "IPv4 0.0.0.0");
            assertEquals(addr("192.168.1.1"), rows.get(1)[1], "IPv4 192.168.1.1 (byte order)");
            assertEquals(addr("255.255.255.255"), rows.get(2)[1], "IPv4 255.255.255.255");
        });
    }

    @Test
    void ipv4EncodeRoundTrip() {
        withTable("ipv4_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, addr IPv4) ENGINE = MergeTree() ORDER BY id");

            List<IpRow> input = List.of(
                    new IpRow(1, addr("0.0.0.0")),
                    new IpRow(2, addr("192.168.1.1")),
                    new IpRow(3, addr("255.255.255.255")));

            try (BulkInserter<IpRow> inserter =
                    conn.createBulkInserter(table, IpRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT id, addr FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());
            assertEquals(addr("0.0.0.0"), rows.get(0)[1]);
            assertEquals(addr("192.168.1.1"), rows.get(1)[1]);
            assertEquals(addr("255.255.255.255"), rows.get(2)[1]);

            // (b) mapped-read via query(sql, Class) -> InetAddress field
            List<IpRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, addr FROM " + table + " ORDER BY id", IpRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, "Mapped IPv4 records must equal inserted records");
        });
    }

    // ------------------------------------------------------------------ IPv6

    @Test
    void ipv6DecodeRoundTrip() {
        withTable("ipv6_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, addr IPv6) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, addr) VALUES"
                    + " (1, '::1'),"
                    + " (2, '2001:db8::1'),"
                    + " (3, '::ffff:192.168.0.1')");

            List<Object[]> rows = decode(conn,
                    "SELECT id, addr FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());

            assertInstanceOf(InetAddress.class, rows.get(0)[1]);
            assertEquals(addr("::1"), rows.get(0)[1], "IPv6 ::1");
            assertEquals(addr("2001:db8::1"), rows.get(1)[1], "IPv6 2001:db8::1");
            assertEquals(addr("::ffff:192.168.0.1"), rows.get(2)[1],
                    "IPv6 IPv4-mapped ::ffff:192.168.0.1");
        });
    }

    @Test
    void ipv6EncodeRoundTrip() {
        withTable("ipv6_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, addr IPv6) ENGINE = MergeTree() ORDER BY id");

            List<IpRow> input = List.of(
                    new IpRow(1, addr("::1")),
                    new IpRow(2, addr("2001:db8::1")),
                    new IpRow(3, addr("::ffff:192.168.0.1")));

            try (BulkInserter<IpRow> inserter =
                    conn.createBulkInserter(table, IpRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode
            List<Object[]> rows = decode(conn,
                    "SELECT id, addr FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size());
            assertEquals(addr("::1"), rows.get(0)[1]);
            assertEquals(addr("2001:db8::1"), rows.get(1)[1]);
            assertEquals(addr("::ffff:192.168.0.1"), rows.get(2)[1]);

            // (b) mapped-read via query(sql, Class) -> InetAddress field
            List<IpRow> mapped;
            try (var stream = conn.query(
                    "SELECT id, addr FROM " + table + " ORDER BY id", IpRow.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, "Mapped IPv6 records must equal inserted records");
        });
    }
}
