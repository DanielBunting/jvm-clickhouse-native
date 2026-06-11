package io.github.danielbunting.clickhouse.test;

import io.github.danielbunting.clickhouse.ClickHouseConnection;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared base for cross-client round-trip integration tests, which validate
 * this library against the <em>official</em> ClickHouse Java client (the
 * {@code com.clickhouse:clickhouse-jdbc} driver, built on client-v2, speaking
 * HTTP on port {@value ClickHouseContainer#HTTP_PORT}) as the reference
 * implementation:
 * <ul>
 *   <li><b>Direction A</b>: the official client inserts (it serializes the
 *       values), the native client reads — exercises native DECODE against
 *       reference-produced data.</li>
 *   <li><b>Direction B</b>: the native {@code BulkInserter} inserts, the
 *       official client reads — exercises native ENCODE as consumed by the
 *       reference client.</li>
 * </ul>
 *
 * <p><b>Scope</b>: both clients talk to the same server, which normalizes
 * whatever it stores, so these tests validate <em>semantic compatibility</em>
 * (type mapping, precision, timezone handling, null handling) — not
 * byte-for-byte wire equality. The official Java client cannot speak the
 * native TCP protocol at all, so this is the strongest cross-validation
 * available on the JVM.
 *
 * <p>Tests assert <em>anchored expectations</em>: each test declares the
 * expected canonical Java values once and asserts both the native-read rows
 * and the (normalized) official-read rows against those same anchors, so a
 * failure pinpoints which client diverges and a shared bug cannot pass
 * silently.
 *
 * <p>Where one side cannot participate (no native encode, or the official
 * driver cannot read a type), tests fall back to a <em>neutral insert</em> —
 * raw {@code INSERT ... VALUES} literals through the native connection (the
 * server parses, neither client's binary encoder is involved) — followed by
 * {@link #assertBothClientsRead}, which still cross-validates both decoders
 * against the same anchors.
 *
 * <p>Server settings for the official client (e.g. experimental-type flags)
 * are passed per-connection via JDBC properties using the driver's
 * {@code clickhouse_setting_} prefix, which the driver forwards as HTTP query
 * parameters on every request. Fallback if ever needed: a
 * {@code SETTINGS x = 1} clause inline in the statement.
 *
 * <p><b>Deferred type families</b> (follow-ups, not yet cross-validated):
 * Interval (not a storable column type); AggregateFunction states (not
 * readable without {@code -Merge}).
 *
 * <p>Findings the suite has surfaced (server float text parsing, official
 * driver DST/Variant/JSON/Time/BFloat16 limitations, server VALUES NULL
 * coercion), the diagnostics that attributed them, and the per-family
 * coverage matrix are written up in {@code docs/cross-client-compatibility.md}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
public abstract class CrossClientRoundTripBase extends TypeRoundTripBase {

    /**
     * Returns the JDBC URL for the official ClickHouse driver, pointed at the
     * shared container's HTTP port.
     *
     * @return a {@code jdbc:clickhouse://host:port/default} URL
     */
    protected static String officialJdbcUrl() {
        return "jdbc:clickhouse://" + clickHouseHost() + ":" + clickHouseHttpPort() + "/default";
    }

    /**
     * Opens a connection through the official ClickHouse JDBC driver
     * (user {@code default}, empty password — the container's
     * {@code zz-open-default.xml} override opens the user to all networks,
     * which applies to HTTP as well).
     *
     * @return an open JDBC connection (caller closes)
     * @throws SQLException if the connection cannot be established
     */
    protected static Connection officialConnection() throws SQLException {
        return officialConnection(Map.of());
    }

    /**
     * Like {@link #officialConnection()} but with ClickHouse server settings
     * applied to every request on the connection, passed via the driver's
     * {@code clickhouse_setting_} property prefix (forwarded as HTTP query
     * parameters). Use for experimental-type flags such as
     * {@code allow_experimental_variant_type}.
     *
     * @param serverSettings server setting name → value (may be empty)
     * @return an open JDBC connection (caller closes)
     * @throws SQLException if the connection cannot be established
     */
    protected static Connection officialConnection(Map<String, String> serverSettings)
            throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "default");
        props.setProperty("password", "");
        for (Map.Entry<String, String> e : serverSettings.entrySet()) {
            props.setProperty("clickhouse_setting_" + e.getKey(), e.getValue());
        }
        return DriverManager.getConnection(officialJdbcUrl(), props);
    }

    /**
     * Direction A insert: the official client serializes the given values
     * (PreparedStatement batch over HTTP).
     *
     * @param table      target table name
     * @param columnsCsv comma-separated column list, e.g. {@code "id, v"}
     * @param rows       positional values, one {@code Object[]} per row
     */
    protected void officialInsert(String table, String columnsCsv, List<Object[]> rows) {
        officialInsert(table, columnsCsv, rows, Map.of());
    }

    /**
     * Like {@link #officialInsert(String, String, List)} but with ClickHouse
     * server settings applied to the official connection (see
     * {@link #officialConnection(Map)}).
     *
     * @param table          target table name
     * @param columnsCsv     comma-separated column list, e.g. {@code "id, v"}
     * @param rows           positional values, one {@code Object[]} per row
     * @param serverSettings server setting name → value (may be empty)
     */
    protected void officialInsert(String table, String columnsCsv, List<Object[]> rows,
            Map<String, String> serverSettings) {
        int columnCount = columnsCsv.split(",").length;
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table)
                .append(" (").append(columnsCsv).append(") VALUES (");
        for (int i = 0; i < columnCount; i++) {
            sql.append(i > 0 ? ", ?" : "?");
        }
        sql.append(')');
        try (Connection conn = officialConnection(serverSettings);
                PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (Object[] row : rows) {
                if (row.length != columnCount) {
                    throw new IllegalArgumentException(
                            "Row has " + row.length + " values but columns are: " + columnsCsv);
                }
                for (int i = 0; i < row.length; i++) {
                    ps.setObject(i + 1, row[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Official-client insert into " + table + " failed", e);
        }
    }

    /**
     * Direction B read: the official client deserializes; every cell is passed
     * through {@link #normalizeOfficial(Object)} so assertions can compare
     * against the same canonical values used for the native side.
     *
     * @param sql the SELECT statement
     * @return all rows as {@code Object[]} of normalized values
     */
    protected List<Object[]> officialSelect(String sql) {
        return officialSelect(sql, (Class<?>[]) null);
    }

    /**
     * Like {@link #officialSelect(String)} but reads each column through the
     * JDBC 4.2 typed accessor {@code getObject(i, type)}. Use this for
     * DateTime/DateTime64 columns ({@code OffsetDateTime.class}) where the
     * untyped {@code getObject()} is session-timezone ambiguous. A {@code null}
     * entry falls back to the untyped accessor for that column.
     *
     * @param sql         the SELECT statement
     * @param columnTypes per-column target types (may be {@code null} overall)
     * @return all rows as {@code Object[]} of normalized values
     */
    protected List<Object[]> officialSelect(String sql, Class<?>... columnTypes) {
        return officialSelect(Map.of(), sql, columnTypes);
    }

    /**
     * Like {@link #officialSelect(String, Class[])} but with ClickHouse server
     * settings applied to the official connection (see
     * {@link #officialConnection(Map)}). Settings come first so the overload
     * never collides with the varargs form.
     *
     * @param serverSettings server setting name → value (may be empty)
     * @param sql            the SELECT statement
     * @param columnTypes    per-column target types (may be {@code null})
     * @return all rows as {@code Object[]} of normalized values
     */
    protected List<Object[]> officialSelect(Map<String, String> serverSettings, String sql,
            Class<?>... columnTypes) {
        try (Connection conn = officialConnection(serverSettings);
                PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            int columnCount = rs.getMetaData().getColumnCount();
            List<Object[]> rows = new ArrayList<>();
            while (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int c = 0; c < columnCount; c++) {
                    Class<?> type = columnTypes != null && c < columnTypes.length
                            ? columnTypes[c] : null;
                    Object raw = type != null ? rs.getObject(c + 1, type) : rs.getObject(c + 1);
                    row[c] = normalizeOfficial(raw);
                }
                rows.add(row);
            }
            return rows;
        } catch (SQLException e) {
            throw new RuntimeException("Official-client select failed: " + sql, e);
        }
    }

    /**
     * Canonicalizes the carrier types the official JDBC driver returns into
     * the representations this library's block API uses, so both sides can be
     * asserted against the same anchored values. Kept total over the 0.8.x and
     * 0.9.x return-type variations so a driver bump does not break tests.
     *
     * @param v a raw cell value from the official driver (may be {@code null})
     * @return the canonical value
     */
    protected static Object normalizeOfficial(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof java.sql.Array sqlArray) {
            try {
                return normalizeOfficial(sqlArray.getArray());
            } catch (SQLException e) {
                throw new RuntimeException("Failed to read JDBC array", e);
            }
        }
        if (v instanceof Object[] array) {
            List<Object> out = new ArrayList<>(array.length);
            for (Object element : array) {
                out.add(normalizeOfficial(element));
            }
            return out;
        }
        if (v.getClass().isArray() && !(v instanceof byte[])) {
            int length = java.lang.reflect.Array.getLength(v);
            List<Object> out = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                out.add(normalizeOfficial(java.lang.reflect.Array.get(v, i)));
            }
            return out;
        }
        if (v instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(normalizeOfficial(element));
            }
            return out;
        }
        if (v instanceof Map<?, ?> map) {
            Map<Object, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(normalizeOfficial(e.getKey()), normalizeOfficial(e.getValue()));
            }
            return out;
        }
        if (v instanceof java.sql.Timestamp ts) {
            return ts.toInstant();
        }
        if (v instanceof OffsetDateTime odt) {
            return odt.toInstant();
        }
        if (v instanceof ZonedDateTime zdt) {
            return zdt.toInstant();
        }
        if (v instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        if (v instanceof java.sql.Time time) {
            return Duration.ofNanos(time.toLocalTime().toNanoOfDay());
        }
        if (v instanceof LocalTime lt) {
            return Duration.ofNanos(lt.toNanoOfDay());
        }
        if (v instanceof com.clickhouse.data.Tuple tuple) {
            return normalizeOfficial(tuple.getValues());
        }
        if (v instanceof com.clickhouse.client.api.data_formats.internal
                .BinaryStreamReader.ArrayValue arrayValue) {
            // The official reader's internal array carrier; surfaces raw inside
            // tuples, and its JDBC java.sql.Array conversion fails for nested
            // arrays — read such columns via getObject(i, Object.class).
            return normalizeOfficial(arrayValue.asList());
        }
        return v;
    }

    /**
     * Parses ClickHouse's {@code toString(Time/Time64)} rendering —
     * {@code [-]H…H:MM:SS[.fraction]} with an unbounded hour count (e.g.
     * {@code 999:59:59}) — into a {@link Duration}. Used for official-client
     * legs reading Time columns, which the official driver cannot decode
     * natively.
     *
     * @param s the rendered time string
     * @return the equivalent duration (negative if the string is negative)
     */
    protected static Duration parseClickHouseTime(String s) {
        Matcher m = Pattern.compile("(-?)(\\d+):(\\d{2}):(\\d{2})(?:\\.(\\d{1,9}))?").matcher(s);
        if (!m.matches()) {
            throw new IllegalArgumentException("Not a ClickHouse time string: " + s);
        }
        Duration d = Duration.ofHours(Long.parseLong(m.group(2)))
                .plusMinutes(Long.parseLong(m.group(3)))
                .plusSeconds(Long.parseLong(m.group(4)));
        if (m.group(5) != null) {
            String frac = m.group(5);
            long nanos = Long.parseLong(frac) * (long) Math.pow(10, 9 - frac.length());
            d = d.plusNanos(nanos);
        }
        return m.group(1).isEmpty() ? d : d.negated();
    }

    /**
     * Neutral-insert assertion leg: reads the already-inserted data back with
     * BOTH clients and asserts each against the same anchored {@code expected}
     * rows. Used where one direction's encoder cannot participate (no native
     * encode for the type, or the official driver cannot bind it) — it still
     * cross-validates the two decoders against each other.
     *
     * <p>Two SQL strings are accepted because the official leg sometimes needs
     * server-side casts the native leg must not use (e.g. {@code toString(t)}
     * for Time, {@code CAST(b AS Float32)} for BFloat16).
     *
     * @param label               assertion context
     * @param conn                an open native connection
     * @param nativeSql           SELECT for the native client
     * @param officialSql         SELECT for the official client
     * @param expected            anchored expected rows
     * @param officialSettings    server settings for the official connection
     * @param officialColumnTypes typed-accessor classes for the official read
     */
    protected void assertBothClientsRead(String label, ClickHouseConnection conn,
            String nativeSql, String officialSql, List<Object[]> expected,
            Map<String, String> officialSettings, Class<?>... officialColumnTypes) {
        assertRowsMatch(label + " native decode", expected, decode(conn, nativeSql));
        assertRowsMatch(label + " official read", expected,
                officialSelect(officialSettings, officialSql, officialColumnTypes));
    }

    /** Single-SQL convenience overload of {@link #assertBothClientsRead}. */
    protected void assertBothClientsRead(String label, ClickHouseConnection conn,
            String sql, List<Object[]> expected) {
        assertBothClientsRead(label, conn, sql, sql, expected, Map.of());
    }

    /** Builds one positional row for {@link #officialInsert}. */
    protected static Object[] row(Object... cells) {
        return cells;
    }

    /** Builds the row list for {@link #officialInsert} / assertions. */
    protected static List<Object[]> rowsOf(Object[]... rows) {
        return Arrays.asList(rows);
    }

    /**
     * Asserts that {@code actual} matches the anchored {@code expected} rows,
     * cell by cell, with representation-tolerant comparison (numeric width,
     * BigDecimal scale, IPv4-mapped IPv6, nested lists/maps).
     *
     * @param label    assertion context, e.g. {@code "Decimal(18,4) native decode"}
     * @param expected anchored expected rows
     * @param actual   rows read back (native {@code decode} or {@code officialSelect})
     */
    protected static void assertRowsMatch(String label, List<Object[]> expected, List<Object[]> actual) {
        assertEquals(expected.size(), actual.size(),
                label + ": expected " + expected.size() + " rows but got " + actual.size());
        for (int r = 0; r < expected.size(); r++) {
            Object[] expectedRow = expected.get(r);
            Object[] actualRow = actual.get(r);
            assertEquals(expectedRow.length, actualRow.length,
                    label + " row " + r + ": column count mismatch");
            for (int c = 0; c < expectedRow.length; c++) {
                Object e = expectedRow[c];
                Object a = actualRow[c];
                assertTrue(cellMatches(e, a),
                        label + " row " + r + " col " + c + ": expected "
                        + describe(e) + " but got " + describe(a));
            }
        }
    }

    /**
     * Representation-tolerant cell equality, coercing {@code actual} toward
     * the type of the anchored {@code expected} value.
     */
    private static boolean cellMatches(Object expected, Object actual) {
        if (expected == null || actual == null) {
            return expected == null && actual == null;
        }
        if (expected instanceof BigDecimal e) {
            BigDecimal a = actual instanceof BigDecimal bd
                    ? bd : new BigDecimal(String.valueOf(actual));
            return e.compareTo(a) == 0;
        }
        if (expected instanceof BigInteger e) {
            BigInteger a = actual instanceof BigInteger bi
                    ? bi : new BigInteger(String.valueOf(actual));
            return e.equals(a);
        }
        if (expected instanceof Float e && actual instanceof Number a) {
            return Float.compare(e, a.floatValue()) == 0;
        }
        if (expected instanceof Double e && actual instanceof Number a) {
            return Double.compare(e, a.doubleValue()) == 0;
        }
        if (expected instanceof Number e && actual instanceof Number a) {
            // Integral widths differ across clients (Byte vs Short vs Long ...).
            return e.longValue() == a.longValue();
        }
        if (expected instanceof Boolean e) {
            if (actual instanceof Boolean a) {
                return e.equals(a);
            }
            if (actual instanceof Number a) {
                return e == (a.longValue() != 0);
            }
            return false;
        }
        if (expected instanceof String e && actual instanceof String a) {
            // FixedString(N): the native codec strips trailing NULs, the
            // official driver returns the full padded N bytes — both carry the
            // same logical value.
            return e.equals(a)
                    || (a.startsWith(e) && a.substring(e.length()).chars().allMatch(c -> c == 0));
        }
        if (expected instanceof Duration e) {
            if (actual instanceof Duration a) {
                return e.equals(a);
            }
            if (actual instanceof String s) {
                // Official legs read Time columns as toString(t).
                return e.equals(parseClickHouseTime(s));
            }
            return false;
        }
        if (expected instanceof InetAddress e) {
            InetAddress a;
            if (actual instanceof InetAddress addr) {
                a = addr;
            } else if (actual instanceof String s) {
                try {
                    a = InetAddress.getByName(s);
                } catch (UnknownHostException ex) {
                    return false;
                }
            } else {
                return false;
            }
            // Compare via address bytes; Java already collapses IPv4-mapped
            // IPv6 (::ffff:a.b.c.d) to Inet4Address on construction, so both
            // sides land on the same canonical form.
            return Arrays.equals(e.getAddress(), a.getAddress());
        }
        if (expected instanceof List<?> e) {
            Object normalized = normalizeOfficial(actual);
            if (!(normalized instanceof List<?> a) || e.size() != a.size()) {
                return false;
            }
            for (int i = 0; i < e.size(); i++) {
                if (!cellMatches(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (expected instanceof Map<?, ?> e) {
            Object normalized = normalizeOfficial(actual);
            if (!(normalized instanceof Map<?, ?> a) || e.size() != a.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : e.entrySet()) {
                if (!a.containsKey(entry.getKey())
                        || !cellMatches(entry.getValue(), a.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(expected, actual);
    }

    private static String describe(Object v) {
        if (v == null) {
            return "null";
        }
        return v + " (" + v.getClass().getName() + ")";
    }
}
