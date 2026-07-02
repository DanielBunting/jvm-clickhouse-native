package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit-level setter-matrix coverage for {@link ChPreparedStatement}, ported from the
 * official clickhouse-java suites (v1 {@code ClickHousePreparedStatementTest},
 * jdbc-v2 {@code PreparedStatementTest} and {@code DateTimeComparisonTest}): primitive
 * setter rendering, {@code setNull} variants, Calendar-overload semantics,
 * {@code BigDecimal} scale, {@code setObject} type-hint handling, unsupported rich
 * setters, temporal renderings, array literals with NULL elements, and binding reuse
 * across executions. No server; a recording fake captures the interpolated SQL.
 *
 * <p>Kept separate from {@code ChPreparedStatementTest} (owned elsewhere).
 */
class ChPreparedStatementBindingTest {

    /** A core {@link ClickHouseConnection} that records executed SQL and does nothing else. */
    private static final class RecordingCore implements ClickHouseConnection {
        final List<String> executed = new ArrayList<>();

        @Override
        public long executeScalar(String sql) {
            return 0;
        }

        @Override
        public void execute(String sql) {
            executed.add(sql);
        }

        @Override
        public QueryResult query(String sql) {
            executed.add(sql);
            return null;
        }

        @Override
        public <T> Stream<T> query(String sql, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static ChConnection conn(RecordingCore core) {
        return new ChConnection(core, "jdbc:chnative://localhost:9000/default", new Properties());
    }

    /** Binds one value through {@code binder} and returns the rendered literal for it. */
    private static String renderedLiteral(Binder binder) throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(conn(core), "INSERT INTO t (x) VALUES (?)");
        binder.bind(ps);
        ps.executeUpdate();
        String sql = core.executed.get(0);
        String prefix = "INSERT INTO t (x) VALUES (";
        assertTrue(sql.startsWith(prefix) && sql.endsWith(")"), "unexpected SQL: " + sql);
        return sql.substring(prefix.length(), sql.length() - 1);
    }

    @FunctionalInterface
    private interface Binder {
        void bind(ChPreparedStatement ps) throws SQLException;
    }

    // ---- primitive numeric setters (jdbc-v2 testSetByte/testSetShort/testSetFloat) ----

    /** setByte/setShort/setLong/setFloat/setDouble render bare numerals. */
    @Test
    void primitiveNumericSettersRenderBareNumerals() throws SQLException {
        assertEquals("1", renderedLiteral(ps -> ps.setByte(1, (byte) 1)));
        assertEquals("-2", renderedLiteral(ps -> ps.setShort(1, (short) -2)));
        assertEquals("3000000000", renderedLiteral(ps -> ps.setLong(1, 3_000_000_000L)));
        assertEquals("1.5", renderedLiteral(ps -> ps.setFloat(1, 1.5f)));
        assertEquals("2.25", renderedLiteral(ps -> ps.setDouble(1, 2.25d)));
    }

    // ---- setNull variants and null-valued typed setters (v1 testInsertWithNullDateTime
    // setter dimension; the DEFAULT-fill config of clickhouse-java has no counterpart) --

    /** Every null-binding route renders SQL NULL: setNull (both overloads) and null args. */
    @Test
    void allNullBindingRoutesRenderSqlNull() throws SQLException {
        assertEquals("NULL", renderedLiteral(ps -> ps.setNull(1, Types.TIMESTAMP)));
        assertEquals("NULL", renderedLiteral(ps -> ps.setNull(1, Types.TIMESTAMP, "DateTime")));
        assertEquals("NULL", renderedLiteral(ps -> ps.setTimestamp(1, null)));
        assertEquals("NULL", renderedLiteral(
                ps -> ps.setTimestamp(1, null, new GregorianCalendar())));
        assertEquals("NULL", renderedLiteral(ps -> ps.setDate(1, null)));
        assertEquals("NULL", renderedLiteral(ps -> ps.setString(1, null)));
        assertEquals("NULL", renderedLiteral(ps -> ps.setBytes(1, null)));
        assertEquals("NULL", renderedLiteral(ps -> ps.setObject(1, null)));
        assertEquals("NULL", renderedLiteral(ps -> ps.setObject(1, null, Types.INTEGER)));
        assertEquals("NULL", renderedLiteral(ps -> ps.setURL(1, null)));
    }

    // ---- Calendar overloads (v1 DateTimeComparisonTest setDate/setTime/setTimestamp) --

    /**
     * The Calendar argument of setTimestamp/setDate/setTime is ignored by design: the
     * value's local wall clock is bound, so an explicit UTC or offset Calendar renders
     * identically to the no-Calendar overload (clickhouse-java v2 behaves the same for
     * dates; v1 shifted by the Calendar zone).
     */
    @Test
    void calendarOverloadsRenderIdenticallyToPlainSetters() throws SQLException {
        Timestamp ts = Timestamp.valueOf("2021-01-01 01:23:45");
        Date d = Date.valueOf("2021-01-01");
        Time t = Time.valueOf("12:34:56");
        GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        GregorianCalendar la = new GregorianCalendar(TimeZone.getTimeZone("America/Los_Angeles"));

        String plainTs = renderedLiteral(ps -> ps.setTimestamp(1, ts));
        assertEquals("'2021-01-01 01:23:45'", plainTs);
        assertEquals(plainTs, renderedLiteral(ps -> ps.setTimestamp(1, ts, utc)));
        assertEquals(plainTs, renderedLiteral(ps -> ps.setTimestamp(1, ts, la)));

        String plainDate = renderedLiteral(ps -> ps.setDate(1, d));
        assertEquals("'2021-01-01'", plainDate);
        assertEquals(plainDate, renderedLiteral(ps -> ps.setDate(1, d, utc)));
        assertEquals(plainDate, renderedLiteral(ps -> ps.setDate(1, d, la)));

        String plainTime = renderedLiteral(ps -> ps.setTime(1, t));
        assertEquals("'12:34:56'", plainTime);
        assertEquals(plainTime, renderedLiteral(ps -> ps.setTime(1, t, utc)));
        assertEquals(plainTime, renderedLiteral(ps -> ps.setTime(1, t, la)));
    }

    // ---- BigDecimal scale (jdbc-v2 testBigDecimal / testTypeCastWithScaleOrLength) ----

    /** setBigDecimal preserves the value's scale exactly (no float round-trip). */
    @Test
    void bigDecimalPreservesScale() throws SQLException {
        assertEquals("10.0000", renderedLiteral(ps -> ps.setBigDecimal(1, new BigDecimal("10.0000"))));
        assertEquals("0.10", renderedLiteral(ps -> ps.setBigDecimal(1, new BigDecimal("0.10"))));
        assertEquals("-12345678901234567890.123456789",
                renderedLiteral(ps -> ps.setBigDecimal(1,
                        new BigDecimal("-12345678901234567890.123456789"))));
    }

    // ---- setObject type hints (jdbc-v2 testJDBCTypeCast/testTypeCastWithScaleOrLength) --

    /**
     * The targetSqlType / scaleOrLength arguments of setObject are accepted and ignored:
     * the value binds by its runtime type only. jdbc-v2 casts to the requested type
     * (e.g. "5" with Types.INTEGER renders 5, BigDecimal with scaleOrLength is
     * rescaled); this driver pins the ignore-hints contract instead.
     */
    @Test
    void setObjectTypeHintsAreIgnored() throws SQLException {
        assertEquals("'5'", renderedLiteral(ps -> ps.setObject(1, "5", Types.INTEGER)));
        assertEquals("'5'", renderedLiteral(ps -> ps.setObject(1, "5", JDBCType.INTEGER)));
        assertEquals("5", renderedLiteral(ps -> ps.setObject(1, 5, Types.VARCHAR)));
        // scaleOrLength does not rescale the value.
        assertEquals("1.23456", renderedLiteral(
                ps -> ps.setObject(1, new BigDecimal("1.23456"), Types.DECIMAL, 2)));
        assertEquals("1.23456", renderedLiteral(
                ps -> ps.setObject(1, new BigDecimal("1.23456"), JDBCType.DECIMAL, 2)));
    }

    // ---- unsupported rich setters (jdbc-v2 setAsciiStream/setBinaryStream/... and
    // setArray, which jdbc-v2 supports but this driver does not) -----------------------

    /** The stream/LOB/Array/Ref/RowId/SQLXML setters are documented unsupported. */
    @Test
    void unsupportedRichSettersThrow() {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(conn(core), "SELECT ?");
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setArray(1, null));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setRef(1, null));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setRowId(1, null));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setSQLXML(1, null));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setBlob(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setClob(1, new StringReader("")));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setNClob(1, new StringReader("")));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setAsciiStream(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setBinaryStream(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setCharacterStream(1, new StringReader("")));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setNCharacterStream(1, new StringReader("")));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> ps.setUnicodeStream(1, new ByteArrayInputStream(new byte[0]), 0));
    }

    /** setURL binds the URL's text; setNString binds exactly like setString. */
    @Test
    void urlAndNStringBindAsText() throws Exception {
        assertEquals("'https://example.com/x'",
                renderedLiteral(ps -> {
                    try {
                        ps.setURL(1, new URL("https://example.com/x"));
                    } catch (java.net.MalformedURLException e) {
                        throw new SQLException(e);
                    }
                }));
        assertEquals("'o\\'k'", renderedLiteral(ps -> ps.setNString(1, "o'k")));
    }

    // ---- array literals with NULL elements (jdbc-v2 testEncodingArray) ---------------

    /**
     * Arrays with interior NULL elements render NULL per element, recursively. Note a
     * divergence from jdbc-v2's encoder: there a null <em>sub-array</em> encodes as
     * {@code []}, here it renders as {@code NULL} (ClickHouse accepts NULL only for
     * Nullable element types, and arrays themselves cannot be Nullable — so a null
     * sub-array is rejected by the server where jdbc-v2 would insert an empty one).
     * Tuple-typed elements have no binding counterpart in this driver (no Tuple class).
     */
    @Test
    void arrayLiteralRendersInteriorNulls() {
        assertEquals("[2,NULL]", ChPreparedStatement.toLiteral(new Long[] {2L, null}));
        assertEquals("['a',NULL]", ChPreparedStatement.toLiteral(Arrays.asList("a", null)));
        assertEquals("[[1,2,3],[4,NULL,6]]", ChPreparedStatement.toLiteral(
                new Object[] {new Object[] {1, 2, 3}, new Object[] {4, null, 6}}));
        // Divergence pin: null sub-array renders NULL (jdbc-v2 encodes []).
        assertEquals("[NULL,[1,2,3]]", ChPreparedStatement.toLiteral(
                new Object[] {null, new Object[] {1, 2, 3}}));
    }

    // ---- quoted identifiers in substitution (jdbc-v2 testReplaceQuestionMark) --------

    /**
     * KNOWN BUG — this test asserts the CORRECT behavior and fails until fixed.
     *
     * <p>Expected (jdbc-v2 {@code PreparedStatementImpl.replaceQuestionMarks}): a
     * {@code ?} inside a backtick- or double-quoted identifier is not a parameter, so
     * only the final {@code ?} substitutes and the identifiers stay verbatim. Actual:
     * the scanner in {@code ChPreparedStatement} only tracks single-quoted string
     * literals, so it rewrites the {@code ?} inside {@code `v2?`} and {@code `v1?`},
     * corrupting the identifiers (and consuming bindings in the wrong positions).
     *
     * <p>HOW TO FIX: in
     * {@code src/main/java/io/github/danielbunting/clickhouse/jdbc/ChPreparedStatement.java},
     * extend the shared scanning loop used by {@code countPlaceholders},
     * {@code substitute} and {@code rewriteToNamedParams} to also track backtick
     * ({@code `}) and double-quote ({@code "}) identifier state exactly like the
     * existing {@code inString} flag (including the doubled-quote escape, e.g.
     * {@code `v?``1`}), and treat {@code ?} as text while inside either.
     */
    @Test
    void knownBug_substituteMustNotReplacePlaceholdersInsideQuotedIdentifiers()
            throws SQLException {
        assertEquals("SELECT `v2?` FROM t WHERE `v1?` = 3",
                ChPreparedStatement.substitute(
                        "SELECT `v2?` FROM t WHERE `v1?` = ?", new Object[] {null, 3}));
        assertEquals("SELECT \"v?\" FROM t WHERE x = 7",
                ChPreparedStatement.substitute(
                        "SELECT \"v?\" FROM t WHERE x = ?", new Object[] {null, 7}));
    }

    // ---- temporal object renderings (v1 testReadWriteDateTimeWithNanos setObject path,
    // jdbc-v2 JDBCDateTimeTests#testTimestampInRange ZonedDateTime binding) ------------

    /** LocalDateTime binds via the fallback path as an ISO 'T' literal (CH-parseable). */
    @Test
    void localDateTimeRendersIsoLiteral() throws SQLException {
        assertEquals("'2021-04-02T03:35:45.321'", renderedLiteral(
                ps -> ps.setObject(1, LocalDateTime.of(2021, 4, 2, 3, 35, 45, 321_000_000))));
    }

    /**
     * KNOWN BUG — this test asserts the CORRECT behavior and fails until fixed.
     *
     * <p>Expected (jdbc-v2 supports {@code setObject(ZonedDateTime)} natively, see
     * {@code JDBCDateTimeTests#testTimestampInRange}): a ZonedDateTime/OffsetDateTime
     * identifies an instant, so it should render the same way an equivalent
     * {@link java.time.Instant} does — the UTC wall clock, which the server parses
     * into a DateTime/DateTime64. Actual: both types miss a {@code toLiteral} branch
     * and fall through to {@code toString()}, yielding ISO forms with a zone suffix
     * ({@code 2025-01-01T10:00Z[UTC]} / {@code 2025-01-01T10:00+02:00}) that
     * ClickHouse cannot parse, so any bind of these types fails server-side.
     *
     * <p>HOW TO FIX: in
     * {@code src/main/java/io/github/danielbunting/clickhouse/jdbc/ChPreparedStatement.java},
     * add to {@code toLiteral(Object)} (next to the {@code Instant} branch):
     * {@code if (value instanceof ZonedDateTime zdt) return quote(formatDateTime(
     * LocalDateTime.ofInstant(zdt.toInstant(), ZoneOffset.UTC)));} and the analogous
     * branch for {@code OffsetDateTime}. Mirror the same two branches in
     * {@code QueryParameters.toText} (clickhouse-native-client
     * {@code src/main/kotlin/io/github/danielbunting/clickhouse/QueryParameters.kt})
     * so the server-side path renders identically.
     */
    @Test
    void knownBug_zonedAndOffsetDateTimeMustRenderUtcWallClockLiterals() {
        ZonedDateTime zdt = ZonedDateTime.of(2025, 1, 1, 10, 0, 0, 0, ZoneId.of("UTC"));
        assertEquals("'2025-01-01 10:00:00'", ChPreparedStatement.toLiteral(zdt));
        // 10:00 at +02:00 is 08:00 UTC.
        OffsetDateTime odt = OffsetDateTime.of(2025, 1, 1, 10, 0, 0, 0, ZoneOffset.ofHours(2));
        assertEquals("'2025-01-01 08:00:00'", ChPreparedStatement.toLiteral(odt));
    }

    // ---- binding reuse across executions (jdbc-v2 testClearParameters companion) -----

    /**
     * Bindings survive an execution: executing again without re-binding reuses the same
     * values, and re-binding a single index changes only that parameter (addBatch, by
     * contrast, clears the bindings — pinned in ChPreparedStatementTest).
     */
    @Test
    void bindingsAreRetainedAcrossExecutionsAndPartiallyRebindable() throws SQLException {
        RecordingCore core = new RecordingCore();
        ChPreparedStatement ps = new ChPreparedStatement(
                conn(core), "INSERT INTO t (a, b) VALUES (?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, "one");
        ps.executeUpdate();
        ps.executeUpdate(); // no re-bind: same values again
        ps.setString(2, "two"); // partial re-bind
        ps.executeUpdate();

        assertEquals(List.of(
                        "INSERT INTO t (a, b) VALUES (1, 'one')",
                        "INSERT INTO t (a, b) VALUES (1, 'one')",
                        "INSERT INTO t (a, b) VALUES (1, 'two')"),
                core.executed);
    }
}
