package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Additional {@link PreparedStatement} coverage ported from the official clickhouse-java
 * test suites (v1 {@code ClickHousePreparedStatementTest} and jdbc-v2
 * {@code PreparedStatementTest}): batched DDL/query semantics, byte-array binding,
 * {@code INSERT ... WITH ... SELECT}, user-written multi-tuple {@code VALUES},
 * {@code SETTINGS} clauses, result-set metadata, database-qualified identifiers, and
 * Calendar-variant date binding. Where a behavior depends on parameter binding both
 * modes are exercised (client-side interpolation and {@code server_side_params=true}).
 */
@Tag("integration")
@Testcontainers
class JdbcPreparedStatementExtrasIT {

    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse><users><default><networks replace=\"replace\">"
            + "<ip>::/0</ip></networks></default></users></clickhouse>";

    private static final int NATIVE_PORT = 9000;

    @Container
    @SuppressWarnings("resource")
    private static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(ClickHouseImages.SERVER)
                    .withExposedPorts(NATIVE_PORT)
                    .withCopyToContainer(
                            Transferable.of(OPEN_DEFAULT_USER_XML),
                            "/etc/clickhouse-server/users.d/zz-open-default.xml")
                    .waitingFor(Wait.forListeningPort());

    private static String url() {
        return "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
    }

    private static Connection connect(boolean serverSideParams) throws SQLException {
        Properties props = new Properties();
        props.setProperty("server_side_params", Boolean.toString(serverSideParams));
        return DriverManager.getConnection(url(), props);
    }

    private static String mode(boolean serverSide) {
        return serverSide ? "srv" : "cli";
    }

    // ---- batch DDL (v1 testBatchDdl) ----------------------------------------

    /**
     * DDL statements (no parameters) accumulated with {@code addBatch()} execute on
     * {@code executeBatch()}. Our driver's documented contract for non-INSERT batches
     * is to execute each statement in turn (it does not reject DDL batches the way
     * clickhouse-java v1 rejects batched queries).
     */
    private void batchDdl(boolean serverSide) throws Exception {
        String table = "pstmt_batch_ddl_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + table + " (id Int32) ENGINE = Memory")) {
                ps.addBatch();
                int[] counts = ps.executeBatch();
                assertEquals(1, counts.length);
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("EXISTS TABLE " + table)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            // Idempotent DDL batched twice runs both entries (reference batches
            // "DROP TABLE IF EXISTS" twice and expects two results).
            try (PreparedStatement ps = conn.prepareStatement(
                    "DROP TABLE IF EXISTS " + table)) {
                ps.addBatch();
                ps.addBatch();
                int[] counts = ps.executeBatch();
                assertEquals(2, counts.length);
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("EXISTS TABLE " + table)) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void batchDdl_clientSide() throws Exception {
        batchDdl(false);
    }

    @Test
    void batchDdl_serverSideParams() throws Exception {
        batchDdl(true);
    }

    // ---- batch of a parameterized SELECT (v1 testBatchQuery) ----------------

    /**
     * Batch semantics for a parameterized SELECT. An empty batch returns an empty
     * result array, and out-of-range parameter indexes throw. Unlike clickhouse-java
     * (which throws {@code BatchUpdateException} on {@code executeBatch} of a query),
     * our driver's documented contract executes each substituted non-INSERT statement
     * in turn and discards any result rows, so a batched SELECT succeeds.
     */
    private void batchQuery(boolean serverSide) throws Exception {
        try (Connection conn = connect(serverSide);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT * FROM numbers(100) WHERE number < ?")) {
            assertEquals(0, ps.executeBatch().length);
            assertThrows(SQLException.class, () -> ps.setInt(0, 5));
            assertThrows(SQLException.class, () -> ps.setInt(2, 5));

            ps.setInt(1, 3);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            // Driver contract divergence from clickhouse-java: executes rather than throws.
            int[] counts = ps.executeBatch();
            assertEquals(2, counts.length);
            assertEquals(0, ps.executeBatch().length);
        }
    }

    @Test
    void batchQuery_clientSide() throws Exception {
        batchQuery(false);
    }

    @Test
    void batchQuery_serverSideParams() throws Exception {
        batchQuery(true);
    }

    // ---- setBytes into a String column (v1 testInsertByteArray) -------------

    /**
     * {@code setBytes} binds a byte array into a String column; the bytes are treated
     * as UTF-8 text by the driver and round-trip through both {@code getBytes} and
     * {@code getString}.
     */
    private void insertByteArrayIntoStringColumn(boolean serverSide) throws Exception {
        String table = "pstmt_bytes_col_" + mode(serverSide);
        byte[] payload = "byte-array éü".getBytes(StandardCharsets.UTF_8);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, s String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, s) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, payload);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT s FROM " + table)) {
                assertTrue(rs.next());
                assertArrayEquals(payload, rs.getBytes(1));
                assertEquals(new String(payload, StandardCharsets.UTF_8), rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void insertByteArrayIntoStringColumn_clientSide() throws Exception {
        insertByteArrayIntoStringColumn(false);
    }

    @Test
    void insertByteArrayIntoStringColumn_serverSideParams() throws Exception {
        insertByteArrayIntoStringColumn(true);
    }

    // ---- setBytes on a SELECT parameter (v2 testSetBytes) -------------------

    /** {@code SELECT ?} with a bound byte array reads back the same bytes. */
    private void setBytesSelectRoundTrip(boolean serverSide) throws Exception {
        byte[] payload = new byte[] {1, 2, 3};
        try (Connection conn = connect(serverSide);
                PreparedStatement ps = conn.prepareStatement("SELECT ?")) {
            ps.setBytes(1, payload);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertArrayEquals(payload, rs.getBytes(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void setBytesSelectRoundTrip_clientSide() throws Exception {
        setBytesSelectRoundTrip(false);
    }

    @Test
    void setBytesSelectRoundTrip_serverSideParams() throws Exception {
        setBytesSelectRoundTrip(true);
    }

    // ---- INSERT ... WITH ... SELECT (v1 testInsertWithAndSelect) ------------

    /** An INSERT whose source is a WITH/CTE SELECT executes and lands rows. */
    private void insertWithCteSelect(boolean serverSide) throws Exception {
        String table = "pstmt_with_cte_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (value String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table
                    + " (value) WITH t AS (SELECT 'testValue1') SELECT * FROM t")) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table
                    + " (value) WITH t AS (SELECT 'testValue2' AS value)"
                    + " SELECT * FROM t WHERE value != ?")) {
                ps.setString(1, "");
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT value FROM " + table + " ORDER BY value")) {
                assertTrue(rs.next());
                assertEquals("testValue1", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("testValue2", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void insertWithCteSelect_clientSide() throws Exception {
        insertWithCteSelect(false);
    }

    @Test
    void insertWithCteSelect_serverSideParams() throws Exception {
        insertWithCteSelect(true);
    }

    // ---- user-written multi-tuple VALUES (v1 testInsertWithMultipleValues) --

    /**
     * A user-written multi-row {@code VALUES (?, ?), (?, ?)} binds parameters across
     * tuples in placeholder order, including a NULL for a Nullable column.
     *
     * <p>The NULL binding is exercised on the client-side path only: with
     * {@code server_side_params=true} every placeholder is declared {@code {_pN:String}}
     * (non-Nullable), so the server rejects the NULL sentinel with
     * "Cannot parse quoted string: expected opening quote ''', got 'N'" — asserted as a
     * bug by {@link #knownBug_nullBindingMustWorkWithServerSideParams()}.
     */
    private void insertWithMultipleValues(boolean serverSide) throws Exception {
        String table = "pstmt_multi_values_" + mode(serverSide);
        String firstB = serverSide ? "sv" : null;
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (a Int32, b Nullable(String)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (?, ?), (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, firstB);
                ps.setInt(3, 2);
                ps.setString(4, "er");
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT a, b FROM " + table + " ORDER BY a")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(firstB, rs.getObject(2));
                assertEquals(firstB == null, rs.wasNull());
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals("er", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void insertWithMultipleValues_clientSide() throws Exception {
        insertWithMultipleValues(false);
    }

    @Test
    void insertWithMultipleValues_serverSideParams() throws Exception {
        insertWithMultipleValues(true);
    }

    /**
     * KNOWN BUG — this test asserts the CORRECT behavior and fails until fixed.
     *
     * <p>Expected (JDBC spec / jdbc-v2 {@code PreparedStatementTest}): binding SQL NULL
     * (via {@code setNull} or a null {@code setString}) works regardless of the binding
     * mode, so an INSERT of NULL into a {@code Nullable(String)} column round-trips with
     * {@code server_side_params=true} exactly as it does client-side. Actual:
     * {@code ChPreparedStatement.rewriteToNamedParams} declares every placeholder as the
     * non-Nullable {@code {_pN:String}}, so when the null binding travels as the
     * {@code \N} sentinel the server rejects it with "Cannot parse quoted string:
     * expected opening quote ''', got 'N'" and {@code executeUpdate} throws.
     *
     * <p>HOW TO FIX: in
     * {@code src/main/java/io/github/danielbunting/clickhouse/jdbc/ChPreparedStatement.java},
     * method {@code rewriteToNamedParams}, declare the placeholders as
     * {@code {_pN:Nullable(String)}} so the server accepts the {@code \N} sentinel
     * (verify the server accepts Nullable-typed query parameters in the target contexts —
     * ClickHouse supports Nullable param types for scalar expressions; contexts that
     * reject them, e.g. identifiers, already fail today). Alternatively, fall back to
     * client-side interpolation in {@code executeQuery}/{@code executeUpdate} when any
     * bound value is null.
     */
    @Test
    void knownBug_nullBindingMustWorkWithServerSideParams() throws Exception {
        String table = "pstmt_null_bind_srv";
        try (Connection conn = connect(true)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int32, s Nullable(String)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, s) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT id, s FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertNull(rs.getObject(2), "the bound NULL must round-trip");
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
        }
    }

    // ---- INSERT ... SETTINGS ... VALUES (v1 testInsertWithSettings) ---------

    /**
     * An INSERT carrying a {@code SETTINGS} clause before {@code VALUES} still binds
     * placeholders and lands rows, both as a direct execute and via the batch path
     * (which, client-side, collapses to a multi-row VALUES after the SETTINGS prefix).
     */
    private void insertWithSettings(boolean serverSide) throws Exception {
        String table = "pstmt_settings_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (i Int32, s String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " SETTINGS async_insert=0 VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "one");
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setString(2, "two");
                ps.addBatch();
                ps.setInt(1, 3);
                ps.setString(2, "three");
                ps.addBatch();
                int[] counts = ps.executeBatch();
                assertEquals(2, counts.length);
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT count(), sum(i) FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals(6, rs.getInt(2));
            }
        }
    }

    @Test
    void insertWithSettings_clientSide() throws Exception {
        insertWithSettings(false);
    }

    @Test
    void insertWithSettings_serverSideParams() throws Exception {
        insertWithSettings(true);
    }

    // ---- result-set metadata (v2 testGetMetadata) ----------------------------

    /**
     * {@code PreparedStatement.getMetaData()} is a documented
     * {@link SQLFeatureNotSupportedException} in this driver (the native client cannot
     * describe a result without executing, and the driver does not surface the executed
     * result's metadata through the statement either — jdbc-v2 does). Column
     * count/names/type are asserted through {@link ResultSet#getMetaData()} instead.
     */
    private void getMetaDataContract(boolean serverSide) throws Exception {
        String table = "pstmt_metadata_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (a1 String, b2 Float64, b3 Float64) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT a1, b2, b3 FROM " + table + " WHERE a1 != ?")) {
                // Pre-execution: documented unsupported.
                assertThrows(SQLFeatureNotSupportedException.class, ps::getMetaData);

                ps.setString(1, "");
                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData md = rs.getMetaData();
                    assertEquals(3, md.getColumnCount());
                    assertEquals("a1", md.getColumnName(1));
                    assertEquals("b2", md.getColumnName(2));
                    assertEquals("b3", md.getColumnName(3));
                    assertEquals("String", md.getColumnTypeName(1));
                }
                // Post-execution: the statement-level accessor still reports unsupported
                // (contract divergence from jdbc-v2, which describes the executed result).
                assertThrows(SQLFeatureNotSupportedException.class, ps::getMetaData);
            }
        }
    }

    @Test
    void getMetaDataContract_clientSide() throws Exception {
        getMetaDataContract(false);
    }

    @Test
    void getMetaDataContract_serverSideParams() throws Exception {
        getMetaDataContract(true);
    }

    // ---- database-qualified identifiers (v2 testStatementsWithDatabaseInTableIdentifier)

    /**
     * Prepared INSERTs address a table in another database via a plain qualified name,
     * a backtick-quoted name, and an ANSI double-quoted name.
     */
    private void databaseQualifiedIdentifiers(boolean serverSide) throws Exception {
        String db = "pstmt_xdb_" + mode(serverSide);
        String tbl = "table1";
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE DATABASE IF NOT EXISTS " + db);
                st.execute("DROP TABLE IF EXISTS " + db + "." + tbl);
                st.execute("CREATE TABLE " + db + "." + tbl
                        + " (v1 Int32, v2 Int32) ENGINE = Memory");
            }
            String[] identifiers = {
                db + "." + tbl,
                "`" + db + "`.`" + tbl + "`",
                "\"" + db + "\".\"" + tbl + "\"",
            };
            for (int i = 0; i < identifiers.length; i++) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + identifiers[i] + " VALUES (?, ?)")) {
                    ps.setInt(1, i + 10);
                    ps.setInt(2, i + 20);
                    ps.executeUpdate();
                }
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT count(), sum(v1), sum(v2) FROM " + db + "." + tbl)) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals(10 + 11 + 12, rs.getInt(2));
                assertEquals(20 + 21 + 22, rs.getInt(3));
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP DATABASE IF EXISTS " + db);
            }
        }
    }

    @Test
    void databaseQualifiedIdentifiers_clientSide() throws Exception {
        databaseQualifiedIdentifiers(false);
    }

    @Test
    void databaseQualifiedIdentifiers_serverSideParams() throws Exception {
        databaseQualifiedIdentifiers(true);
    }

    // ---- setDate with/without Calendar (v2 testDateWithAndWithoutCalendar) --

    /**
     * {@code setDate(i, d)} and {@code setDate(i, d, cal)} store the same wall-clock
     * date for a {@code Date} column: the driver binds the date's local value and
     * ignores the Calendar, so an explicit UTC or offset Calendar cannot shift the day.
     */
    private void dateWithAndWithoutCalendar(boolean serverSide) throws Exception {
        String table = "pstmt_date_cal_" + mode(serverSide);
        Date testDate = Date.valueOf("2024-06-15");
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, d Date) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, d) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setDate(2, testDate); // no Calendar
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setDate(2, testDate, new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                ps.executeUpdate();

                ps.setInt(1, 3);
                ps.setDate(2, testDate,
                        new GregorianCalendar(TimeZone.getTimeZone("America/Los_Angeles")));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, d FROM " + table + " ORDER BY id")) {
                for (int id = 1; id <= 3; id++) {
                    assertTrue(rs.next());
                    assertEquals(id, rs.getInt(1));
                    assertEquals(testDate.toString(), rs.getDate(2).toString());
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void dateWithAndWithoutCalendar_clientSide() throws Exception {
        dateWithAndWithoutCalendar(false);
    }

    @Test
    void dateWithAndWithoutCalendar_serverSideParams() throws Exception {
        dateWithAndWithoutCalendar(true);
    }

    // ======================================================================
    // Wave 2: setter-matrix and clause-shape ports (v1 ClickHousePreparedStatementTest,
    // jdbc-v2 PreparedStatementTest / JDBCDateTimeTests, DateTimeComparisonTest).
    // ======================================================================

    // ---- Bool writes (v1 testReadWriteBool) ----------------------------------

    /**
     * Bool columns accept {@code setBoolean} (rendered 0/1 client-side, true/false
     * server-side), a {@code "true"}/{@code "false"} string, and {@code setObject}
     * of a boxed Boolean.
     */
    private void boolWriteRoundTrip(boolean serverSide) throws Exception {
        String table = "pstmt_bool_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, b Bool) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, b) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setBoolean(2, true);
                ps.executeUpdate();
                ps.setInt(1, 2);
                ps.setBoolean(2, false);
                ps.executeUpdate();
                ps.setInt(1, 3);
                ps.setString(2, "true");
                ps.executeUpdate();
                ps.setInt(1, 4);
                ps.setString(2, "false");
                ps.executeUpdate();
                ps.setInt(1, 5);
                ps.setObject(2, Boolean.TRUE);
                ps.executeUpdate();
            }
            boolean[] expected = {true, false, true, false, true};
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, b FROM " + table + " ORDER BY id")) {
                for (int id = 1; id <= expected.length; id++) {
                    assertTrue(rs.next());
                    assertEquals(id, rs.getInt(1));
                    assertEquals(expected[id - 1], rs.getBoolean(2), "row " + id);
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void boolWriteRoundTrip_clientSide() throws Exception {
        boolWriteRoundTrip(false);
    }

    @Test
    void boolWriteRoundTrip_serverSideParams() throws Exception {
        boolWriteRoundTrip(true);
    }

    // ---- setBytes into FixedString + predicate (v1 testReadWriteBinaryString) ----

    /**
     * {@code setBytes} binds into FixedString and Nullable(String) columns and works
     * as a WHERE predicate against a FixedString column. The NULL binding for the
     * Nullable column is client-side only (server-side {_pN:String} placeholders are
     * non-Nullable — asserted as a bug by
     * {@link #knownBug_nullBindingMustWorkWithServerSideParams()}).
     */
    private void setBytesFixedStringMatrix(boolean serverSide) throws Exception {
        String table = "pstmt_fixed_bytes_" + mode(serverSide);
        byte[] abc = {0x61, 0x62, 0x63}; // "abc"
        byte[] nullable = serverSide ? abc : null;
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int32, f0 FixedString(3), s0 String, s1 Nullable(String))"
                        + " ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, f0, s0, s1) VALUES (?, ?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, abc);
                ps.setBytes(3, abc);
                ps.setBytes(4, nullable);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT f0, s0, s1 FROM " + table + " WHERE f0 = ?")) {
                ps.setBytes(1, abc);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertArrayEquals(abc, rs.getBytes(1));
                    assertEquals("abc", rs.getString(1));
                    assertArrayEquals(abc, rs.getBytes(2));
                    if (serverSide) {
                        assertArrayEquals(abc, rs.getBytes(3));
                    } else {
                        assertNull(rs.getBytes(3));
                        assertTrue(rs.wasNull());
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    void setBytesFixedStringMatrix_clientSide() throws Exception {
        setBytesFixedStringMatrix(false);
    }

    @Test
    void setBytesFixedStringMatrix_serverSideParams() throws Exception {
        setBytesFixedStringMatrix(true);
    }

    // ---- Date / Date32 write matrix (v1 testReadWriteDate, jdbc-v2 testSetDate,
    // JDBCDateTimeTests#testDaysBeforeBirthdayParty binding dimension) -------------

    /**
     * Date and Date32 columns accept {@code setDate}, {@code setObject(LocalDate)},
     * {@code setObject(java.sql.Date)} and {@code setObject(String)}; reads come back
     * through {@code getDate}, {@code getObject(LocalDate.class)} and
     * {@code getObject(String.class)}. Also covers a date parameter inside a
     * function call ({@code SELECT toDate(?)}, jdbc-v2 testSetDate). The reference's
     * session-timezone dimension has no counterpart (no session_timezone property).
     */
    private void dateWriteMatrix(boolean serverSide) throws Exception {
        String table = "pstmt_date_matrix_" + mode(serverSide);
        LocalDate ld = LocalDate.of(2021, 3, 25);
        Date d = Date.valueOf(ld);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int32, d1 Date, d2 Date32) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, d1, d2) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setDate(2, d);
                ps.setDate(3, d);
                ps.executeUpdate();
                ps.setInt(1, 2);
                ps.setObject(2, ld);
                ps.setObject(3, ld);
                ps.executeUpdate();
                ps.setInt(1, 3);
                ps.setObject(2, d);
                ps.setObject(3, d);
                ps.executeUpdate();
                ps.setInt(1, 4);
                ps.setObject(2, "2021-03-25");
                ps.setObject(3, "2021-03-25");
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, d1, d2 FROM " + table + " ORDER BY id")) {
                for (int id = 1; id <= 4; id++) {
                    assertTrue(rs.next());
                    assertEquals(id, rs.getInt(1));
                    for (int col = 2; col <= 3; col++) {
                        assertEquals(d.toString(), rs.getDate(col).toString(), "row " + id);
                        assertEquals(ld, rs.getObject(col, LocalDate.class), "row " + id);
                        assertEquals("2021-03-25", rs.getObject(col, String.class), "row " + id);
                    }
                }
                assertFalse(rs.next());
            }
            // Date parameter as a function argument (jdbc-v2 testSetDate).
            try (PreparedStatement ps = conn.prepareStatement("SELECT toDate(?)")) {
                ps.setDate(1, d);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(d.toString(), rs.getDate(1).toString());
                    assertFalse(rs.next());
                }
                ps.setObject(1, Date.valueOf("2021-01-02"));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("2021-01-02", rs.getDate(1).toString());
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    void dateWriteMatrix_clientSide() throws Exception {
        dateWriteMatrix(false);
    }

    @Test
    void dateWriteMatrix_serverSideParams() throws Exception {
        dateWriteMatrix(true);
    }

    // ---- DateTime64(9) nanosecond writes (v1 testReadWriteDateTimeWithNanos) ----

    /**
     * Sub-second precision survives the write path to the full nanosecond:
     * {@code setTimestamp} with nanos and {@code setObject(LocalDateTime)} (which
     * binds via the ISO fallback literal) land the exact DateTime64(9) value.
     */
    private void dateTime64NanosRoundTrip(boolean serverSide) throws Exception {
        String table = "pstmt_dt64_nanos_" + mode(serverSide);
        LocalDateTime wall = LocalDateTime.of(2021, 4, 2, 3, 35, 45, 123_456_789);
        Instant expected = wall.toInstant(ZoneOffset.UTC); // server stores UTC wall clock
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id Int32, ts DateTime64(9)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, ts) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setTimestamp(2, Timestamp.valueOf(wall));
                ps.executeUpdate();
                ps.setInt(1, 2);
                ps.setObject(2, wall);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, ts FROM " + table + " ORDER BY id")) {
                for (int id = 1; id <= 2; id++) {
                    assertTrue(rs.next());
                    assertEquals(id, rs.getInt(1));
                    assertEquals(expected, rs.getObject(2), "row " + id);
                    assertEquals(Timestamp.from(expected), rs.getTimestamp(2), "row " + id);
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void dateTime64NanosRoundTrip_clientSide() throws Exception {
        dateTime64NanosRoundTrip(false);
    }

    @Test
    void dateTime64NanosRoundTrip_serverSideParams() throws Exception {
        dateTime64NanosRoundTrip(true);
    }

    // ---- Enum writes (v1 testReadWriteEnums) ---------------------------------

    /** Enum8/Enum16 columns accept the member name via setString/setObject. */
    private void enumWriteByName(boolean serverSide) throws Exception {
        String table = "pstmt_enum_name_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, e1 Enum8('v1'=1,'v2'=2),"
                        + " e2 Enum16('v11'=11,'v22'=22)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, e1, e2) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "v1");
                ps.setObject(3, "v11");
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT e1, e2 FROM " + table)) {
                assertTrue(rs.next());
                assertEquals("v1", rs.getString(1));
                assertEquals("v11", rs.getObject(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void enumWriteByName_clientSide() throws Exception {
        enumWriteByName(false);
    }

    @Test
    void enumWriteByName_serverSideParams() throws Exception {
        enumWriteByName(true);
    }

    /**
     * Enum members can also be written by numeric value (setObject(int)/setByte).
     * Client-side only: the server-side rewrite types every parameter String, and
     * ClickHouse resolves a String cast to Enum by <em>name</em>, so "2" is not a
     * valid member name — a known limitation of the all-String rewrite.
     */
    @Test
    void enumWriteByValue_clientSide() throws Exception {
        String table = "pstmt_enum_value_cli";
        try (Connection conn = connect(false)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, e1 Enum8('v1'=1,'v2'=2),"
                        + " e2 Enum16('v11'=11,'v22'=22)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, e1, e2) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, 2);
                ps.setByte(3, (byte) 22);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT e1, e2 FROM " + table)) {
                assertTrue(rs.next());
                assertEquals("v2", rs.getString(1));
                assertEquals("v22", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- arrays with Nullable elements (v1 testReadWriteArrayWithNullableTypes,
    // jdbc-v2 testEncodingArray live dimension) --------------------------------

    /**
     * Interior NULL elements in Array(Nullable(T)) columns survive a setObject write
     * (Object[]/List with nulls, and empty primitive arrays). Client-side only: array
     * values cannot travel as all-String server-side parameters.
     */
    @Test
    void arrayWithNullableElementsWrite_clientSide() throws Exception {
        String table = "pstmt_null_arrays_cli";
        try (Connection conn = connect(false)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, a1 Array(Nullable(Int8)),"
                        + " a2 Array(Nullable(Int64))) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, a1, a2) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, new int[0]);
                ps.setObject(3, new long[0]);
                ps.executeUpdate();
                ps.setInt(1, 2);
                ps.setObject(2, new Object[] {2, null});
                ps.setObject(3, Arrays.asList(2L, null));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, a1, a2 FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(0, ((Object[]) rs.getArray(2).getArray()).length);
                assertEquals(0, ((Object[]) rs.getArray(3).getArray()).length);
                assertTrue(rs.next());
                Object[] a1 = (Object[]) rs.getArray(2).getArray();
                assertEquals(2, a1.length);
                assertEquals(2L, ((Number) a1[0]).longValue());
                assertNull(a1[1]);
                Object[] a2 = (Object[]) rs.getArray(3).getArray();
                assertEquals(2, a2.length);
                assertEquals(2L, ((Number) a2[0]).longValue());
                assertNull(a2[1]);
                assertFalse(rs.next());
            }
        }
    }

    // ---- non-batch executeUpdate failure class (v1 testNonBatchUpdate) --------

    /**
     * A failed single-shot {@code executeUpdate} surfaces as a plain
     * {@link SQLException} — never a {@link BatchUpdateException}.
     */
    private void nonBatchUpdateFailureIsPlainSqlException(boolean serverSide) throws Exception {
        String table = "pstmt_nonbatch_fail_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, s String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, s) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "1");
                ps.executeUpdate();
                try (Statement st = conn.createStatement()) {
                    st.execute("DROP TABLE " + table);
                }
                ps.setInt(1, 2);
                ps.setString(2, "2");
                SQLException e = assertThrows(SQLException.class, ps::executeUpdate);
                assertFalse(e instanceof BatchUpdateException,
                        "single-shot failure must not be a BatchUpdateException");
            }
        }
    }

    @Test
    void nonBatchUpdateFailureIsPlainSqlException_clientSide() throws Exception {
        nonBatchUpdateFailureIsPlainSqlException(false);
    }

    @Test
    void nonBatchUpdateFailureIsPlainSqlException_serverSideParams() throws Exception {
        nonBatchUpdateFailureIsPlainSqlException(true);
    }

    // ---- Nested(...) insert via flattened columns (v1 testInsertNestedValue) ----

    /**
     * A Nested(c1, c2) column is written through its flattened {@code n.c1}/{@code n.c2}
     * array columns with setObject array bindings. Client-side only (array bindings);
     * the reference's value-object and flatten_nested=0 variants have no counterpart.
     */
    @Test
    void nestedFlattenedArraysInsert_clientSide() throws Exception {
        String table = "pstmt_nested_cli";
        try (Connection conn = connect(false)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table
                        + " (id UInt32, n Nested(c1 Int8, c2 Int8)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (id, `n.c1`, `n.c2`) VALUES (?, ?, ?)")) {
                ps.setInt(1, 0);
                ps.setObject(2, new int[0]);
                ps.setObject(3, new int[0]);
                ps.executeUpdate();
                ps.setInt(1, 1);
                ps.setObject(2, new int[] {1, 2});
                ps.setObject(3, new int[] {3, 4});
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT id, n.c1, n.c2 FROM " + table + " ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(0, ((Object[]) rs.getArray(2).getArray()).length);
                assertEquals(0, ((Object[]) rs.getArray(3).getArray()).length);
                assertTrue(rs.next());
                Object[] c1 = (Object[]) rs.getArray(2).getArray();
                Object[] c2 = (Object[]) rs.getArray(3).getArray();
                assertEquals(1L, ((Number) c1[0]).longValue());
                assertEquals(2L, ((Number) c1[1]).longValue());
                assertEquals(3L, ((Number) c2[0]).longValue());
                assertEquals(4L, ((Number) c2[1]).longValue());
                assertFalse(rs.next());
            }
        }
    }

    // ---- setTimestamp Calendar variants into DateTime64 (DateTimeComparisonTest
    // setTimestampTest) ---------------------------------------------------------

    /**
     * {@code setTimestamp} with and without a Calendar stores the same wall-clock
     * instant (the Calendar is ignored by design), and a nanosecond-precision value
     * is truncated — not rounded — by a lower-precision DateTime64(3) column.
     */
    private void timestampCalendarVariantsStoreWallClock(boolean serverSide) throws Exception {
        String table = "pstmt_ts_cal_" + mode(serverSide);
        LocalDateTime wall = LocalDateTime.of(2021, 1, 1, 1, 23, 45, 123_456_789);
        Timestamp ts = Timestamp.valueOf(wall);
        Instant full = wall.toInstant(ZoneOffset.UTC);
        Instant millis = wall.withNano(123_000_000).toInstant(ZoneOffset.UTC);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int8, t0 DateTime64(3),"
                        + " t1 DateTime64(9), t2 DateTime64(9), t3 DateTime64(9))"
                        + " ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " VALUES (1, ?, ?, ?, ?)")) {
                ps.setTimestamp(1, ts);
                ps.setTimestamp(2, ts);
                ps.setTimestamp(3, ts, new GregorianCalendar());
                ps.setTimestamp(4, ts, new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT t0, t1, t2, t3 FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(millis, rs.getObject(1), "DateTime64(3) truncates");
                assertEquals(full, rs.getObject(2));
                assertEquals(full, rs.getObject(3), "default Calendar is ignored");
                assertEquals(full, rs.getObject(4), "UTC Calendar is ignored");
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void timestampCalendarVariantsStoreWallClock_clientSide() throws Exception {
        timestampCalendarVariantsStoreWallClock(false);
    }

    @Test
    void timestampCalendarVariantsStoreWallClock_serverSideParams() throws Exception {
        timestampCalendarVariantsStoreWallClock(true);
    }

    // ---- scalar setter matrix on SELECT ? (jdbc-v2 testSetByte/Short/Float,
    // DateTimeComparisonTest setTimeTest write dimension) ------------------------

    /** setByte/setShort/setFloat/setTime round-trip through {@code SELECT ?}. */
    private void scalarSetterMatrix(boolean serverSide) throws Exception {
        try (Connection conn = connect(serverSide);
                PreparedStatement ps = conn.prepareStatement("SELECT ?, ?, ?, ?")) {
            ps.setByte(1, (byte) 7);
            ps.setShort(2, (short) 300);
            ps.setFloat(3, 1.5f);
            ps.setTime(4, Time.valueOf("12:34:56"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals((byte) 7, rs.getByte(1));
                assertEquals((short) 300, rs.getShort(2));
                assertEquals(1.5f, rs.getFloat(3), 0.0f);
                // Time has no ClickHouse scalar counterpart; it binds as its text form.
                assertEquals("12:34:56", rs.getString(4));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void scalarSetterMatrix_clientSide() throws Exception {
        scalarSetterMatrix(false);
    }

    @Test
    void scalarSetterMatrix_serverSideParams() throws Exception {
        scalarSetterMatrix(true);
    }

    // ---- primitive arrays through SELECT ? (jdbc-v2 testPrimitiveArrays) --------

    /**
     * {@code setObject(String[][])} and {@code setObject(Object[])} bind as array
     * literals and read back through {@code getArray}. Client-side only: arrays
     * cannot travel as all-String server-side parameters.
     */
    @Test
    void primitiveArraysSelectRoundTrip_clientSide() throws Exception {
        try (Connection conn = connect(false)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?")) {
                ps.setObject(1, new String[][] {{"a"}, {"b"}, {"c"}});
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    java.sql.Array a1 = rs.getArray(1);
                    assertEquals("[[a], [b], [c]]",
                            Arrays.deepToString((Object[]) a1.getArray()));
                    assertFalse(rs.next());
                }
                ps.setObject(1, new Object[] {1, 2, 3});
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("[1, 2, 3]",
                            Arrays.deepToString((Object[]) rs.getArray(1).getArray()));
                    assertFalse(rs.next());
                }
            }
        }
    }

    // ---- WITH-clause shapes (jdbc-v2 testWithClause / testMultipleWithClauses /
    // testRecursiveWithClause) ---------------------------------------------------

    /** Prepared CTE queries: plain WITH, chained CTEs, and WITH RECURSIVE. */
    private void withClauseVariants(boolean serverSide) throws Exception {
        try (Connection conn = connect(serverSide)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "WITH data AS (SELECT number FROM numbers(100)) SELECT count() FROM data")) {
                assertTrue(ps.execute());
                try (ResultSet rs = ps.getResultSet()) {
                    assertTrue(rs.next());
                    assertEquals(100, rs.getInt(1));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "WITH data1 AS (SELECT 1 AS a), data2 AS (SELECT a + 1 AS b FROM data1) "
                            + "SELECT * FROM data2")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) "
                            + "SELECT * FROM r ORDER BY n")) {
                try (ResultSet rs = ps.executeQuery()) {
                    for (int i = 1; i <= 5; i++) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    void withClauseVariants_clientSide() throws Exception {
        withClauseVariants(false);
    }

    @Test
    void withClauseVariants_serverSideParams() throws Exception {
        withClauseVariants(true);
    }

    // ---- parameters inside WITH clauses (jdbc-v2 testWithClauseWithParams) ------

    /** A parameter bound inside a CTE prologue ({@code toDateTime(?)}) substitutes. */
    private void withClauseParams(boolean serverSide) throws Exception {
        String table = "pstmt_with_params_" + mode(serverSide);
        LocalDateTime wall = LocalDateTime.of(2025, 6, 1, 10, 20, 30);
        Instant expected = wall.toInstant(ZoneOffset.UTC);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (v1 String) ENGINE = Memory");
                st.execute("INSERT INTO " + table + " VALUES ('A'), ('B')");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "WITH toDateTime(?) AS target_time, (SELECT 123) AS magic_number "
                            + "SELECT v1, target_time, magic_number FROM " + table
                            + " ORDER BY v1")) {
                ps.setTimestamp(1, Timestamp.valueOf(wall));
                try (ResultSet rs = ps.executeQuery()) {
                    assertEquals(3, rs.getMetaData().getColumnCount());
                    int count = 0;
                    while (rs.next()) {
                        assertEquals(expected, rs.getObject("target_time"));
                        assertEquals(123, rs.getInt("magic_number"));
                        count++;
                    }
                    assertEquals(2, count);
                }
            }
        }
    }

    @Test
    void withClauseParams_clientSide() throws Exception {
        withClauseParams(false);
    }

    @Test
    void withClauseParams_serverSideParams() throws Exception {
        withClauseParams(true);
    }

    /**
     * Parameters in a CTE body and the outer WHERE bind in placeholder order
     * (jdbc-v2 testWithClauseWithMultipleParameters). Client-side only because the
     * first parameter is a {@code numbers(?)} table-function argument, which the
     * all-String server-side rewrite cannot type (see
     * {@link #selectFromTableFunctionArg_clientSide()}).
     */
    @Test
    void withClauseMultipleParameters_clientSide() throws Exception {
        try (Connection conn = connect(false);
                PreparedStatement ps = conn.prepareStatement(
                        "WITH data AS ((SELECT number AS n FROM numbers(?) WHERE n > ?)) "
                                + "SELECT * FROM data WHERE n < ? ORDER BY n")) {
            ps.setInt(1, 10);
            ps.setInt(2, 3);
            ps.setInt(3, 7);
            try (ResultSet rs = ps.executeQuery()) {
                for (int n = 4; n <= 6; n++) {
                    assertTrue(rs.next());
                    assertEquals(n, rs.getInt(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    // ---- parameter as table-function argument (jdbc-v2 testSelectFromArray) -----

    /**
     * A parameter as a table-function argument ({@code numbers(?)}). Client-side
     * only: the server-side rewrite declares {_p1:String} and ClickHouse rejects a
     * String argument to numbers() — a known limitation of the all-String rewrite.
     */
    @Test
    void selectFromTableFunctionArg_clientSide() throws Exception {
        try (Connection conn = connect(false);
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM numbers(?)")) {
            ps.setInt(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(10, count);
            }
        }
    }

    // ---- unbound-column CTE (jdbc-v2 testCTEWithUnboundCol) ----------------------

    /** {@code with ? as text} binds the parameter as the CTE scalar. */
    private void cteWithUnboundCol(boolean serverSide) throws Exception {
        try (Connection conn = connect(serverSide);
                PreparedStatement ps = conn.prepareStatement(
                        "with ? as text, numz as (select text, number from system.numbers limit 10) "
                                + "select * from numz")) {
            ps.setString(1, "1000");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("1000", rs.getString(1));
                assertEquals("0", rs.getString(2));
            }
        }
    }

    @Test
    void cteWithUnboundCol_clientSide() throws Exception {
        cteWithUnboundCol(false);
    }

    @Test
    void cteWithUnboundCol_serverSideParams() throws Exception {
        cteWithUnboundCol(true);
    }

    // ---- ?::type adjacency (jdbc-v2 testParamWithCast) ---------------------------

    /**
     * A placeholder immediately followed by a {@code ::type} cast binds, while a
     * quoted {@code '?::type'} stays a literal.
     */
    private void paramWithCast(boolean serverSide) throws Exception {
        java.util.UUID uuid = java.util.UUID.randomUUID();
        try (Connection conn = connect(serverSide);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT ?::Int32, '?::integer', ?::UUID, ?")) {
            ps.setString(1, "1000");
            ps.setString(2, uuid.toString());
            ps.setInt(3, 3003001);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1000, rs.getInt(1));
                assertEquals("?::integer", rs.getString(2));
                assertEquals(uuid.toString(), rs.getString(3));
                assertEquals(3003001, rs.getInt(4));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void paramWithCast_clientSide() throws Exception {
        paramWithCast(false);
    }

    @Test
    void paramWithCast_serverSideParams() throws Exception {
        paramWithCast(true);
    }

    // ---- IN-clause bindings (jdbc-v2 testWithInClause, JDBCDateTimeTests
    // testDateInRange) -------------------------------------------------------------

    /** Multiple scalar parameters inside {@code IN (?, ?)} filter as a tuple. */
    private void inClauseWithMultipleParams(boolean serverSide) throws Exception {
        try (Connection conn = connect(serverSide);
                PreparedStatement ps = conn.prepareStatement(
                        "with t as (select arrayJoin([1, 2, 3]) as a) "
                                + "select * from t where a in (?, ?) order by a")) {
            ps.setInt(1, 2);
            ps.setInt(2, 3);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getLong(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void inClauseWithMultipleParams_clientSide() throws Exception {
        inClauseWithMultipleParams(false);
    }

    @Test
    void inClauseWithMultipleParams_serverSideParams() throws Exception {
        inClauseWithMultipleParams(true);
    }

    /**
     * An array bound with setObject expands inside {@code IN (?)} (jdbc-v2 binds via
     * setArray, which this driver does not support — pinned in
     * ChPreparedStatementBindingTest). Client-side only (array bindings).
     */
    @Test
    void inClauseWithArrayBinding_clientSide() throws Exception {
        try (Connection conn = connect(false)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "select number from numbers(10) where number in (?) order by number")) {
                ps.setObject(1, new Long[] {2L, 4L, 6L});
                try (ResultSet rs = ps.executeQuery()) {
                    for (long expected : new long[] {2, 4, 6}) {
                        assertTrue(rs.next());
                        assertEquals(expected, rs.getLong(1));
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    /** Date predicates: {@code d IN (?)} with setDate and setObject(LocalDate). */
    private void dateInPredicate(boolean serverSide) throws Exception {
        String table = "pstmt_date_in_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id UInt32, d Date) ENGINE = Memory");
                st.execute("INSERT INTO " + table
                        + " VALUES (1, '2025-01-01'), (2, '2025-02-01'), (3, '2025-02-03')");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM " + table + " WHERE d IN (?) ORDER BY id")) {
                ps.setDate(1, Date.valueOf("2025-02-01"));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertFalse(rs.next());
                }
                ps.setObject(1, LocalDate.parse("2025-02-01"));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertFalse(rs.next());
                }
                if (!serverSide) {
                    // Array-of-dates expansion is client-side only (array bindings).
                    ps.setObject(1, new Date[] {Date.valueOf("2025-02-01"),
                            Date.valueOf("2025-02-03")});
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1));
                        assertFalse(rs.next());
                    }
                }
            }
        }
    }

    @Test
    void dateInPredicate_clientSide() throws Exception {
        dateInPredicate(false);
    }

    @Test
    void dateInPredicate_serverSideParams() throws Exception {
        dateInPredicate(true);
    }

    // ---- multi-row VALUES with function calls (jdbc-v2 testMetabaseBug01) --------

    /**
     * A user-written multi-row VALUES whose tuples contain function calls over
     * placeholders plus literal columns binds across tuples, both as a direct
     * executeUpdate and through the batch path (the INSERT ... SELECT batch half of
     * the reference is covered by ChJdbcIssuesTest#issue402_insertSelectWithParams).
     */
    private void multiRowValuesWithFunctionCalls(boolean serverSide) throws Exception {
        String table = "pstmt_metabase_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
                st.execute("CREATE TABLE " + table + " (id Int32, name Nullable(String),"
                        + " last_login Nullable(DateTime64(3)), password Nullable(String))"
                        + " ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (name, last_login, password, id) VALUES "
                            + "(?, parseDateTimeBestEffort(?, ?), ?, 1), "
                            + "(?, parseDateTimeBestEffort(?, ?), ?, 2)")) {
                ps.setObject(1, "Plato Yeshua");
                ps.setObject(2, "2014-04-01 08:30:00.000");
                ps.setObject(3, "UTC");
                ps.setObject(4, "4be68cda-6fd5-4ba7-944e-2b475600bda5");
                ps.setObject(5, "Felipinho Asklepios");
                ps.setObject(6, "2014-12-05 15:15:00.000");
                ps.setObject(7, "UTC");
                ps.setObject(8, "5bb19ad9-f3f8-421f-9750-7d398e38428d");
                ps.executeUpdate();
            }
            // Batch path: single tuple with a function call collapses (client) or
            // re-executes per row (server-side).
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (name, last_login, password, id) VALUES "
                            + "(?, parseDateTimeBestEffort(?, ?), ?, ?)")) {
                for (int i = 0; i < 3; i++) {
                    ps.setObject(1, "User " + i);
                    ps.setObject(2, "2014-04-01 08:30:00.000");
                    ps.setObject(3, "UTC");
                    ps.setObject(4, "password" + i);
                    ps.setObject(5, 10 + i);
                    ps.addBatch();
                }
                assertEquals(3, ps.executeBatch().length);
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT count(), sum(id), min(last_login) FROM " + table)) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertEquals(1 + 2 + 10 + 11 + 12, rs.getInt(2));
                assertEquals(LocalDateTime.of(2014, 4, 1, 8, 30)
                                .toInstant(ZoneOffset.UTC),
                        rs.getObject(3));
            }
        }
    }

    @Test
    void multiRowValuesWithFunctionCalls_clientSide() throws Exception {
        multiRowValuesWithFunctionCalls(false);
    }

    @Test
    void multiRowValuesWithFunctionCalls_serverSideParams() throws Exception {
        multiRowValuesWithFunctionCalls(true);
    }

    // ---- classification edges (jdbc-v2 testUnknownStatement) ---------------------

    /**
     * A trailing-comma SELECT still classifies as a query, and a prepared DDL
     * reports no result set from execute(). (jdbc-v2 additionally throws from
     * executeQuery() on a DDL; this driver defers that to the server and is not
     * pinned here.)
     */
    private void trailingCommaSelectAndDdlClassification(boolean serverSide) throws Exception {
        String table = "pstmt_unknown_stmt_" + mode(serverSide);
        try (Connection conn = connect(serverSide)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT number, FROM system.numbers LIMIT 3")) {
                assertTrue(ps.execute());
                try (ResultSet rs = ps.getResultSet()) {
                    for (int i = 0; i < 3; i++) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getLong(1));
                    }
                    assertFalse(rs.next());
                }
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + table + " (x Int32) ENGINE = Memory")) {
                assertFalse(ps.execute(), "DDL should not produce a ResultSet");
                assertNull(ps.getResultSet());
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @Test
    void trailingCommaSelectAndDdlClassification_clientSide() throws Exception {
        trailingCommaSelectAndDdlClassification(false);
    }

    @Test
    void trailingCommaSelectAndDdlClassification_serverSideParams() throws Exception {
        trailingCommaSelectAndDdlClassification(true);
    }

    // ---- ZonedDateTime binding (jdbc-v2 JDBCDateTimeTests#testTimestampInRange) ---

    private void createTimestampRangeTable(Connection conn, String table) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute("CREATE TABLE " + table + " (id UInt32, ts DateTime) ENGINE = Memory");
            st.execute("INSERT INTO " + table + " VALUES"
                    + " (1, '2025-01-01 08:00:00'), (2, '2025-01-01 12:00:00'),"
                    + " (3, '2025-01-01 18:00:00'), (4, '2025-01-02 00:00:00')");
        }
    }

    /**
     * The supported instant-typed BETWEEN predicate: {@code setObject(Instant)} binds
     * the UTC wall clock and reads come back as {@link Instant}.
     */
    @Test
    void timestampBetweenWithInstantBinding_clientSide() throws Exception {
        String table = "pstmt_ts_between_cli";
        ZoneId utc = ZoneId.of("UTC");
        try (Connection conn = connect(false)) {
            createTimestampRangeTable(conn, table);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, ts FROM " + table + " WHERE ts BETWEEN ? AND ? ORDER BY id")) {
                ps.setObject(1, ZonedDateTime.of(2025, 1, 1, 10, 0, 0, 0, utc).toInstant());
                ps.setObject(2, ZonedDateTime.of(2025, 1, 1, 20, 0, 0, 0, utc).toInstant());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertEquals(ZonedDateTime.of(2025, 1, 1, 12, 0, 0, 0, utc).toInstant(),
                            rs.getObject(2));
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    /**
     * KNOWN BUG — this test asserts the CORRECT behavior and fails until fixed.
     *
     * <p>Expected (jdbc-v2 {@code JDBCDateTimeTests#testTimestampInRange}):
     * {@code setObject(ZonedDateTime)} binds the instant the value identifies, so the
     * BETWEEN predicate matches rows 2 and 3, and
     * {@code getObject(..., ZonedDateTime.class)} reads a DateTime column back as a
     * ZonedDateTime. Actual: (a) {@code ChPreparedStatement.toLiteral} has no
     * ZonedDateTime branch, so the value renders via {@code toString()} with an
     * unparseable {@code [zone]} suffix and the query fails server-side; (b)
     * {@code ChResultSet.coerceTo} has no ZonedDateTime coercion, so the read throws.
     *
     * <p>HOW TO FIX: (a) add ZonedDateTime/OffsetDateTime branches to
     * {@code ChPreparedStatement.toLiteral} rendering the UTC wall clock (see the
     * companion unit test
     * {@code ChPreparedStatementBindingTest#knownBug_zonedAndOffsetDateTimeMustRenderUtcWallClockLiterals}
     * for the exact snippet), plus the mirror in {@code QueryParameters.toText};
     * (b) in {@code ChResultSet.coerceTo}
     * ({@code src/main/java/io/github/danielbunting/clickhouse/jdbc/ChResultSet.java}),
     * add {@code if (type == ZonedDateTime.class && v instanceof Instant i) return
     * i.atZone(ZoneOffset.UTC);} (and the analogous OffsetDateTime coercion).
     */
    @Test
    void knownBug_zonedDateTimeBindingAndTypedRead_clientSide() throws Exception {
        String table = "pstmt_zdt_cli";
        ZoneId utc = ZoneId.of("UTC");
        try (Connection conn = connect(false)) {
            createTimestampRangeTable(conn, table);
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, ts FROM " + table + " WHERE ts BETWEEN ? AND ? ORDER BY id")) {
                ps.setObject(1, ZonedDateTime.of(2025, 1, 1, 10, 0, 0, 0, utc));
                ps.setObject(2, ZonedDateTime.of(2025, 1, 1, 20, 0, 0, 0, utc));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertEquals(
                            ZonedDateTime.of(2025, 1, 1, 12, 0, 0, 0, utc).toInstant(),
                            rs.getObject(2, ZonedDateTime.class).toInstant());
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }
}
