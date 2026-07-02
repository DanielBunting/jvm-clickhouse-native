package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
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
     * "Cannot parse quoted string: expected opening quote ''', got 'N'" — a known
     * limitation of the all-String server-side rewrite.
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
}
