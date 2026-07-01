package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.net.InetAddress;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Central regression suite porting the concrete bugs the official ClickHouse JDBC driver hit
 * (its {@code JdbcIssuesTest} plus issue-tagged cases across {@code PreparedStatementTest} /
 * {@code JdbcDataTypeTests}), proving our driver handles the same scenarios. The unit-testable
 * slice of most issues also lives in the relevant unit test (see the {@code // also:} notes).
 */
@Tag("integration")
@Testcontainers
class ChJdbcIssuesTest {

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

    private static Connection connect() throws SQLException {
        String url = "jdbc:chnative://" + CLICKHOUSE.getHost() + ":"
                + CLICKHOUSE.getMappedPort(NATIVE_PORT) + "/default";
        return DriverManager.getConnection(url);
    }

    /**
     * clickhouse-java#2723 — getString / getArray on (nested) array columns. Faithful port of
     * {@code JdbcDataTypeTests.testNestedArrayToString}: getString previously NPE'd on an array,
     * including inside a CASE/WHEN and for deeply-nested arrays. also: ChResultSetComplexTypeTest.
     *
     * <p>Divergence: the official Test 3 casts {@code getArray()} to {@code String[][][]}; our
     * {@link ChArray} materialises nested {@code Object[]} (not typed arrays), so the deep-array
     * assertion below compares the {@code Object[]} structure instead.
     */
    @Test
    void issue2723_getStringAndGetArrayOnArrays() throws Exception {
        // Test 1: getString on Array(Array(Int32)) — NPE'd before the fix.
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT [[1, 2, 3], [4, 5, 6]] AS nested_array")) {
            assertTrue(rs.next());
            assertEquals("[[1, 2, 3], [4, 5, 6]]", rs.getString("nested_array"));
        }

        // Test 2: splitByChar array + array getString inside a CASE/WHEN.
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            String query = "SELECT "
                    + "splitByChar('_', 'field1_field2_field3') as split_result, "
                    + "CASE "
                    + "    WHEN "
                    + "         splitByChar('_', 'field1_field2_field3')[1] IN ('field1', 'field2') "
                    + "         AND match( "
                    + "             splitByChar('_', 'field1_field2_field3')[2], "
                    + "             '(field1|field2|field3)' "
                    + "         ) "
                    + "        THEN 'Matched' "
                    + "    ELSE 'NotMatched' "
                    + "END AS action_to_do";
            try (ResultSet rs = st.executeQuery(query)) {
                assertTrue(rs.next());
                assertEquals("['field1', 'field2', 'field3']", rs.getString("split_result"));
                assertEquals("Matched", rs.getString("action_to_do"));
            }
        }

        // Test 3: deeply nested Array(Array(Array(String))) — getString + getArray structure.
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT [[['a', 'b'], ['c']], [['d', 'e', 'f']]] as deep_nested")) {
            assertTrue(rs.next());
            assertEquals("[[['a', 'b'], ['c']], [['d', 'e', 'f']]]", rs.getString("deep_nested"));
            Object[] deep = (Object[]) rs.getArray(1).getArray();
            // [ [['a','b'],['c']], [['d','e','f']] ]
            assertEquals(2, deep.length);
            Object[] g0 = (Object[]) deep[0];
            assertArrayEquals(new Object[] {"a", "b"}, (Object[]) g0[0]);
            assertArrayEquals(new Object[] {"c"}, (Object[]) g0[1]);
            Object[] g1 = (Object[]) deep[1];
            assertArrayEquals(new Object[] {"d", "e", "f"}, (Object[]) g1[0]);
        }
    }

    /**
     * clickhouse-java#2657 — Array(Map(LowCardinality(String), String)) with interleaved empty
     * maps must decode element-by-element. Faithful port of
     * {@code JdbcDataTypeTests.testArrayOfMapsWithLowCardinalityAndEmptyMaps}, including the
     * 8-map layout, {@code getBaseTypeName} check, and the 10-iteration loop (the original bug
     * was intermittent).
     */
    @Test
    void issue2657_arrayOfMapsWithEmptyMaps() throws Exception {
        java.util.Map<String, String> expectedNonEmptyMap = new java.util.HashMap<>();
        for (int i = 1; i <= 8; i++) {
            expectedNonEmptyMap.put("RandomKey" + i, "Value" + i);
        }
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS issue_2657");
            st.execute("CREATE TABLE issue_2657 (traits Array(Map(LowCardinality(String), String))) "
                    + "ENGINE = MergeTree ORDER BY tuple()");
            st.executeUpdate("INSERT INTO issue_2657 (traits) VALUES (["
                    + "  map(), "
                    + "  map('RandomKey1','Value1','RandomKey2','Value2','RandomKey3','Value3',"
                    + "      'RandomKey4','Value4','RandomKey5','Value5','RandomKey6','Value6',"
                    + "      'RandomKey7','Value7','RandomKey8','Value8'), "
                    + "  map(), map(), map(), map(), map(), map()"
                    + "])");

            // Run multiple iterations because the original decode bug was intermittent.
            for (int attempt = 0; attempt < 10; attempt++) {
                try (ResultSet rs = st.executeQuery("SELECT traits FROM issue_2657")) {
                    assertTrue(rs.next(), "Expected a row on attempt " + attempt);
                    Array traitsArray = rs.getArray(1);
                    assertEquals("Map(LowCardinality(String), String)", traitsArray.getBaseTypeName());

                    Object[] maps = (Object[]) traitsArray.getArray();
                    assertEquals(8, maps.length, "Expected 8 maps on attempt " + attempt);
                    assertTrue(((Map<?, ?>) maps[0]).isEmpty(), "First map empty on attempt " + attempt);
                    assertEquals(expectedNonEmptyMap, maps[1], "Second map on attempt " + attempt);
                    for (int i = 2; i < 8; i++) {
                        assertTrue(((Map<?, ?>) maps[i]).isEmpty(),
                                "Map " + i + " empty on attempt " + attempt);
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    /** clickhouse-java#2299 — clearParameters keeps the parameter count. also: ChParameterMetaDataTest. */
    @Test
    void issue2299_clearParametersThenReuse() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_2299");
                st.execute("CREATE TABLE issue_2299 (id Nullable(String), name Nullable(String), age Int32) "
                        + "ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_2299 (id, name, age) VALUES (?, ?, ?)")) {
                assertEquals(3, ps.getParameterMetaData().getParameterCount());
                ps.setString(1, "id1");
                ps.setString(2, "n1");
                ps.setInt(3, 18);
                ps.executeUpdate();
                ps.clearParameters();
                assertEquals(3, ps.getParameterMetaData().getParameterCount());
                ps.setString(1, "id2");
                ps.setString(2, "n2");
                ps.setInt(3, 19);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count() FROM issue_2299")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    /** clickhouse-java#2327 — write a UUID and match it via IN (CAST(? AS UUID)). also: ChPreparedStatementTest. */
    @Test
    void issue2327_uuidWriteAndInClause() throws Exception {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_2327");
                st.execute("CREATE TABLE issue_2327 (id Nullable(String), uuid UUID) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_2327 (id, uuid) VALUES (?, ?)")) {
                ps.setString(1, "id1");
                ps.setObject(2, uuid);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, uuid FROM issue_2327 WHERE uuid IN (CAST(? AS UUID))")) {
                ps.setString(1, uuid.toString());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("id1", rs.getString(1));
                    assertEquals(uuid, rs.getObject(2));
                }
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_2327");
            }
        }
    }

    /**
     * clickhouse-java#2329 — write an Array(String) via {@code setObject(collection)}. The
     * official repro binds an <em>empty</em> collection and checks the row inserts; we also
     * assert a non-empty collection round-trips through {@code getArray}. also: ChPreparedStatementTest.
     */
    @Test
    void issue2329_writeArrayViaSetObject() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_2329");
                st.execute("CREATE TABLE issue_2329 (id String, arr Array(String)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_2329 (id, arr) VALUES (?, ?)")) {
                // Official repro: an empty Collection must be accepted.
                ps.setString(1, "empty");
                ps.setObject(2, new java.util.ArrayList<String>());
                ps.executeUpdate();
                // Non-empty round-trip (stronger than the original assertion).
                ps.setString(1, "full");
                ps.setObject(2, List.of("a", "b", "c"));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT id, arr FROM issue_2329 ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("empty", rs.getString(1));
                assertEquals(0, ((Object[]) rs.getArray(2).getArray()).length);
                assertTrue(rs.next());
                assertEquals("full", rs.getString(1));
                assertArrayEquals(new Object[] {"a", "b", "c"}, (Object[]) rs.getArray(2).getArray());
            }
        }
    }

    /** clickhouse-java#1373 — a null bound mid-batch must not throw. also: ChPreparedStatementTest. */
    @Test
    void issue1373_nullInBatch() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_1373");
                st.execute("CREATE TABLE issue_1373 (a String, b Int8, c Nullable(String)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_1373 (a, b, c) VALUES (?, ?, ?)")) {
                for (int i = 1; i <= 5; i++) {
                    ps.setString(1, "row");
                    ps.setInt(2, 10);
                    ps.setString(3, i == 2 ? null : "val");
                    ps.addBatch();
                }
                ps.executeBatch(); // must not throw
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count() FROM issue_1373")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    /** clickhouse-java#612 — UUID + DateTime64(6) round-trip preserving microseconds. */
    @Test
    void issue612_uuidAndDateTime64() throws Exception {
        UUID uuid = UUID.randomUUID();
        Timestamp ts = Timestamp.valueOf("2026-05-30 13:45:07.123456");
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_612");
                st.execute("CREATE TABLE issue_612 (id UUID, ts DateTime64(6)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_612 (id, ts) VALUES (?, ?)")) {
                ps.setObject(1, uuid);
                ps.setTimestamp(2, ts);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, ts FROM issue_612 WHERE id = ?")) {
                ps.setObject(1, uuid);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(uuid, rs.getObject(1));
                    // The microsecond fraction survives the round trip.
                    assertEquals(123_456_000, rs.getTimestamp(2).getNanos());
                }
            }
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_612");
            }
        }
    }

    /** clickhouse-java#315 — IPv6 write/read via a prepared statement. */
    @Test
    void issue315_ipv6RoundTrip() throws Exception {
        InetAddress addr = InetAddress.getByName("2001:db8::1");
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_315");
                st.execute("CREATE TABLE issue_315 (id Int32, addr IPv6) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_315 (id, addr) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, addr);
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT addr FROM issue_315")) {
                assertTrue(rs.next());
                assertEquals(addr, rs.getObject(1));
            }
        }
    }

    /** clickhouse-java#402 — INSERT ... SELECT with positional parameters (not a VALUES list). */
    @Test
    void issue402_insertSelectWithParams() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_402");
                st.execute("CREATE TABLE issue_402 (uid Int32, name String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO issue_402 SELECT ?, ?")) {
                ps.setInt(1, 7);
                ps.setString(2, "seven");
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT uid, name FROM issue_402")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
                assertEquals("seven", rs.getString(2));
            }
        }
    }

    /**
     * Ported from the official JdbcIssuesTest "decompress" cases: batching increasingly large
     * string payloads through a prepared INSERT must not fail.
     */
    @Test
    void decompress_largeStringBatchInsert() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_decompress");
                st.execute("CREATE TABLE issue_decompress (event_id String) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_decompress (event_id) VALUES (?)")) {
                for (int size = 1; size <= 100_000; size *= 10) {
                    ps.setString(1, "*".repeat(size));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(
                            "SELECT max(length(event_id)) FROM issue_decompress")) {
                assertTrue(rs.next());
                assertEquals(100_000, rs.getInt(1));
            }
        }
    }

    /**
     * Ported from PreparedStatementTest batch-reuse: a failed batch can be cleared and reused.
     *
     * <p><b>Intentional divergence from clickhouse-java:</b> the official
     * {@code testBatchInsertValuesReuse} asserts a failed {@code executeBatch} <em>persists</em>
     * the bad rows, so a second {@code executeBatch} (before {@code clearBatch}) throws again.
     * Our driver clears the accumulated batch in a {@code finally} on failure, so it does not
     * throw a second time — this test therefore verifies fail-then-recover with a single throw.
     */
    @Test
    void batchReuseAfterFailure() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_batch_reuse");
                st.execute("CREATE TABLE issue_batch_reuse (v1 Int32, v2 Int32) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_batch_reuse (v1, v2) VALUES (?, ?)")) {
                // A non-numeric value for an Int32 column fails the batch.
                ps.setString(1, "invalid");
                ps.setInt(2, 1);
                ps.addBatch();
                assertThrows(SQLException.class, ps::executeBatch);
                // Divergence from clickhouse-java: our driver already cleared the failed batch
                // (finally), so a second executeBatch() is an empty no-op rather than a repeat
                // failure — no clearBatch() is required before reuse.
                assertEquals(0, ps.executeBatch().length);

                // Reuse the same statement with valid rows.
                for (int i = 1; i <= 4; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i * 10);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                assertEquals(4, counts.length);
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT count() FROM issue_batch_reuse")) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
            }
        }
    }

    @Test
    void driverSmoke() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1")) {
            assertNotNull(conn);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }
}
