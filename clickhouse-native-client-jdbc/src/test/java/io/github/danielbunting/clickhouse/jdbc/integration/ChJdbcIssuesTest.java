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
     * clickhouse-java#2723 — getString / getArray on (nested) array columns. Previously
     * getString on an array NPE'd. also: ChResultSetComplexTypeTest.
     */
    @Test
    void issue2723_getStringAndGetArrayOnArrays() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT [[1, 2, 3], [4, 5, 6]] AS nested")) {
                assertTrue(rs.next());
                assertEquals("[[1, 2, 3], [4, 5, 6]]", rs.getString("nested"));
            }
            try (ResultSet rs = st.executeQuery(
                    "SELECT splitByChar('_', 'field1_field2_field3') AS parts")) {
                assertTrue(rs.next());
                assertEquals("['field1', 'field2', 'field3']", rs.getString("parts"));
                Array arr = rs.getArray(1);
                assertArrayEquals(new Object[] {"field1", "field2", "field3"}, (Object[]) arr.getArray());
            }
        }
    }

    /**
     * clickhouse-java#2657 — Array(Map(LowCardinality(String), String)) with interleaved
     * empty maps must decode element-by-element.
     */
    @Test
    void issue2657_arrayOfMapsWithEmptyMaps() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS issue_2657");
            st.execute("CREATE TABLE issue_2657 (traits Array(Map(LowCardinality(String), String))) "
                    + "ENGINE = Memory");
            st.execute("INSERT INTO issue_2657 VALUES ([map(), map('k1','v1','k2','v2'), map(), map()])");

            try (ResultSet rs = st.executeQuery("SELECT traits FROM issue_2657")) {
                assertTrue(rs.next());
                Array arr = rs.getArray(1);
                Object[] maps = (Object[]) arr.getArray();
                assertEquals(4, maps.length);
                assertTrue(((Map<?, ?>) maps[0]).isEmpty());
                assertEquals(Map.of("k1", "v1", "k2", "v2"), maps[1]);
                assertTrue(((Map<?, ?>) maps[2]).isEmpty());
                assertTrue(((Map<?, ?>) maps[3]).isEmpty());
            }
            st.execute("DROP TABLE IF EXISTS issue_2657");
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

    /** clickhouse-java#2329 — write an Array(String) via setObject(collection). also: ChPreparedStatementTest. */
    @Test
    void issue2329_writeArrayViaSetObject() throws Exception {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS issue_2329");
                st.execute("CREATE TABLE issue_2329 (id String, arr Array(String)) ENGINE = Memory");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO issue_2329 (id, arr) VALUES (?, ?)")) {
                ps.setString(1, "id1");
                ps.setObject(2, List.of("a", "b", "c"));
                ps.executeUpdate();
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT arr FROM issue_2329")) {
                assertTrue(rs.next());
                assertArrayEquals(new Object[] {"a", "b", "c"}, (Object[]) rs.getArray(1).getArray());
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

    /** Ported from PreparedStatementTest batch-reuse: a failed batch can be cleared and reused. */
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

                // Reuse the same statement with valid rows.
                List<Integer> ids = new ArrayList<>();
                for (int i = 1; i <= 4; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i * 10);
                    ps.addBatch();
                    ids.add(i);
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
