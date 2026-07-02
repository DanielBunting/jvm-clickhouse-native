package io.github.danielbunting.clickhouse.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end JDBC coverage of the {@link ResultSet} + {@link ResultSetMetaData} read path against
 * a real server — the module previously had only a {@code SELECT 1} smoke test. Exercises typed
 * getters by index and label, {@code wasNull} for a SQL NULL, {@code getObject}, result-set
 * exhaustion, out-of-range access, and live column metadata.
 */
@Tag("integration")
@Testcontainers
class JdbcResultSetIT {

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

    @Test
    void typedGetters_wasNull_metadata_andExhaustion() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_rs");
            st.execute("CREATE TABLE jdbc_rs (id UInt32, name String, score Float64, "
                    + "maybe Nullable(Int32)) ENGINE = Memory");
            st.execute("INSERT INTO jdbc_rs VALUES (1, 'alice', 1.5, 42), (2, 'bob', 2.5, NULL)");

            try (ResultSet rs = st.executeQuery(
                    "SELECT id, name, score, maybe FROM jdbc_rs ORDER BY id")) {

                // Column metadata off the live result.
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(4, md.getColumnCount());
                assertEquals("id", md.getColumnName(1));
                assertEquals(Types.VARCHAR, md.getColumnType(2), "String -> VARCHAR");
                assertEquals(ResultSetMetaData.columnNullable, md.isNullable(4),
                        "Nullable(Int32) reported nullable");

                // Row 1 — typed getters by index and by label, non-null maybe.
                assertTrue(rs.next(), "first row present");
                assertEquals(1, rs.getInt("id"));
                assertEquals("alice", rs.getString(2));
                assertEquals(1.5, rs.getDouble("score"), 0.0);
                assertEquals(42, rs.getInt("maybe"));
                assertFalse(rs.wasNull(), "maybe is 42, not NULL");
                assertEquals(1L, ((Number) rs.getObject(1)).longValue(), "getObject on id");

                // Row 2 — NULL in the nullable column drives wasNull().
                assertTrue(rs.next(), "second row present");
                assertEquals("bob", rs.getString("name"));
                int maybe = rs.getInt("maybe");
                assertTrue(rs.wasNull(), "maybe is SQL NULL on row 2");
                assertEquals(0, maybe, "getInt returns 0 for SQL NULL");

                // Out-of-range column access surfaces a SQLException (on a valid current row).
                assertThrows(SQLException.class, () -> rs.getString(99));

                // Exhaustion.
                assertFalse(rs.next(), "exactly two rows");
            }
            st.execute("DROP TABLE IF EXISTS jdbc_rs");
        }
    }

    /**
     * Deeply-nested map values (reference: jdbc-v1 ClickHouseResultSetTest#testMap):
     * {@code Map(String, Array(Nullable(DateTime64(3[, tz]))))} read back via
     * {@code getObject}. This driver boxes Map as {@link java.util.Map}, Array as
     * {@link java.util.List} (with interior nulls preserved) and DateTime64 as an
     * absolute {@link Instant} — the column timezone only shifts how the server
     * interprets the inserted wall-clock literal (the reference instead yields
     * LocalDateTime/OffsetDateTime element types).
     */
    @Test
    void mapOfArrayOfNullableDateTime64_getObject() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_rs_map_deep");
            st.execute("CREATE TABLE jdbc_rs_map_deep (id Int8, "
                    + "m0 Map(String, Array(Nullable(DateTime64(3)))), "
                    + "m1 Map(String, Array(Nullable(DateTime64(3, 'Asia/Shanghai'))))) "
                    + "ENGINE = Memory");
            st.execute("INSERT INTO jdbc_rs_map_deep VALUES (1, "
                    + "{'a': [], 'b': ['2022-03-30 00:00:00.123', NULL]}, "
                    + "{'a': [], 'b': ['2022-03-30 00:00:00.123', NULL]})");

            try (ResultSet rs = st.executeQuery("SELECT * FROM jdbc_rs_map_deep ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));

                // m0: the literal was interpreted in the server timezone (UTC here).
                Map<?, ?> m0 = (Map<?, ?>) rs.getObject(2);
                assertEquals(2, m0.size());
                assertEquals(List.of(), m0.get("a"));
                assertEquals(Arrays.asList(Instant.parse("2022-03-30T00:00:00.123Z"), null),
                        m0.get("b"));

                // m1: the same wall-clock literal in Asia/Shanghai (+08:00) is an
                // earlier absolute instant; the boxed Instant is timezone-agnostic.
                Map<?, ?> m1 = (Map<?, ?>) rs.getObject(3);
                assertEquals(2, m1.size());
                assertEquals(List.of(), m1.get("a"));
                assertEquals(Arrays.asList(Instant.parse("2022-03-29T16:00:00.123Z"), null),
                        m1.get("b"));

                assertFalse(rs.next());
            }
            st.execute("DROP TABLE IF EXISTS jdbc_rs_map_deep");
        }
    }

    /**
     * Mixed-type tuples including nested array/map elements (reference: jdbc-v1
     * ClickHouseResultSetTest#testTuple). A Tuple boxes as a {@link java.util.List}
     * whose elements carry each member's own boxing (Int16 as Short, UInt8 as Integer,
     * UInt32 as Long, nested Array as List, nested Map as Map).
     */
    @Test
    void mixedNestedTuple_getObject() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT (toInt16(1), 'a', toFloat32(1.2), "
                    + "CAST([1, 2] AS Array(Nullable(UInt8))), map(toUInt32(1), 'a')) v")) {
                assertTrue(rs.next());
                List<?> v = (List<?>) rs.getObject(1);
                assertEquals(5, v.size());
                assertEquals((short) 1, v.get(0), "Int16 element boxes as Short");
                assertEquals("a", v.get(1));
                assertEquals(1.2f, v.get(2), "Float32 element boxes as Float");
                assertEquals(Arrays.asList(1, 2), v.get(3),
                        "Array(Nullable(UInt8)) element boxes as List<Integer>");
                assertEquals(Map.of(1L, "a"), v.get(4),
                        "Map(UInt32, String) element boxes as Map<Long, String>");
                assertFalse(rs.next());
            }

            try (ResultSet rs = st.executeQuery(
                    "SELECT CAST(tuple(1, [2, 3], ('4', [5, 6]), map('seven', 8)) AS "
                    + "Tuple(Int16, Array(Nullable(Int16)), Tuple(String, Array(Int32)), "
                    + "Map(String, Int32))) v")) {
                assertTrue(rs.next());
                List<?> v = (List<?>) rs.getObject(1);
                assertEquals(4, v.size());
                assertEquals((short) 1, v.get(0));
                assertEquals(Arrays.asList((short) 2, (short) 3), v.get(1));
                List<?> inner = (List<?>) v.get(2);
                assertEquals("4", inner.get(0));
                assertEquals(Arrays.asList(5, 6), inner.get(1));
                assertEquals(Map.of("seven", 8), v.get(3));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Nested inside SimpleAggregateFunction read over JDBC (reference: jdbc-v1
     * ClickHouseResultSetTest#testNested, which exposes the column as a Map keyed by
     * the Nested member names). DEVIATION: this driver has no Map view — with
     * {@code flatten_nested = 0} a {@code Nested(a String, b String)} column travels
     * as Array(Tuple(...)) and {@code getObject} returns the raw List-of-tuples.
     */
    @Test
    void nestedInsideSimpleAggregateFunction_getObject() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("SET flatten_nested = 0");
            st.execute("DROP TABLE IF EXISTS jdbc_rs_saf_nested");
            st.execute("CREATE TABLE jdbc_rs_saf_nested (id Int8, "
                    + "n0 SimpleAggregateFunction(anyLast, Nested(a String, b String))) "
                    + "ENGINE = AggregatingMergeTree() ORDER BY (id)");
            st.execute("INSERT INTO jdbc_rs_saf_nested VALUES "
                    + "(1, [tuple('foo1', 'bar1'), tuple('foo11', 'bar11')]), "
                    + "(2, [tuple('foo2', 'bar2'), tuple('foo22', 'bar22')])");

            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM jdbc_rs_saf_nested ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                List<?> rows = (List<?>) rs.getObject(2);
                assertEquals(2, rows.size());
                assertEquals(List.of("foo1", "bar1"), rows.get(0));
                assertEquals(List.of("foo11", "bar11"), rows.get(1));

                assertTrue(rs.next());
                rows = (List<?>) rs.getObject(2);
                assertEquals(List.of("foo2", "bar2"), rows.get(0));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE IF EXISTS jdbc_rs_saf_nested");
        }
    }

    /**
     * Min/max/random temporal fidelity through the JDBC getters (reference: jdbc-v2
     * DetachedResultSetTest#testDateTimeTypes). Covers Date, Date32, DateTime and
     * DateTime64(3/6/9) at their range edges plus a sub-second value; string literals
     * are interpreted in the server timezone (UTC in this container), and the getters
     * return {@code Date.valueOf(localDate)} / {@code Timestamp.from(instant)}.
     * Calendar-aware overloads are N/A here (they delegate and ignore the Calendar —
     * see ChResultSetTemporalTest#calendarOverloadsDelegate).
     */
    @Test
    void dateTimeTypesMinMaxFidelity() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_rs_dates");
            st.execute("CREATE TABLE jdbc_rs_dates (ord Int8, d Date, d32 Date32, "
                    + "dt DateTime, dt643 DateTime64(3), dt646 DateTime64(6), "
                    + "dt649 DateTime64(9)) ENGINE = Memory");
            st.execute("INSERT INTO jdbc_rs_dates VALUES "
                    + "(1, '1970-01-01', '1970-01-01', '1970-01-01 00:00:00', "
                    + "'1970-01-01 00:00:00.000', '1970-01-01 00:00:00.000000', "
                    + "'1970-01-01 00:00:00.000000000'), "
                    + "(2, '2149-06-06', '2299-12-31', '2106-02-07 06:28:15', "
                    + "'2261-12-31 23:59:59.999', '2261-12-31 23:59:59.999999', "
                    + "'2261-12-31 23:59:59.999999999'), "
                    + "(3, '2024-05-30', '2024-05-30', '2024-05-30 13:45:07', "
                    + "'2024-05-30 13:45:07.333', '2024-05-30 13:45:07.333333', "
                    + "'2024-05-30 13:45:07.333333333')");

            try (ResultSet rs = st.executeQuery("SELECT * FROM jdbc_rs_dates ORDER BY ord")) {
                // Row 1: epoch minimums.
                assertTrue(rs.next());
                assertEquals(LocalDate.parse("1970-01-01"), rs.getDate("d").toLocalDate());
                assertEquals(LocalDate.parse("1970-01-01"), rs.getDate("d32").toLocalDate());
                assertEquals(Instant.EPOCH, rs.getTimestamp("dt").toInstant());
                assertEquals(Instant.EPOCH, rs.getTimestamp("dt643").toInstant());
                assertEquals(Instant.EPOCH, rs.getTimestamp("dt646").toInstant());
                assertEquals(Instant.EPOCH, rs.getTimestamp("dt649").toInstant());

                // Row 2: type maximums, sub-second precision preserved exactly.
                assertTrue(rs.next());
                assertEquals(LocalDate.parse("2149-06-06"), rs.getDate("d").toLocalDate());
                assertEquals(LocalDate.parse("2299-12-31"), rs.getDate("d32").toLocalDate());
                assertEquals(Instant.parse("2106-02-07T06:28:15Z"),
                        rs.getTimestamp("dt").toInstant());
                assertEquals(Instant.parse("2261-12-31T23:59:59.999Z"),
                        rs.getTimestamp("dt643").toInstant());
                assertEquals(Instant.parse("2261-12-31T23:59:59.999999Z"),
                        rs.getTimestamp("dt646").toInstant());
                assertEquals(Instant.parse("2261-12-31T23:59:59.999999999Z"),
                        rs.getTimestamp("dt649").toInstant());

                // Row 3: a mid-range value; getObject boxing and getString rendering.
                assertTrue(rs.next());
                assertEquals(LocalDate.parse("2024-05-30"), rs.getObject("d"),
                        "Date boxes as LocalDate");
                assertEquals(Instant.parse("2024-05-30T13:45:07.333333333Z"),
                        rs.getObject("dt649"), "DateTime64 boxes as Instant");
                assertEquals("2024-05-30", rs.getString("d"));
                assertEquals("2024-05-30T13:45:07.333Z", rs.getString("dt643"),
                        "getString renders the Instant's ISO form");

                assertFalse(rs.next());
            }
            st.execute("DROP TABLE IF EXISTS jdbc_rs_dates");
        }
    }
}
