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
}
