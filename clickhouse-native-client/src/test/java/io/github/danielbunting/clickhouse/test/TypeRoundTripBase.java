package io.github.danielbunting.clickhouse.test;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Shared base for type round-trip integration tests.
 *
 * <p>Centralises the boilerplate that every type IT needs — building a
 * {@link ClickHouseConfig} pointed at the shared container, materialising a
 * {@link QueryResult} into row-major boxed values, and managing a
 * unique-per-test table with guaranteed cleanup — so individual test classes
 * contain only the per-type DDL, literals/records, and assertions.
 *
 * <p>Two round-trip directions are exercised by subclasses on top of these
 * helpers:
 * <ul>
 *   <li><b>DECODE</b>: raw {@code INSERT ... VALUES (literals)} (server encodes)
 *       then {@link #decode(ClickHouseConnection, String)} reads back and the
 *       client decodes — the authoritative path, available for every type.</li>
 *   <li><b>ENCODE</b>: {@code createBulkInserter} writes a mapped record (client
 *       encodes), then read back via the same decode path.</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
public abstract class TypeRoundTripBase extends IntegrationTestBase {

    /**
     * Builds a default config pointing at the shared test container.
     *
     * @return a fresh {@link ClickHouseConfig}
     */
    protected ClickHouseConfig config() {
        return ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .build();
    }

    /**
     * Reads all blocks from a {@link QueryResult} and materialises every
     * {@code (column, row)} value into a row-major {@code List<Object[]>} using
     * the null-aware {@link Column#value(int)} accessor (a {@code null} entry
     * means the cell is SQL {@code NULL}).
     *
     * @param result the query result (fully iterated inside this call)
     * @return list of rows, each a {@code Object[]} of boxed, null-aware values
     */
    protected List<Object[]> materialize(QueryResult result) {
        List<Object[]> rows = new ArrayList<>();
        Iterator<Block> blocks = result.blocks();
        while (blocks.hasNext()) {
            Block block = blocks.next();
            if (block.isEmpty()) {
                continue;
            }
            int colCount = block.columnCount();
            int rowCount = block.rowCount();
            for (int r = 0; r < rowCount; r++) {
                Object[] row = new Object[colCount];
                for (int c = 0; c < colCount; c++) {
                    Column col = block.column(c);
                    boolean isNull = col.nulls() != null && col.nulls()[r];
                    row[c] = isNull ? null : col.value(r);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /**
     * Runs {@code selectSql} and materialises the full result.
     *
     * @param conn      an open connection
     * @param selectSql the SELECT statement
     * @return all rows as {@code Object[]} of boxed, null-aware values
     */
    protected List<Object[]> decode(ClickHouseConnection conn, String selectSql) {
        try (QueryResult result = conn.query(selectSql)) {
            return materialize(result);
        }
    }

    /**
     * Opens a connection, hands a unique table name to {@code body}, and always
     * {@code DROP}s that table afterwards.
     *
     * <p>The body is responsible for the {@code CREATE TABLE <name> (...)},
     * inserts, queries, and assertions. The table name is
     * {@code prefix_<nanoTime>} so concurrent test classes never collide.
     *
     * @param prefix table-name prefix (kept short and identifier-safe)
     * @param body   the test body, receiving the open connection and table name
     */
    protected void withTable(String prefix, BiConsumer<ClickHouseConnection, String> body) {
        String table = prefix + "_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            try {
                body.accept(conn, table);
            } finally {
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    /**
     * Returns the connected server's {@code [major, minor]} version, parsed from
     * {@code SELECT version()} (e.g. {@code "26.4.1.2087"} → {@code [26, 4]}).
     *
     * <p>Opens its own short-lived connection, so it is safe to call outside an
     * existing {@link #withTable} body.
     *
     * @return a two-element array of the major and minor version numbers
     */
    protected int[] serverVersion() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            String version = String.valueOf(decode(conn, "SELECT version()").get(0)[0]);
            String[] parts = version.split("\\.");
            return new int[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
        }
    }

    /**
     * Reports whether the connected server is at least {@code major.minor}. Used
     * to gate assertions on behaviour that changed across the supported version
     * matrix (see {@link ClickHouseImages#SUPPORTED_VERSIONS}).
     *
     * @param major required major version
     * @param minor required minor version
     * @return {@code true} if the server version is {@code >= major.minor}
     */
    protected boolean serverVersionAtLeast(int major, int minor) {
        int[] v = serverVersion();
        return v[0] > major || (v[0] == major && v[1] >= minor);
    }
}
