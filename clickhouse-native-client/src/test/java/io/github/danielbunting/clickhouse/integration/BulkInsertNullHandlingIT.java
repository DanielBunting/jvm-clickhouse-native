package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.mapping.RowMapper;
import io.github.danielbunting.clickhouse.mapping.RowMapperFactory;
import io.github.danielbunting.clickhouse.test.BlockMaterializer;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Null handling on the bulk-insert fallback (Object-scratch) path, driven through a
 * {@link RowMapperFactory} like the ADBC ingest bridge: a null feeds the parallel null-map for
 * {@code Nullable(T)}, is stored INSIDE the codec for {@code LowCardinality(Nullable(T))}
 * (whose dictionary reserves the NULL placeholder slot — the guard used to reject this case
 * as "non-nullable"), and fails clearly for a genuinely non-nullable column.
 */
@Tag("integration")
class BulkInsertNullHandlingIT extends IntegrationTestBase {

    private static ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    /** A write-only mapper binding each row value straight into the single column. */
    private static RowMapperFactory<Object> singleColumnMapper() {
        return names -> new RowMapper<Object>() {
            @Override
            public String[] columnNames() {
                return names;
            }

            @Override
            public Object map(Object[] columnValues) {
                throw new UnsupportedOperationException("write-only");
            }

            @Override
            public void bind(Object value, Object[] dest) {
                dest[0] = value == NULL_ROW ? null : value;
            }
        };
    }

    /** Sentinel because {@code BulkInserter.add} rejects null ROW objects (not values). */
    private static final Object NULL_ROW = new Object();

    private static List<Object> ingestAndReadBack(String columnType, Object... rows) {
        String table = "bulk_nulls_" + System.nanoTime();
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            try {
                conn.execute("CREATE TABLE " + table + " (v " + columnType + ")"
                        + " ENGINE = MergeTree ORDER BY tuple()");
                try (BulkInserter<Object> inserter = conn.createBulkInserter(
                        table, Object.class, singleColumnMapper())) {
                    inserter.init();
                    for (Object row : rows) {
                        inserter.add(row);
                    }
                    inserter.complete();
                }
                try (QueryResult result = conn.query("SELECT v FROM " + table)) {
                    List<Object> values = new ArrayList<>();
                    for (Object[] row : BlockMaterializer.materialize(result)) {
                        values.add(row[0]);
                    }
                    return values;
                }
            } finally {
                conn.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @Test
    void nullableColumnRoutesNullsThroughTheNullMap() {
        assertEquals(Arrays.asList("a", null, "b"),
                ingestAndReadBack("Nullable(String)", "a", NULL_ROW, "b"));
    }

    @Test
    void lowCardinalityNullableStoresNullsInsideTheCodec() {
        // LowCardinality(Nullable(T)) has no parallel null-map; before the guard fix this
        // was wrongly rejected as "Cannot insert null into non-nullable column".
        assertEquals(Arrays.asList("x", null, "x"),
                ingestAndReadBack("LowCardinality(Nullable(String))", "x", NULL_ROW, "x"));
    }

    @Test
    void nonNullableColumnRejectsNullClearly() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ingestAndReadBack("String", "ok", NULL_ROW));
        assertTrue(ex.getMessage().contains("non-nullable column 'v'"), ex.getMessage());
        assertNull(ex.getCause(), "a clear rejection, not a wrapped codec NPE");
    }
}
