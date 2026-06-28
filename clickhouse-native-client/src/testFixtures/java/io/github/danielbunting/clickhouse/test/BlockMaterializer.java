package io.github.danielbunting.clickhouse.test;

import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Shared test helper: drains a {@link QueryResult} into a null-aware {@code List<Object[]>}, one
 * array per row of boxed column values. Promoted to test fixtures so every module (the core's own
 * type tests, the ADBC equivalence suite, …) reuses one canonical materialisation instead of a
 * hand-rolled copy.
 */
public final class BlockMaterializer {

    private BlockMaterializer() {}

    /** Materialises every non-empty block of {@code result} into rows of boxed, null-aware values. */
    public static List<Object[]> materialize(QueryResult result) {
        List<Object[]> rows = new ArrayList<>();
        Iterator<Block> blocks = result.blocks();
        while (blocks.hasNext()) {
            Block block = blocks.next();
            if (block.isEmpty()) {
                continue;
            }
            int columns = block.columnCount();
            int rowCount = block.rowCount();
            for (int r = 0; r < rowCount; r++) {
                Object[] row = new Object[columns];
                for (int c = 0; c < columns; c++) {
                    Column column = block.column(c);
                    boolean isNull = column.nulls() != null && column.nulls()[r];
                    row[c] = isNull ? null : column.value(r);
                }
                rows.add(row);
            }
        }
        return rows;
    }
}
