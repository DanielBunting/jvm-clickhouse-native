package io.github.danielbunting.clickhouse.adbc;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.codec.Float64Codec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Int64Codec;
import io.github.danielbunting.clickhouse.types.codec.StringColumn;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;

/**
 * Hand-built {@link Column}/{@link Block} builders for offline (no-Docker) bridge tests —
 * a Java port of the Kotlin module's {@code TestSupport.kt} helpers, which are not visible
 * across modules.
 */
final class TestBlocks {

    private TestBlocks() {}

    static Column int64Column(String name, long[] values, boolean[] nulls) {
        Column column = new Column(name, nulls == null ? "Int64" : "Nullable(Int64)");
        column.codec(new Int64Codec());
        column.values(values);
        column.rowCount(values.length);
        if (nulls != null) {
            column.nulls(nulls);
        }
        return column;
    }

    static Column int32Column(String name, int[] values, boolean[] nulls) {
        Column column = new Column(name, nulls == null ? "Int32" : "Nullable(Int32)");
        column.codec(new Int32Codec());
        column.values(values);
        column.rowCount(values.length);
        if (nulls != null) {
            column.nulls(nulls);
        }
        return column;
    }

    static Column float64Column(String name, double[] values) {
        Column column = new Column(name, "Float64");
        column.codec(new Float64Codec());
        column.values(values);
        column.rowCount(values.length);
        return column;
    }

    static Column stringColumn(String name, String[] values, boolean[] nulls) {
        StringColumnCodec codec = new StringColumnCodec();
        StringColumn holder = codec.allocate(values.length);
        for (int i = 0; i < values.length; i++) {
            codec.set(holder, i, values[i] == null ? "" : values[i]);
        }
        Column column = new Column(name, nulls == null ? "String" : "Nullable(String)");
        column.codec(codec);
        column.values(holder);
        column.rowCount(values.length);
        if (nulls != null) {
            column.nulls(nulls);
        }
        return column;
    }

    static Block blockOf(Column... columns) {
        Block block = new Block();
        int rows = columns.length == 0 ? 0 : columns[0].rowCount();
        for (Column column : columns) {
            block.addColumn(column);
        }
        block.rowCount(rows);
        return block;
    }
}
