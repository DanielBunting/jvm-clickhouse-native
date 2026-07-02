package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.ColumnCodec;

import java.util.Iterator;
import java.util.List;

/**
 * Shared, server-free plumbing for {@link ChResultSet} unit tests: builds a single
 * {@link Block} from hand-populated {@link Column}s and exposes it via a fake
 * {@link QueryResult}. Mirrors the harness in {@code ChResultSetPrimitiveTest} but is
 * reusable across the complex-type / typed-getObject / temporal suites.
 */
final class RsFixtures {

    private RsFixtures() {
    }

    private static final class FakeResult implements QueryResult {
        private final Block block;

        FakeResult(Block block) {
            this.block = block;
        }

        @Override
        public List<String> columnNames() {
            return block.columns().stream().map(Column::name).toList();
        }

        @Override
        public List<String> columnTypes() {
            return block.columns().stream().map(Column::type).toList();
        }

        @Override
        public Iterator<Block> blocks() {
            return List.of(block).iterator();
        }

        @Override
        public void close() {
        }
    }

    /** Builds a column from a codec and its raw backing array (as {@code ChResultSetPrimitiveTest.col}). */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static Column col(String name, String type, ColumnCodec codec, Object values, int rows) {
        Column c = new Column(name, type);
        c.codec(codec);
        c.values(values);
        c.rowCount(rows);
        return c;
    }

    /**
     * Builds a single-row-per-value column by allocating the codec's backing array and
     * setting each value through {@link ColumnCodec#set}. Works for any codec whose
     * read path mirrors its write path (Array, LowCardinality, UUID, DateTime, Decimal,
     * IPv4/IPv6, primitives) — notably NOT Map, whose offsets are module-internal.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static Column complexCol(String name, String type, ColumnCodec codec, Object... values) {
        Object backing = codec.allocate(values.length);
        for (int i = 0; i < values.length; i++) {
            codec.set(backing, i, values[i]);
        }
        Column c = new Column(name, type);
        c.codec(codec);
        c.values(backing);
        c.rowCount(values.length);
        return c;
    }

    /** Wraps columns in a single block and returns an un-advanced {@link ChResultSet}. */
    static ChResultSet open(Column... cols) {
        Block b = new Block();
        int rows = cols.length == 0 ? 0 : cols[0].rowCount();
        for (Column c : cols) {
            b.addColumn(c);
        }
        b.rowCount(rows);
        return new ChResultSet(new FakeResult(b));
    }
}
