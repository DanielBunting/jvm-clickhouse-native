package io.github.danielbunting.clickhouse.test;

import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import java.util.Iterator;
import java.util.List;

/**
 * Factories for scripted {@link QueryResult}s backing server-free unit tests: a fixed list of
 * pre-built {@link Block}s plus column metadata. The returned {@link Scripted} instance counts
 * {@link QueryResult#close()} calls and can be armed to throw from {@code blocks().next()} at a
 * chosen index, so consumers' mid-stream failure and resource-release paths are testable offline.
 */
public final class QueryResults {

    private QueryResults() {}

    /** A result with the given column metadata delivering {@code blocks} in order. */
    public static Scripted of(List<String> names, List<String> types, List<Block> blocks) {
        return new Scripted(names, types, blocks);
    }

    /** A result with column metadata but no data blocks (a query matching zero rows). */
    public static Scripted empty(List<String> names, List<String> types) {
        return new Scripted(names, types, List.of());
    }

    /** A scripted {@link QueryResult}: fixed blocks, close counting, optional mid-stream failure. */
    public static final class Scripted implements QueryResult {

        private final List<String> names;
        private final List<String> types;
        private final List<Block> blocks;
        private int closeCount;
        private int failAtBlock = -1;
        private RuntimeException failure;

        private Scripted(List<String> names, List<String> types, List<Block> blocks) {
            this.names = List.copyOf(names);
            this.types = List.copyOf(types);
            this.blocks = List.copyOf(blocks);
        }

        /** Arms {@code blocks().next()} to throw {@code failure} instead of yielding block {@code index}. */
        public Scripted failAtBlock(int index, RuntimeException failure) {
            this.failAtBlock = index;
            this.failure = failure;
            return this;
        }

        public int closeCount() {
            return closeCount;
        }

        @Override
        public List<String> columnNames() {
            return names;
        }

        @Override
        public List<String> columnTypes() {
            return types;
        }

        @Override
        public Iterator<Block> blocks() {
            return new Iterator<>() {
                private int next;

                @Override
                public boolean hasNext() {
                    return next < blocks.size() || next == failAtBlock;
                }

                @Override
                public Block next() {
                    if (next == failAtBlock) {
                        next++;
                        throw failure;
                    }
                    return blocks.get(next++);
                }
            };
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
