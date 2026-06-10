package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.protocol.BinaryWriter;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Behavioural unit tests for the lazy, {@link java.util.Spliterator}-backed
 * {@code query(sql, Class)} stream — exercised via the package-private
 * {@link ClickHouseConnectionImpl#streamMapped(QueryResult, Class)} entry point with a
 * fake {@link QueryResult} so no server or transport is needed.
 *
 * <p>The fake's block iterator is a counting spy that records how many blocks have
 * actually been pulled, proving the stream maps rows on demand (no eager
 * {@code List}), and the fake records whether {@link QueryResult#close()} has run —
 * standing in for the connection-guard release callback wired into the stream's
 * {@code onClose}.
 */
class MappedStreamLazinessTest {

    /** Simple row type. */
    public record IdRow(long id) {}

    @Test
    void streamIsLazy_firstRowAvailableBeforeAllBlocksPulled() {
        // Two blocks of one row each.
        FakeQueryResult result = new FakeQueryResult(block(1L), block(2L));

        try (Stream<IdRow> stream = ClickHouseConnectionImpl.streamMapped(result, IdRow.class)) {
            Iterator<IdRow> it = stream.iterator();

            // No block pulled yet: building the stream only reads metadata (the header),
            // never the data blocks.
            assertEquals(0, result.blocksPulled(), "no data blocks should be pulled before consumption");

            assertTrue(it.hasNext());
            assertEquals(1L, it.next().id());
            // First row produced after pulling exactly ONE block — not the whole result.
            assertEquals(1, result.blocksPulled(), "only the first block should be pulled for the first row");

            assertTrue(it.hasNext());
            assertEquals(2L, it.next().id());
            assertEquals(2, result.blocksPulled());

            assertFalse(it.hasNext());
        }
    }

    @Test
    void exhaustingTheStreamReleasesTheGuard() {
        FakeQueryResult result = new FakeQueryResult(block(1L), block(2L));

        List<Long> ids = new ArrayList<>();
        try (Stream<IdRow> stream = ClickHouseConnectionImpl.streamMapped(result, IdRow.class)) {
            stream.forEach(r -> ids.add(r.id()));
            // Natural exhaustion runs the result's release (its block iterator hits
            // end-of-stream and closes); the try-with-resources close is then idempotent.
            assertTrue(result.isClosed(), "exhausting the stream must release (close) the result");
        }
        assertEquals(Arrays.asList(1L, 2L), ids);
        assertEquals(1, result.closeCount(), "release must run exactly once");
    }

    @Test
    void closingAConsumedButUnclosedStreamReleasesTheGuard() {
        // A FakeQueryResult that does NOT self-close on exhaustion, so the ONLY path to
        // release is the stream's onClose -> result.close().
        FakeQueryResult result = new FakeQueryResult(false, block(1L), block(2L));

        List<Long> ids = new ArrayList<>();
        try (Stream<IdRow> stream = ClickHouseConnectionImpl.streamMapped(result, IdRow.class)) {
            stream.iterator().forEachRemaining(r -> ids.add(r.id()));
            assertFalse(result.isClosed(), "guard not yet released before stream close");
        } // <- onClose fires here
        assertEquals(Arrays.asList(1L, 2L), ids);
        assertTrue(result.isClosed(), "closing the stream must release the guard");
        assertEquals(1, result.closeCount());
    }

    @Test
    void closedButUnconsumedStreamReleasesTheGuard() {
        FakeQueryResult result = new FakeQueryResult(block(1L), block(2L), block(3L));

        // Open the stream and close it WITHOUT consuming any row.
        Stream<IdRow> stream = ClickHouseConnectionImpl.streamMapped(result, IdRow.class);
        assertEquals(0, result.blocksPulled());
        stream.close();

        assertTrue(result.isClosed(), "closing an unconsumed stream must release the guard");
        assertEquals(1, result.closeCount());
    }

    @Test
    void mappingFailureMidStreamReleasesGuard_withoutTerminalOpOrTryWithResources() {
        // selfCloseOnExhaust=false so the ONLY way the guard can be released is the new
        // tryAdvance onFailure hook — the stream is driven manually (no terminal op, no
        // try-with-resources), so onClose would NOT run on the failure path.
        FakeQueryResult result = new FakeQueryResult(false, poisonBlock());

        Stream<IdRow> stream = ClickHouseConnectionImpl.streamMapped(result, IdRow.class);
        Iterator<IdRow> it = stream.iterator();

        RuntimeException ex = assertThrows(RuntimeException.class, it::next);
        assertEquals("boom", ex.getMessage(), "original mapping failure must propagate");
        assertTrue(result.isClosed(),
                "a mid-stream failure must release the guard even with no terminal op / close");
        assertEquals(1, result.closeCount(), "release must run exactly once");
    }

    @Test
    void doubleCloseReleasesExactlyOnce() {
        FakeQueryResult result = new FakeQueryResult(block(1L));

        Stream<IdRow> stream = ClickHouseConnectionImpl.streamMapped(result, IdRow.class);
        stream.close();
        stream.close();
        assertEquals(1, result.closeCount(), "Stream.onClose runs at most once even on repeated close");
    }

    // ------------------------------------------------------------------
    // Test doubles
    // ------------------------------------------------------------------

    /** Builds a single-column ("id" UInt64-like) block carrying one long value. */
    private static Block block(long value) {
        Block b = new Block();
        Column col = new Column("id", "UInt64");
        col.codec(new LongArrayCodec());
        col.values(new long[] {value});
        b.addColumn(col);
        b.rowCount(1);
        return b;
    }

    /** A one-row block whose codec throws on {@code get}, to force a mid-stream tryAdvance failure. */
    private static Block poisonBlock() {
        Block b = new Block();
        Column col = new Column("id", "UInt64");
        col.codec(new PoisonCodec());
        col.values(new long[] {0L});
        b.addColumn(col);
        b.rowCount(1);
        return b;
    }

    /** Codec whose {@link #get} always throws — simulates a decode/mapping failure mid-stream. */
    private static final class PoisonCodec implements ColumnCodec<long[]> {
        @Override public String typeName() { return "UInt64"; }
        @Override public long[] allocate(int rowCount) { return new long[rowCount]; }
        @Override public void read(BinaryReader in, int rowCount, long[] dest) { }
        @Override public void write(BinaryWriter out, long[] src, int rowCount) { }
        @Override public Object get(long[] array, int row) { throw new RuntimeException("boom"); }
        @Override public void set(long[] array, int row, Object value) { }
        @Override public Class<?> javaType() { return Long.class; }
    }

    /** Minimal codec backing a {@code long[]} — only {@link #get} is exercised by the mapper path. */
    private static final class LongArrayCodec implements ColumnCodec<long[]> {
        @Override public String typeName() { return "UInt64"; }
        @Override public long[] allocate(int rowCount) { return new long[rowCount]; }
        @Override public void read(BinaryReader in, int rowCount, long[] dest) { }
        @Override public void write(BinaryWriter out, long[] src, int rowCount) { }
        @Override public Object get(long[] array, int row) { return array[row]; }
        @Override public void set(long[] array, int row, Object value) { array[row] = ((Number) value).longValue(); }
        @Override public Class<?> javaType() { return Long.class; }
    }

    /**
     * Fake {@link QueryResult} over a fixed set of data blocks. Records how many blocks
     * have been pulled (laziness spy) and how many times {@link #close()} ran (guard
     * release spy).
     */
    private static final class FakeQueryResult implements QueryResult {
        private final List<Block> data;
        private final boolean selfCloseOnExhaust;
        private int blocksPulled;
        /** Number of times the (idempotent) guard release actually ran. */
        private int releaseCount;
        private boolean released;
        private boolean iteratorIssued;

        FakeQueryResult(Block... blocks) {
            this(true, blocks);
        }

        FakeQueryResult(boolean selfCloseOnExhaust, Block... blocks) {
            this.data = Arrays.asList(blocks);
            this.selfCloseOnExhaust = selfCloseOnExhaust;
        }

        int blocksPulled() { return blocksPulled; }
        int closeCount() { return releaseCount; }
        boolean isClosed() { return released; }

        @Override
        public List<String> columnNames() {
            return List.of("id");
        }

        @Override
        public List<String> columnTypes() {
            return List.of("UInt64");
        }

        @Override
        public Iterator<Block> blocks() {
            if (iteratorIssued) {
                throw new IllegalStateException("blocks() may only be consumed once");
            }
            iteratorIssued = true;
            return new Iterator<>() {
                private int idx;

                @Override
                public boolean hasNext() {
                    if (idx < data.size()) {
                        return true;
                    }
                    // Mirror QueryResultImpl: end-of-stream releases the guard.
                    if (selfCloseOnExhaust) {
                        close();
                    }
                    return false;
                }

                @Override
                public Block next() {
                    if (idx >= data.size()) {
                        throw new NoSuchElementException();
                    }
                    blocksPulled++;
                    return data.get(idx++);
                }
            };
        }

        @Override
        public void close() {
            // Idempotent guard release, mirroring QueryResultImpl#markReleased: the
            // connection guard is released at most once however many times close()
            // (self-close on exhaust + the stream's onClose) fires.
            if (!released) {
                released = true;
                releaseCount++;
            }
        }
    }
}
