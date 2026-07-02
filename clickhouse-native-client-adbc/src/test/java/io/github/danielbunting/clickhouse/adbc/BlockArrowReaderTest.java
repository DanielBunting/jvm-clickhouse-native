package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.test.QueryResults;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Offline unit coverage for {@link BlockArrowReader} streaming semantics: schema-before-data,
 * one-native-block-per-batch, empty-block skipping, exhaustion, close ownership (the reader owns
 * the {@code QueryResult} and its allocator), and mid-stream failure propagation. The ADBC
 * analogue of the JDBC module's {@code ChResultSetContractTest} cursor contract.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class BlockArrowReaderTest {

    // ---- schema & iteration ---------------------------------------------------------------------

    @Test
    @DisplayName("the schema is available before the first batch is loaded")
    void schemaAvailableBeforeFirstBatch(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = int64Blocks("n", new long[] {1, 2});
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            assertEquals("n", schema.getFields().get(0).getName());
        }
    }

    @Test
    @DisplayName("one native block becomes one Arrow batch, in order, with per-block row counts")
    void oneBlockPerBatchInOrder(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = QueryResults.of(
                List.of("n"), List.of("Int64"),
                List.of(
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {1, 2, 3}, null)),
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {4, 5}, null))));
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            assertTrue(reader.loadNextBatch());
            assertEquals(3, root.getRowCount());
            assertEquals(1L, ((BigIntVector) root.getVector("n")).get(0));
            assertEquals(3L, ((BigIntVector) root.getVector("n")).get(2));

            assertTrue(reader.loadNextBatch());
            assertEquals(2, root.getRowCount());
            assertEquals(4L, ((BigIntVector) root.getVector("n")).get(0));
            assertEquals(5L, ((BigIntVector) root.getVector("n")).get(1));

            assertFalse(reader.loadNextBatch());
        }
    }

    @Test
    @DisplayName("empty progress/terminator blocks are skipped, not surfaced as empty batches")
    void emptyBlocksAreSkipped(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = QueryResults.of(
                List.of("n"), List.of("Int64"),
                List.of(
                        TestBlocks.blockOf(),
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {1}, null)),
                        TestBlocks.blockOf(),
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {2}, null)),
                        TestBlocks.blockOf()));
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            int batches = 0;
            long rows = 0;
            while (reader.loadNextBatch()) {
                batches++;
                rows += reader.getVectorSchemaRoot().getRowCount();
            }
            assertEquals(2, batches);
            assertEquals(2, rows);
        }
    }

    @Test
    @DisplayName("a result with no blocks exhausts immediately")
    void zeroBlocksExhaustImmediately(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = QueryResults.empty(List.of("n"), List.of("Int64"));
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            assertFalse(reader.loadNextBatch());
            assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
        }
    }

    @Test
    @DisplayName("exhaustion is stable: loadNextBatch keeps returning false")
    void exhaustionIsStable(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = int64Blocks("n", new long[] {1});
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            assertTrue(reader.loadNextBatch());
            assertFalse(reader.loadNextBatch());
            assertFalse(reader.loadNextBatch());
        }
    }

    @Test
    @DisplayName("the VectorSchemaRoot instance is reused across batches (vectors refilled in place)")
    void rootIsReusedAcrossBatches(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = QueryResults.of(
                List.of("n"), List.of("Int64"),
                List.of(
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {1}, null)),
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {2}, null))));
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            VectorSchemaRoot first = reader.getVectorSchemaRoot();
            reader.loadNextBatch();
            reader.loadNextBatch();
            assertSame(first, reader.getVectorSchemaRoot());
        }
    }

    @Test
    @DisplayName("a Nullable column's null map carries through to the Arrow validity bitmap")
    void nullableColumnCarriesNulls(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = QueryResults.of(
                List.of("n"), List.of("Nullable(Int64)"),
                List.of(TestBlocks.blockOf(TestBlocks.int64Column(
                        "n", new long[] {1, 0, 3}, new boolean[] {false, true, false}))));
        try (BlockArrowReader reader = reader(allocator, result, "n", "Nullable(Int64)")) {
            assertTrue(reader.loadNextBatch());
            BigIntVector n = (BigIntVector) reader.getVectorSchemaRoot().getVector("n");
            assertFalse(n.isNull(0));
            assertTrue(n.isNull(1), "the null cell must be null in Arrow");
            assertFalse(n.isNull(2));
            assertEquals(3L, n.get(2));
        }
    }

    // ---- close ownership ------------------------------------------------------------------------

    @Test
    @DisplayName("close closes the underlying QueryResult exactly once, even when called twice")
    void closeClosesQueryResultOnce(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = int64Blocks("n", new long[] {1});
        BlockArrowReader reader = reader(allocator, result, "n", "Int64");
        reader.loadNextBatch();
        reader.close();
        reader.close();
        assertEquals(1, result.closeCount());
    }

    @Test
    @DisplayName("closing mid-stream still closes the QueryResult (early consumer abandon)")
    void closeMidStreamClosesQueryResult(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = QueryResults.of(
                List.of("n"), List.of("Int64"),
                List.of(
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {1}, null)),
                        TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {2}, null))));
        BlockArrowReader reader = reader(allocator, result, "n", "Int64");
        assertTrue(reader.loadNextBatch());
        reader.close();
        assertEquals(1, result.closeCount(), "abandoning a stream must still release the core result");
    }

    // ---- failure propagation ---------------------------------------------------------------------

    @Test
    @DisplayName("a mid-stream core failure propagates from loadNextBatch and close still releases everything")
    void midStreamFailurePropagates(BufferAllocator allocator) throws Exception {
        // DIVERGENCE from the query-start path: loadNextBatch surfaces the raw core exception
        // (no AdbcException wrapping happens below executeQuery). Pinned deliberately.
        QueryResults.Scripted result = QueryResults.of(
                List.of("n"), List.of("Int64"),
                List.of(TestBlocks.blockOf(TestBlocks.int64Column("n", new long[] {1}, null))))
                .failAtBlock(1, new ServerException(159, "DB::Exception", "Timeout exceeded", null));
        BlockArrowReader reader = reader(allocator, result, "n", "Int64");
        try {
            assertTrue(reader.loadNextBatch());
            ClickHouseException ex = assertThrows(ClickHouseException.class, reader::loadNextBatch);
            assertTrue(ex.getMessage().contains("Timeout exceeded"));
        } finally {
            reader.close();
        }
        assertEquals(1, result.closeCount());
    }

    @Test
    @DisplayName("bytesRead is not tracked and reports zero")
    void bytesReadReportsZero(BufferAllocator allocator) throws Exception {
        QueryResults.Scripted result = int64Blocks("n", new long[] {1});
        try (BlockArrowReader reader = reader(allocator, result, "n", "Int64")) {
            reader.loadNextBatch();
            assertEquals(0L, reader.bytesRead());
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static QueryResults.Scripted int64Blocks(String name, long[] values) {
        return QueryResults.of(
                List.of(name), List.of("Int64"),
                List.of(TestBlocks.blockOf(TestBlocks.int64Column(name, values, null))));
    }

    /** A reader owning a child of the test allocator, over {@code result} with one column. */
    private static BlockArrowReader reader(
            BufferAllocator allocator, QueryResults.Scripted result, String name, String type) {
        BufferAllocator readerAllocator =
                allocator.newChildAllocator("test-reader", 0, Long.MAX_VALUE);
        return new BlockArrowReader(
                readerAllocator, result, ClickHouseArrowTypes.schema(List.of(name), List.of(type)));
    }
}
