package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.QueryResult
import io.github.danielbunting.clickhouse.protocol.Block
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

/**
 * An [ArrowReader] over a lazy ClickHouse [QueryResult]: each [loadNextBatch] pulls **one**
 * [Block] from `QueryResult.blocks()` into the reused [org.apache.arrow.vector.VectorSchemaRoot],
 * so one native block becomes one Arrow batch and the whole result is never buffered.
 *
 * **Ownership.** This reader owns [readerAllocator] (a child of the connection allocator) and
 * the underlying [QueryResult]. Consumers read the root but must call [close] when done; close
 * frees the root's off-heap buffers, closes the query stream, and then closes the allocator —
 * which asserts (via the allocator) that nothing leaked. The reader does not outlive a [close].
 */
public class BlockArrowReader internal constructor(
    private val readerAllocator: BufferAllocator,
    private val queryResult: QueryResult,
    private val schema: Schema,
) : ArrowReader(readerAllocator) {

    private val blocks: Iterator<Block> = queryResult.blocks()
    private var closed = false

    override fun readSchema(): Schema = schema

    override fun loadNextBatch(): Boolean {
        // Skip empty terminator / progress blocks; one non-empty block = one Arrow batch.
        while (blocks.hasNext()) {
            val block = blocks.next()
            if (block.isEmpty) {
                continue
            }
            BlockToArrow.fill(vectorSchemaRoot, block)
            return true
        }
        return false
    }

    override fun bytesRead(): Long = 0

    override fun closeReadSource() {
        if (closed) {
            return
        }
        closed = true
        try {
            queryResult.close()
        } finally {
            readerAllocator.close()
        }
    }
}
