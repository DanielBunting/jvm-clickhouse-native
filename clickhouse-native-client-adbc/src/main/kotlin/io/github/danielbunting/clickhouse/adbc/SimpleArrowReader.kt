package io.github.danielbunting.clickhouse.adbc

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

/**
 * An [ArrowReader] that delivers a single, eagerly-populated batch — used for metadata results
 * (`getTableTypes`, `getInfo`) that fit in one in-memory [VectorSchemaRoot]. Owns its allocator
 * (a child of the connection allocator) and frees it on [close].
 */
internal class SimpleArrowReader(
    private val readerAllocator: BufferAllocator,
    private val schema: Schema,
    private val populate: (VectorSchemaRoot) -> Unit,
) : ArrowReader(readerAllocator) {

    private var delivered = false
    private var closed = false

    override fun readSchema(): Schema = schema

    override fun loadNextBatch(): Boolean {
        if (delivered) {
            return false
        }
        delivered = true
        populate(vectorSchemaRoot)
        return true
    }

    override fun bytesRead(): Long = 0

    override fun closeReadSource() {
        if (closed) {
            return
        }
        closed = true
        readerAllocator.close()
    }
}
