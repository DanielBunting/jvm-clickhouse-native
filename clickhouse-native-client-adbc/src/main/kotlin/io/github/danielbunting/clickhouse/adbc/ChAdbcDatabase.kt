package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ClickHouseConnection
import org.apache.arrow.adbc.core.AdbcConnection
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.memory.BufferAllocator

/**
 * An [AdbcDatabase]: holds the parsed [ClickHouseConfig] (no connection is opened until
 * [connect]) and owns [databaseAllocator] (a child of the driver allocator). Each [connect]
 * opens a fresh native connection with its own child allocator.
 */
public class ChAdbcDatabase internal constructor(
    private val config: ClickHouseConfig,
    private val databaseAllocator: BufferAllocator,
) : AdbcDatabase {

    override fun connect(): AdbcConnection {
        val core = try {
            ClickHouseConnection.open(config)
        } catch (e: RuntimeException) {
            throw AdbcErrors.io("Failed to open ClickHouse connection: ${e.message}", e)
        }
        val connectionAllocator =
            databaseAllocator.newChildAllocator("adbc-connection", 0, Long.MAX_VALUE)
        return try {
            ChAdbcConnection(core, connectionAllocator)
        } catch (t: Throwable) {
            core.close()
            connectionAllocator.close()
            throw t
        }
    }

    private var closed = false

    override fun close() {
        if (closed) {
            return
        }
        closed = true
        databaseAllocator.close()
    }
}
