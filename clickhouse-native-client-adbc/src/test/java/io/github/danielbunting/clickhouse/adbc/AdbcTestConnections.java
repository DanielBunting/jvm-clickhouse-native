package io.github.danielbunting.clickhouse.adbc;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Wires a fake/scripted {@link ClickHouseConnection} (from the core module's test fixtures) into
 * the ADBC driver stack for server-free unit tests. The returned {@link ChAdbcConnection} owns a
 * child of the test's leak-checked allocator, so a test that closes the connection proves it
 * released every Arrow buffer.
 */
final class AdbcTestConnections {

    private AdbcTestConnections() {}

    /** A {@link ChAdbcConnection} over {@code core}, owning a child of {@code allocator}. */
    static ChAdbcConnection connection(ClickHouseConnection core, BufferAllocator allocator) {
        return new ChAdbcConnection(
                core, allocator.newChildAllocator("adbc-test-connection", 0, Long.MAX_VALUE));
    }
}
