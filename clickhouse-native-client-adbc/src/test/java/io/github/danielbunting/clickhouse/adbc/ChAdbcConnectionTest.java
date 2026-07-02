package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.test.ScriptedConnection;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection.GetObjectsDepth;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Server-free unit coverage for {@link ChAdbcConnection}: the autocommit-only transaction
 * contract, close ordering/idempotency, cancel delegation, {@code getTableSchema} argument
 * validation and SQL shape, and the closed-connection guards. The ADBC analogue of the
 * behavioral kernel of the JDBC module's {@code ChConnectionTest}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class ChAdbcConnectionTest {

    // ---- transaction stance ---------------------------------------------------------------

    @Test
    @DisplayName("autocommit reports true")
    void autoCommitIsAlwaysOn(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            assertTrue(connection.getAutoCommit());
        }
    }

    @Test
    @DisplayName("setAutoCommit(true) is an accepted no-op")
    void enablingAutoCommitIsNoOp(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            connection.setAutoCommit(true);
            assertTrue(connection.getAutoCommit());
        }
    }

    @Test
    @DisplayName("setAutoCommit(false) raises NOT_IMPLEMENTED (no ClickHouse transactions)")
    void disablingAutoCommitIsNotImplemented(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcException ex = assertThrows(AdbcException.class, () -> connection.setAutoCommit(false));
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    @Test
    @DisplayName("commit() raises NOT_IMPLEMENTED")
    void commitIsNotImplemented(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcException ex = assertThrows(AdbcException.class, connection::commit);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    @Test
    @DisplayName("rollback() raises NOT_IMPLEMENTED")
    void rollbackIsNotImplemented(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcException ex = assertThrows(AdbcException.class, connection::rollback);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    // ---- lifecycle --------------------------------------------------------------------------

    @Test
    @DisplayName("close closes the native connection and is idempotent")
    void closeClosesCoreOnceAndIsIdempotent(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
        connection.close();
        connection.close();
        assertEquals(1, core.closeCount, "the native connection must be closed exactly once");
    }

    @Test
    @DisplayName("the connection allocator is freed even when the native close throws")
    void allocatorFreedWhenCoreCloseThrows(BufferAllocator allocator) {
        // The leak-check extension proves the allocator half at teardown: if close() skipped
        // the allocator on a core failure, the child allocator would still be open.
        ScriptedConnection core = new ScriptedConnection();
        core.failCloseWith(new ClickHouseException("socket already broken"));
        ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
        assertThrows(ClickHouseException.class, connection::close);
        assertEquals(1, core.closeCount);
    }

    @Test
    @DisplayName("cancel() delegates to the native connection")
    void cancelDelegatesToCore(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            connection.cancel();
            assertEquals(1, core.cancelCount);
        }
    }

    @Test
    @DisplayName("createStatement returns a fresh statement per call")
    void createStatementReturnsDistinctStatements(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            assertNotSame(connection.createStatement(), connection.createStatement());
        }
    }

    // ---- getTableSchema ------------------------------------------------------------------------

    @Test
    @DisplayName("getTableSchema without a table name raises INVALID_ARGUMENT before any query")
    void getTableSchemaRequiresTableName(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcException ex = assertThrows(AdbcException.class,
                    () -> connection.getTableSchema(null, null, null));
            assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
            assertTrue(core.queried.isEmpty(), "validation must fail before touching the wire");
        }
    }

    @Test
    @DisplayName("getTableSchema probes system.columns for the named database and table, ordered by position")
    void getTableSchemaSqlShape(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.columns", SystemTableBlocks.schemaColumns(
                new String[] {"id", "Int64"}));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            connection.getTableSchema(null, "analytics", "events");
            String sql = core.queried.get(0);
            assertTrue(sql.contains("database = 'analytics'"), sql);
            assertTrue(sql.contains("table = 'events'"), sql);
            assertTrue(sql.contains("ORDER BY position"), sql);
        }
    }

    @Test
    @DisplayName("getTableSchema single-quotes in identifiers are SQL-escaped (no injection)")
    void getTableSchemaEscapesQuotes(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.columns", SystemTableBlocks.schemaColumns(
                new String[] {"id", "Int64"}));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            connection.getTableSchema(null, "we'ird", "ta'ble");
            String sql = core.queried.get(0);
            assertTrue(sql.contains("we''ird"), sql);
            assertTrue(sql.contains("ta''ble"), sql);
        }
    }

    @Test
    @DisplayName("getTableSchema without a dbSchema falls back to currentDatabase()")
    void getTableSchemaDefaultsToCurrentDatabase(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.columns", SystemTableBlocks.schemaColumns(
                new String[] {"id", "Int64"}));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            connection.getTableSchema(null, null, "events");
            assertTrue(core.queried.get(0).contains("database = currentDatabase()"), core.queried.get(0));
        }
    }

    @Test
    @DisplayName("getTableSchema skips empty progress blocks in the system.columns stream")
    void getTableSchemaSkipsEmptyBlocks(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.columns", io.github.danielbunting.clickhouse.test.QueryResults.of(
                List.of("name", "type"), List.of("String", "String"),
                List.of(
                        io.github.danielbunting.clickhouse.adbc.TestBlocks.blockOf(),
                        io.github.danielbunting.clickhouse.adbc.TestBlocks.blockOf(
                                io.github.danielbunting.clickhouse.adbc.TestBlocks.stringColumn(
                                        "name", new String[] {"id"}, null),
                                io.github.danielbunting.clickhouse.adbc.TestBlocks.stringColumn(
                                        "type", new String[] {"Int64"}, null)))));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            Schema schema = connection.getTableSchema(null, null, "events");
            assertEquals(List.of("id"), schema.getFields().stream().map(f -> f.getName()).toList());
        }
    }

    @Test
    @DisplayName("getTableSchema on a missing table raises NOT_FOUND naming the table")
    void getTableSchemaMissingTableIsNotFound(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.columns", SystemTableBlocks.schemaColumns());
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcException ex = assertThrows(AdbcException.class,
                    () -> connection.getTableSchema(null, null, "nope"));
            assertEquals(AdbcStatusCode.NOT_FOUND, ex.getStatus());
            assertTrue(ex.getMessage().contains("nope"));
        }
    }

    @Test
    @DisplayName("a core failure during getTableSchema maps to IO")
    void getTableSchemaCoreFailureIsIo(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new ClickHouseException("stream broken"));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcException ex = assertThrows(AdbcException.class,
                    () -> connection.getTableSchema(null, null, "events"));
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
        }
    }

    @Test
    @DisplayName("getTableSchema maps the ClickHouse column types to Arrow fields in declared order")
    void getTableSchemaMapsFields(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("system.columns", SystemTableBlocks.schemaColumns(
                new String[] {"id", "Int64"},
                new String[] {"name", "Nullable(String)"}));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            Schema schema = connection.getTableSchema(null, null, "events");
            assertEquals(List.of("id", "name"),
                    schema.getFields().stream().map(f -> f.getName()).toList());
            assertEquals(new ArrowType.Int(64, true), schema.getFields().get(0).getType());
            assertFalse(schema.getFields().get(0).isNullable(), "Int64 is not nullable");
            assertEquals(ArrowType.Utf8.INSTANCE, schema.getFields().get(1).getType());
            assertTrue(schema.getFields().get(1).isNullable(), "Nullable(String) must map nullable");
        }
    }

    // ---- closed-connection guards ------------------------------------------------------------

    @Test
    @DisplayName("createStatement/bulkIngest on a closed connection raise INVALID_STATE")
    void statementCreationOnClosedConnectionThrows(BufferAllocator allocator) throws Exception {
        ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator);
        connection.close();
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class, connection::createStatement).getStatus());
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class,
                        () -> connection.bulkIngest("t", org.apache.arrow.adbc.core.BulkIngestMode.APPEND))
                        .getStatus());
    }

    @Test
    @DisplayName("metadata calls on a closed connection raise INVALID_STATE")
    void metadataOnClosedConnectionThrows(BufferAllocator allocator) throws Exception {
        ChAdbcConnection connection = AdbcTestConnections.connection(new ScriptedConnection(), allocator);
        connection.close();
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class, () -> connection.getTableSchema(null, null, "t")).getStatus());
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class, connection::getTableTypes).getStatus());
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class, () -> connection.getInfo((int[]) null)).getStatus());
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class,
                        () -> connection.getObjects(GetObjectsDepth.ALL, null, null, null, null, null))
                        .getStatus());
    }

    @Test
    @DisplayName("cancel on a closed connection raises INVALID_STATE")
    void cancelOnClosedConnectionThrows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
        connection.close();
        assertEquals(AdbcStatusCode.INVALID_STATE,
                assertThrows(AdbcException.class, connection::cancel).getStatus());
        assertEquals(0, core.cancelCount);
    }
}
