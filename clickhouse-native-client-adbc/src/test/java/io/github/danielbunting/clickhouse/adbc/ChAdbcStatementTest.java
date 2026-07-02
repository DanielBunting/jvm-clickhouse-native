package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.ServerException;
import io.github.danielbunting.clickhouse.UnsupportedTypeException;
import io.github.danielbunting.clickhouse.test.QueryResults;
import io.github.danielbunting.clickhouse.test.ScriptedConnection;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Server-free unit coverage for {@link ChAdbcStatement}: invalid-state guards, verbatim SQL
 * passthrough (ADBC does no client-side SQL rewriting, unlike the JDBC prepared-statement
 * layer), the -1 affected-rows contract, the error funnel through {@code AdbcErrors.wrap}, and
 * resource release on failure. The ADBC analogue of the JDBC module's {@code ChStatementTest}/
 * {@code ChStatementBehaviorTest}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class ChAdbcStatementTest {

    // ---- invalid-state guards ---------------------------------------------------------------

    @Test
    @DisplayName("executeQuery without setSqlQuery raises INVALID_STATE")
    void executeQueryWithoutSqlIsInvalidState(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.INVALID_STATE, ex.getStatus());
            assertTrue(core.queried.isEmpty(), "no query must reach the wire");
        }
    }

    @Test
    @DisplayName("executeUpdate without setSqlQuery raises INVALID_STATE")
    void executeUpdateWithoutSqlIsInvalidState(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
            assertEquals(AdbcStatusCode.INVALID_STATE, ex.getStatus());
            assertTrue(core.executed.isEmpty());
        }
    }

    @Test
    @DisplayName("an ingest statement without a bound root raises INVALID_STATE")
    void ingestWithoutBindIsInvalidState(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.bulkIngest("target", BulkIngestMode.APPEND)) {
            AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
            assertEquals(AdbcStatusCode.INVALID_STATE, ex.getStatus());
        }
    }

    // ---- SQL passthrough & affected-rows contract --------------------------------------------

    @Test
    @DisplayName("executeQuery passes the SQL to the core verbatim — no client-side rewriting")
    void executeQueryPassesSqlVerbatim(BufferAllocator allocator) throws Exception {
        // Placeholder-looking tokens must survive untouched: ADBC has no `?` interpolation.
        String sql = "SELECT /* ? {p:Int32} '?' */ toInt64(1) AS n -- trailing ?";
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(int64Result("n", 1));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery(sql);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                assertEquals(List.of(sql), core.queried);
            }
        }
    }

    @Test
    @DisplayName("executeUpdate passes DDL/DML to the core verbatim")
    void executeUpdatePassesSqlVerbatim(BufferAllocator allocator) throws Exception {
        String sql = "CREATE TABLE t (id Int64) ENGINE = MergeTree ORDER BY id";
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery(sql);
            statement.executeUpdate();
            assertEquals(List.of(sql), core.executed);
        }
    }

    @Test
    @DisplayName("executeUpdate reports -1 affected rows (native protocol gives no count)")
    void executeUpdateReportsUnknownAffectedRows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("INSERT INTO t VALUES (1)");
            assertEquals(-1, statement.executeUpdate().getAffectedRows());
        }
    }

    @Test
    @DisplayName("executeQuery reports -1 affected rows (not meaningful for a SELECT)")
    void executeQueryReportsUnknownAffectedRows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(int64Result("n", 7));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 7 AS n");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                assertEquals(-1, result.getAffectedRows());
            }
        }
    }

    @Test
    @DisplayName("setSqlQuery replaces the previously set query")
    void setSqlQueryReplacesPrevious(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("CREATE TABLE old (id Int64)");
            statement.setSqlQuery("CREATE TABLE current (id Int64)");
            statement.executeUpdate();
            assertEquals(List.of("CREATE TABLE current (id Int64)"), core.executed);
        }
    }

    @Test
    @DisplayName("prepare() is a no-op: execution still works afterwards")
    void prepareIsNoOp(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(int64Result("n", 3));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 3 AS n");
            statement.prepare();
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch());
                assertEquals(3L, ((BigIntVector) reader.getVectorSchemaRoot().getVector("n")).get(0));
            }
        }
    }

    @Test
    @DisplayName("a bound root is ignored when the SQL has no parameter placeholders")
    void bindOnNonIngestStatementIsIgnored(BufferAllocator allocator) throws Exception {
        // A bound root is only consulted when the statement is an ingest statement or the SQL
        // carries placeholders (see ChAdbcStatementParamsTest); plain SQL executes verbatim.
        ScriptedConnection core = new ScriptedConnection();
        Schema schema = new Schema(List.of(Field.nullable("x", new ArrowType.Int(64, true))));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            statement.bind(root);
            statement.setSqlQuery("INSERT INTO t SELECT 1");
            assertEquals(-1, statement.executeUpdate().getAffectedRows());
            assertEquals(List.of("INSERT INTO t SELECT 1"), core.executed);
        }
    }

    @Test
    @DisplayName("cancel() delegates to the native connection's cross-thread cancel")
    void cancelDelegatesToCore(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.cancel();
            assertEquals(1, core.cancelCount);
        }
    }

    // ---- error funnel -------------------------------------------------------------------------

    @Test
    @DisplayName("a core ClickHouseException from query() maps to IO and keeps the cause")
    void queryFailureMapsToIo(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new ClickHouseException("connection reset mid-handshake"));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 1");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertInstanceOf(ClickHouseException.class, ex.getCause());
            assertNotNull(ex.getMessage());
            assertTrue(ex.getMessage().contains("connection reset mid-handshake"),
                    "the cause's message must surface to the ADBC caller");
        }
    }

    @Test
    @DisplayName("an UnsupportedTypeException from query() maps to NOT_IMPLEMENTED")
    void unsupportedTypeAtQueryMapsToNotImplemented(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new UnsupportedTypeException("AggregateFunction(sum, UInt64)"));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT state FROM agg");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    @Test
    @DisplayName("an UnsupportedOperationException from query() maps to NOT_IMPLEMENTED")
    void unsupportedOperationMapsToNotImplemented(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new UnsupportedOperationException("not supported by this connection"));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 1");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    @Test
    @DisplayName("a ServerException's ClickHouse error code becomes the AdbcException vendor code")
    void serverErrorCodePropagatesToVendorCode(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new ServerException(60, "DB::Exception", "Table default.missing does not exist", null));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT * FROM missing");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertEquals(60, ex.getVendorCode(), "UNKNOWN_TABLE (60) must surface as the vendor code");
        }
    }

    @Test
    @DisplayName("an unsupported column type at schema-mapping time maps to NOT_IMPLEMENTED and frees the result")
    void unsupportedTypeAtSchemaMappingFreesResources(BufferAllocator allocator) throws Exception {
        // query() succeeds, but the result metadata declares a column ADBC cannot map; the
        // statement must close the QueryResult AND the reader allocator before rethrowing
        // (the allocator half is proven by the leak-check extension at teardown).
        ScriptedConnection core = new ScriptedConnection();
        QueryResults.Scripted result = QueryResults.empty(
                List.of("state"), List.of("AggregateFunction(sum, UInt64)"));
        core.enqueueResult(result);
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT state FROM agg");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
            assertEquals(1, result.closeCount(), "the streaming result must be closed on failure");
        }
    }

    @Test
    @DisplayName("a core failure in executeUpdate maps to IO with the vendor code")
    void executeUpdateFailureMapsToIo(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextExecuteWith(new ServerException(57, "DB::Exception", "Table already exists", null));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("CREATE TABLE t (id Int64) ENGINE = MergeTree ORDER BY id");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertEquals(57, ex.getVendorCode());
        }
    }

    @Test
    @DisplayName("a statement stays usable after a scripted failure")
    void statementUsableAfterFailure(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new ClickHouseException("transient failure"));
        core.enqueueResult(int64Result("n", 5));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT 5 AS n");
            assertThrows(AdbcException.class, statement::executeQuery);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch());
                assertEquals(5L, ((BigIntVector) reader.getVectorSchemaRoot().getVector("n")).get(0));
            }
        }
    }

    // ---- closed-statement guards ---------------------------------------------------------------

    @Test
    @DisplayName("executeQuery on a closed statement raises INVALID_STATE")
    void executeQueryOnClosedStatementThrows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcStatement statement = connection.createStatement();
            statement.setSqlQuery("SELECT 1");
            statement.close();
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.INVALID_STATE, ex.getStatus());
            assertTrue(core.queried.isEmpty());
        }
    }

    @Test
    @DisplayName("executeUpdate on a closed statement raises INVALID_STATE")
    void executeUpdateOnClosedStatementThrows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcStatement statement = connection.createStatement();
            statement.setSqlQuery("DROP TABLE t");
            statement.close();
            AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
            assertEquals(AdbcStatusCode.INVALID_STATE, ex.getStatus());
            assertTrue(core.executed.isEmpty());
        }
    }

    @Test
    @DisplayName("closing a statement twice is safe")
    void doubleCloseIsSafe(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcStatement statement = connection.createStatement();
            statement.close();
            statement.close();
        }
    }

    @Test
    @DisplayName("a fully consumed reader closes the underlying core result exactly once")
    void readerCloseClosesCoreResultOnce(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        QueryResults.Scripted scripted = (QueryResults.Scripted) int64Result("n", 1, 2, 3);
        core.enqueueResult(scripted);
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT n FROM t");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                long rows = 0;
                while (reader.loadNextBatch()) {
                    rows += reader.getVectorSchemaRoot().getRowCount();
                }
                assertEquals(3, rows);
                assertEquals(0, scripted.closeCount(), "result must stay open while streaming");
            }
            assertEquals(1, scripted.closeCount(), "closing the ADBC result must close the core result once");
        }
    }

    @Test
    @DisplayName("an empty result exposes the mapped schema and zero batches")
    void emptyResultExposesSchema(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("n"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT n FROM t WHERE 0");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertEquals("n", reader.getVectorSchemaRoot().getSchema().getFields().get(0).getName());
                assertFalse(reader.loadNextBatch());
                assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
            }
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** A one-column Int64 scripted result named {@code name} carrying {@code values}. */
    private static io.github.danielbunting.clickhouse.QueryResult int64Result(String name, long... values) {
        return QueryResults.of(
                List.of(name),
                List.of("Int64"),
                List.of(TestBlocks.blockOf(TestBlocks.int64Column(name, values, null))));
    }
}
