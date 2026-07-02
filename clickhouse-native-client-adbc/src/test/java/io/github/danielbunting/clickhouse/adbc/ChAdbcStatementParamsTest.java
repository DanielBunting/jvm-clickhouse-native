package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.QueryParameters;
import io.github.danielbunting.clickhouse.test.QueryResults;
import io.github.danielbunting.clickhouse.test.ScriptedConnection;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Server-free unit coverage for the query-parameter binding surface of {@link ChAdbcStatement}:
 * positional {@code ?} → {@code {_pN:String}} rewriting, named {@code {name:Type}} passthrough,
 * Arrow-cell → wire-text conversion, the one-row-per-executeQuery / row-per-execution batch
 * contract, {@code getParameterSchema()}, and the argument/state validation. The ADBC analogue
 * of the JDBC module's {@code ChPreparedStatementServerParamsTest} + binding matrix.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class ChAdbcStatementParamsTest {

    // ---- positional rewrite ---------------------------------------------------------------

    @Test
    @DisplayName("? placeholders rewrite to {_pN:String} and values travel as named parameters")
    void positionalRewriteAndBinding(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "a", "b")) {
            fillUtf8(root, 0, "alpha", "42");
            statement.setSqlQuery("SELECT r FROM t WHERE name = ? AND n = ?");
            statement.bind(root);
            statement.executeQuery().close();

            assertEquals("SELECT r FROM t WHERE name = {_p1:String} AND n = {_p2:String}",
                    core.queried.get(0));
            QueryParameters params = core.queriedParams.get(0);
            assertEquals("alpha", params.wireValue("_p1"));
            assertEquals("42", params.wireValue("_p2"));
        }
    }

    @Test
    @DisplayName("a null cell declares its placeholder Nullable(String) and binds the \\N sentinel")
    void nullCellRewritesNullable(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "a", "b")) {
            VarCharVector a = (VarCharVector) root.getVector("a");
            a.setNull(0);
            ((VarCharVector) root.getVector("b")).setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);
            statement.setSqlQuery("SELECT r FROM t WHERE v = ? AND w = ?");
            statement.bind(root);
            statement.executeQuery().close();

            assertEquals("SELECT r FROM t WHERE v = {_p1:Nullable(String)} AND w = {_p2:String}",
                    core.queried.get(0));
            assertEquals("\\N", core.queriedParams.get(0).wireValue("_p1"),
                    "a null binding travels as the NULL sentinel");
        }
    }

    @Test
    @DisplayName("?s inside strings, comments and ternaries are not placeholders")
    void nonPlaceholderQuestionMarksIgnored(BufferAllocator allocator) throws Exception {
        // With no bindable ?s the statement has no parameters: it executes without a bound
        // root and the SQL passes through verbatim.
        String sql = "SELECT '?', /* ? */ if(1 > 0 ? 1 : 0, 'y', 'n') -- trailing ?";
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery(sql);
            statement.executeQuery().close();
            assertEquals(List.of(sql), core.queried);
            assertNull(core.queriedParams.get(0), "no parameters must travel");
        }
    }

    // ---- named passthrough ------------------------------------------------------------------

    @Test
    @DisplayName("{name:Type} placeholders pass through untouched and bind root fields by name")
    void namedParametersBindByName(BufferAllocator allocator) throws Exception {
        String sql = "SELECT r FROM t WHERE x = {x:Int32} AND s = {s:String}";
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        // Fields deliberately in the OPPOSITE order of the SQL tokens: name wins, not position.
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "s", "x")) {
            fillUtf8(root, 0, "hello", "7");
            statement.setSqlQuery(sql);
            statement.bind(root);
            statement.executeQuery().close();

            assertEquals(List.of(sql), core.queried, "named placeholders must not be rewritten");
            QueryParameters params = core.queriedParams.get(0);
            assertEquals("7", params.wireValue("x"));
            assertEquals("hello", params.wireValue("s"));
        }
    }

    @Test
    @DisplayName("a named parameter used twice in the SQL binds one value")
    void namedParameterUsedTwiceBindsOnce(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "n")) {
            fillUtf8(root, 0, "5");
            statement.setSqlQuery("SELECT {n:Int32} + {n:Int32}");
            statement.bind(root);
            statement.executeQuery().close();
            assertEquals("5", core.queriedParams.get(0).wireValue("n"));
        }
    }

    @Test
    @DisplayName("map-literal braces are not parameter tokens")
    void mapLiteralBracesAreNotParameters(BufferAllocator allocator) throws Exception {
        String sql = "SELECT {'k': 1} AS m";
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("m"), List.of("Map(String, Int32)")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery(sql);
            statement.executeQuery().close();
            assertEquals(List.of(sql), core.queried);
            assertNull(core.queriedParams.get(0));
        }
    }

    // ---- value conversion matrix ---------------------------------------------------------------

    @Test
    @DisplayName("typed Arrow cells convert to their ClickHouse wire text")
    void typedCellsConvertToWireText(BufferAllocator allocator) throws Exception {
        Schema schema = new Schema(List.of(
                Field.nullable("i", new ArrowType.Int(64, true)),
                Field.nullable("f", new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("b", new ArrowType.Bool()),
                Field.nullable("dec", new ArrowType.Decimal(10, 2, 128)),
                Field.nullable("d", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                Field.nullable("ts", new ArrowType.Timestamp(
                        org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC"))));
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ((BigIntVector) root.getVector("i")).setSafe(0, -42L);
            ((Float8Vector) root.getVector("f")).setSafe(0, 1.5);
            ((BitVector) root.getVector("b")).setSafe(0, 1);
            ((DecimalVector) root.getVector("dec")).setSafe(0, new BigDecimal("12345.67"));
            ((DateDayVector) root.getVector("d")).setSafe(0,
                    (int) java.time.LocalDate.of(2026, 5, 30).toEpochDay());
            ((TimeStampMicroTZVector) root.getVector("ts")).setSafe(0,
                    java.time.Instant.parse("2021-06-15T12:34:56.789123Z").getEpochSecond() * 1_000_000L
                            + 789_123L);
            root.setRowCount(1);

            statement.setSqlQuery("SELECT r FROM t WHERE a=? AND b=? AND c=? AND d=? AND e=? AND f=?");
            statement.bind(root);
            statement.executeQuery().close();

            QueryParameters params = core.queriedParams.get(0);
            assertEquals("-42", params.wireValue("_p1"));
            assertEquals("1.5", params.wireValue("_p2"));
            assertEquals("true", params.wireValue("_p3"));
            assertEquals("12345.67", params.wireValue("_p4"));
            assertEquals("2026-05-30", params.wireValue("_p5"));
            assertEquals("2021-06-15 12:34:56.789123000", params.wireValue("_p6"),
                    "sub-second instants keep their fraction");
        }
    }

    @Test
    @DisplayName("a FixedSizeBinary(16) cell with UUID/IPv6 metadata re-widens to its text form")
    void metadataDisambiguatesFixedBinaryParams(BufferAllocator allocator) throws Exception {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        Schema schema = new Schema(List.of(
                ClickHouseArrowTypes.arrowField("u", "UUID"),
                ClickHouseArrowTypes.arrowField("addr", "IPv6")));
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ((FixedSizeBinaryVector) root.getVector("u")).setSafe(0, ByteBuffer.allocate(16)
                    .putLong(uuid.getMostSignificantBits())
                    .putLong(uuid.getLeastSignificantBits()).array());
            ((FixedSizeBinaryVector) root.getVector("addr")).setSafe(0,
                    java.net.InetAddress.getByName("2001:db8::1").getAddress());
            root.setRowCount(1);

            statement.setSqlQuery("SELECT r FROM t WHERE id = ? AND a = ?");
            statement.bind(root);
            statement.executeQuery().close();

            QueryParameters params = core.queriedParams.get(0);
            assertEquals(uuid.toString(), params.wireValue("_p1"));
            assertEquals("2001:db8:0:0:0:0:0:1", params.wireValue("_p2"),
                    "InetAddress.getHostAddress canonical form");
        }
    }

    @Test
    @DisplayName("an IPv4 cell (unsigned-int vector with IPv4 metadata) re-widens to dotted-quad text")
    void ipv4UintParamRewidensToDottedQuad(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        Schema schema = new Schema(List.of(ClickHouseArrowTypes.arrowField("a", "IPv4")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ((org.apache.arrow.vector.UInt4Vector) root.getVector("a")).setSafe(0, 0xC0A80001);
            root.setRowCount(1);
            statement.setSqlQuery("SELECT r FROM t WHERE addr = ?");
            statement.bind(root);
            statement.executeQuery().close();
            assertEquals("192.168.0.1", core.queriedParams.get(0).wireValue("_p1"));
        }
    }

    @Test
    @DisplayName("a list cell renders as a ClickHouse array literal (IN-clause shape)")
    void listCellRendersArrayLiteral(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        Schema schema = new Schema(List.of(ClickHouseArrowTypes.arrowField("xs", "Array(Int32)")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            org.apache.arrow.vector.complex.ListVector xs =
                    (org.apache.arrow.vector.complex.ListVector) root.getVector("xs");
            org.apache.arrow.vector.complex.impl.UnionListWriter writer = xs.getWriter();
            writer.setPosition(0);
            writer.startList();
            writer.writeInt(1);
            writer.writeInt(2);
            writer.writeInt(3);
            writer.endList();
            xs.setValueCount(1);
            root.setRowCount(1);

            statement.setSqlQuery("SELECT r FROM t WHERE x IN ?");
            statement.bind(root);
            statement.executeQuery().close();
            assertEquals("[1,2,3]", core.queriedParams.get(0).wireValue("_p1"));
        }
    }

    @Test
    @DisplayName("Duration cells are rejected as parameters with NOT_IMPLEMENTED")
    void durationParamsNotImplemented(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        Schema schema = new Schema(List.of(ClickHouseArrowTypes.arrowField("t", "Time")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ((org.apache.arrow.vector.DurationVector) root.getVector("t")).setSafe(0, 90L);
            root.setRowCount(1);
            statement.setSqlQuery("SELECT r FROM t WHERE span = ?");
            statement.bind(root);
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    // ---- batch shape ------------------------------------------------------------------------------

    @Test
    @DisplayName("executeUpdate runs the statement once per parameter row, with per-row nullability")
    void executeUpdateLoopsParameterRows(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "v")) {
            VarCharVector v = (VarCharVector) root.getVector("v");
            v.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
            v.setNull(1);
            v.setSafe(2, "c".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(3);

            statement.setSqlQuery("INSERT INTO t VALUES (?)");
            statement.bind(root);
            assertEquals(-1, statement.executeUpdate().getAffectedRows());

            assertEquals(3, core.executed.size(), "one execution per parameter row");
            assertEquals("INSERT INTO t VALUES ({_p1:String})", core.executed.get(0));
            assertEquals("INSERT INTO t VALUES ({_p1:Nullable(String)})", core.executed.get(1),
                    "the null row's placeholder must be declared Nullable");
            assertEquals("a", core.executedParams.get(0).wireValue("_p1"));
            assertEquals("\\N", core.executedParams.get(1).wireValue("_p1"));
            assertEquals("c", core.executedParams.get(2).wireValue("_p1"));
        }
    }

    @Test
    @DisplayName("executeQuery rejects a multi-row parameter root with NOT_IMPLEMENTED")
    void executeQueryRejectsMultiRowRoot(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "v")) {
            fillUtf8(root, 0, "a");
            ((VarCharVector) root.getVector("v")).setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(2);
            statement.setSqlQuery("SELECT r FROM t WHERE v = ?");
            statement.bind(root);
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
            assertTrue(core.queried.isEmpty());
        }
    }

    @Test
    @DisplayName("re-binding a fresh root re-executes with the new values")
    void rebindExecutesFreshValues(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        core.enqueueResult(QueryResults.empty(List.of("r"), List.of("Int64")));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "v")) {
            statement.setSqlQuery("SELECT r FROM t WHERE v = ?");
            fillUtf8(root, 0, "first");
            statement.bind(root);
            statement.executeQuery().close();
            fillUtf8(root, 0, "second");
            statement.bind(root);
            statement.executeQuery().close();

            assertEquals("first", core.queriedParams.get(0).wireValue("_p1"));
            assertEquals("second", core.queriedParams.get(1).wireValue("_p1"));
        }
    }

    // ---- validation --------------------------------------------------------------------------------

    @Test
    @DisplayName("placeholders without a bound root raise INVALID_STATE before touching the wire")
    void unboundParametersRaiseInvalidState(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery("SELECT r FROM t WHERE v = ?");
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.INVALID_STATE, ex.getStatus());
            assertTrue(core.queried.isEmpty());
        }
    }

    @Test
    @DisplayName("a zero-row parameter root raises INVALID_STATE")
    void zeroRowRootRaisesInvalidState(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "v")) {
            root.setRowCount(0);
            statement.setSqlQuery("SELECT r FROM t WHERE v = ?");
            statement.bind(root);
            assertEquals(AdbcStatusCode.INVALID_STATE,
                    assertThrows(AdbcException.class, statement::executeQuery).getStatus());
        }
    }

    @Test
    @DisplayName("a field-count mismatch against ? placeholders raises INVALID_ARGUMENT")
    void positionalFieldCountMismatch(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "only")) {
            fillUtf8(root, 0, "x");
            statement.setSqlQuery("SELECT r FROM t WHERE a = ? AND b = ?");
            statement.bind(root);
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
            assertTrue(ex.getMessage().contains("2"), ex.getMessage());
        }
    }

    @Test
    @DisplayName("a missing named field raises INVALID_ARGUMENT naming it")
    void missingNamedFieldRaisesInvalidArgument(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator);
                AdbcStatement statement = connection.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "wrong")) {
            fillUtf8(root, 0, "x");
            statement.setSqlQuery("SELECT r FROM t WHERE v = {expected:String}");
            statement.bind(root);
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
            assertTrue(ex.getMessage().contains("expected"), ex.getMessage());
        }
    }

    // ---- getParameterSchema --------------------------------------------------------------------------

    @Test
    @DisplayName("getParameterSchema reports _pN nullable Utf8 fields for ? placeholders")
    void parameterSchemaPositional(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcStatement statement = connection.createStatement();
            statement.setSqlQuery("SELECT 1 FROM t WHERE a = ? AND b = ?");
            Schema schema = statement.getParameterSchema();
            assertEquals(List.of("_p1", "_p2"),
                    schema.getFields().stream().map(Field::getName).toList());
            for (Field field : schema.getFields()) {
                assertEquals(ArrowType.Utf8.INSTANCE, field.getType());
                assertTrue(field.isNullable());
            }
        }
    }

    @Test
    @DisplayName("getParameterSchema maps {name:Type} declarations through the type table")
    void parameterSchemaNamed(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcStatement statement = connection.createStatement();
            statement.setSqlQuery("SELECT 1 FROM t WHERE x = {x:Int32} AND s = {s:Nullable(String)}");
            Schema schema = statement.getParameterSchema();
            assertEquals(List.of("x", "s"),
                    schema.getFields().stream().map(Field::getName).toList());
            assertEquals(new ArrowType.Int(32, true), schema.getFields().get(0).getType());
            assertTrue(!schema.getFields().get(0).isNullable());
            assertTrue(schema.getFields().get(1).isNullable());
        }
    }

    @Test
    @DisplayName("getParameterSchema is empty for SQL without parameters")
    void parameterSchemaEmpty(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcStatement statement = connection.createStatement();
            statement.setSqlQuery("SELECT 1");
            assertEquals(0, statement.getParameterSchema().getFields().size());
        }
    }

    @Test
    @DisplayName("getParameterSchema without SQL raises INVALID_STATE; unknown types NOT_IMPLEMENTED")
    void parameterSchemaValidation(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            AdbcStatement statement = connection.createStatement();
            assertEquals(AdbcStatusCode.INVALID_STATE,
                    assertThrows(AdbcException.class, statement::getParameterSchema).getStatus());

            statement.setSqlQuery("SELECT 1 FROM t WHERE v = {v:TotallyBogusType}");
            assertEquals(AdbcStatusCode.NOT_IMPLEMENTED,
                    assertThrows(AdbcException.class, statement::getParameterSchema).getStatus());
        }
    }

    // ---- helpers ----------------------------------------------------------------------------------------

    /** A root of nullable Utf8 fields with the given names (no rows filled yet). */
    private static VectorSchemaRoot utf8Root(BufferAllocator allocator, String... names) {
        Schema schema = new Schema(java.util.Arrays.stream(names)
                .map(n -> new Field(n, FieldType.nullable(new ArrowType.Utf8()), null))
                .toList());
        return VectorSchemaRoot.create(schema, allocator);
    }

    /** Fills one row of string cells across the root's fields and sets rowCount to row+1. */
    private static void fillUtf8(VectorSchemaRoot root, int row, String... values) {
        for (int i = 0; i < values.length; i++) {
            ((VarCharVector) root.getFieldVectors().get(i))
                    .setSafe(row, values[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(row + 1);
    }
}
