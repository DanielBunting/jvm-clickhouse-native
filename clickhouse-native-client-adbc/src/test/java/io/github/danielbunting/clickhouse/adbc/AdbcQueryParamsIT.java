package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Parameterized queries against a live server, ported from the server-side halves of the JDBC
 * module's {@code JdbcPreparedStatementIT}/{@code JdbcPreparedStatementExtrasIT} plus the
 * previously-skipped parameter-binding issues (#1373 null-in-batch, #2327 UUID + IN clause,
 * #402 INSERT…SELECT with params). Values travel as server-side parameters — assertions include
 * the injection-shaped payloads that would break under client-side interpolation.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcQueryParamsIT extends AdbcRoundTripBase {

    // ---- positional ? ----------------------------------------------------------------------

    @Test
    @DisplayName("positional params filter scalar columns (WHERE name = ? AND n > ?)")
    void positionalWhereFilter(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_qp_where");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (id Int64, name String) ENGINE = Memory");
            try {
                execDdl(conn, "INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'b')");
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = utf8Root(allocator, "p1", "p2")) {
                    fillUtf8Row(root, "b", "1");
                    statement.setSqlQuery(
                            "SELECT id FROM " + table + " WHERE name = ? AND id > ? ORDER BY id");
                    statement.bind(root);
                    assertEquals(List.of(List.of(2L), List.of(3L)), readRows(statement));
                }
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("injection-shaped values stay data: quotes, \\, unicode, and placeholder look-alikes")
    void injectionShapedValuesStayData(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_qp_inject");
        String[] payloads = {
                "it's", "back\\slash", "'; DROP TABLE students; --",
                "{p:Int32}", "?", "héllo 世界", "line\nbreak",
        };
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (id Int64, v String) ENGINE = Memory");
            try {
                // Write every payload through a positional parameter batch...
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = utf8Root(allocator, "id", "v")) {
                    VarCharVector id = (VarCharVector) root.getVector("id");
                    VarCharVector v = (VarCharVector) root.getVector("v");
                    for (int i = 0; i < payloads.length; i++) {
                        id.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                        v.setSafe(i, payloads[i].getBytes(StandardCharsets.UTF_8));
                    }
                    root.setRowCount(payloads.length);
                    statement.setSqlQuery("INSERT INTO " + table + " VALUES (?, ?)");
                    statement.bind(root);
                    statement.executeUpdate();
                }
                // ...and read each back through an equality parameter.
                for (int i = 0; i < payloads.length; i++) {
                    try (AdbcStatement statement = conn.createStatement();
                            VectorSchemaRoot root = utf8Root(allocator, "v")) {
                        fillUtf8Row(root, payloads[i]);
                        statement.setSqlQuery("SELECT id FROM " + table + " WHERE v = ?");
                        statement.bind(root);
                        assertEquals(List.of(List.of((long) i)), readRows(statement),
                                "payload must round trip as pure data: " + payloads[i]);
                    }
                }
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("adversarial payloads arrive byte-exact via a PARAMETERLESS read-back")
    void adversarialPayloadsArriveByteExact(BufferAllocator allocator) throws Exception {
        // Regression proof for the QueryParameters ESCAPED-form fix, asymmetric on purpose:
        // the write side binds parameters, the read side is plain SQL — so a symmetric
        // encode/decode bug cannot cancel out (the WHERE-equality test above could pass even
        // if both sides corrupted identically). Before the fix "a\b" arrived as a + backspace
        // (silent corruption) and the newline payload failed with BAD_QUERY_PARAMETER (457).
        String table = uniqueTable("adbc_qp_exact");
        String[] payloads = {
                "a\\b", "c:\\path\\to\\file", "trailing\\", "line\nbreak",
                "tab\there", "cr\rreturn", "nul\u0000byte", "it's",
                "quote\"double", "\\N", "{p:String}", "héllo 世界",
        };
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (id Int64, v String) ENGINE = Memory");
            try {
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = utf8Root(allocator, "id", "v")) {
                    VarCharVector id = (VarCharVector) root.getVector("id");
                    VarCharVector v = (VarCharVector) root.getVector("v");
                    for (int i = 0; i < payloads.length; i++) {
                        id.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                        v.setSafe(i, payloads[i].getBytes(StandardCharsets.UTF_8));
                    }
                    root.setRowCount(payloads.length);
                    statement.setSqlQuery("INSERT INTO " + table + " VALUES (?, ?)");
                    statement.bind(root);
                    statement.executeUpdate();
                }

                List<List<Object>> read = rows(conn,
                        "SELECT v, length(v) FROM " + table + " ORDER BY id");
                assertEquals(payloads.length, read.size());
                for (int i = 0; i < payloads.length; i++) {
                    assertEquals(payloads[i], read.get(i).get(0),
                            "row " + i + " must arrive byte-exact");
                    assertEquals((long) payloads[i].getBytes(StandardCharsets.UTF_8).length,
                            read.get(i).get(1), "server-side length of row " + i);
                }

                // The literal 2-char string \N must not have collapsed into SQL NULL.
                assertEquals(List.of(List.of(1L)),
                        rows(conn, "SELECT count() FROM " + table + " WHERE v = '\\\\N'"));
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("#1373: a null inside a parameter batch inserts SQL NULL, not a filler")
    void issue1373NullInBatch(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_qp_1373");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (id Int64, v Nullable(String)) ENGINE = Memory");
            try {
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = utf8Root(allocator, "id", "v")) {
                    VarCharVector id = (VarCharVector) root.getVector("id");
                    VarCharVector v = (VarCharVector) root.getVector("v");
                    id.setSafe(0, "1".getBytes(StandardCharsets.UTF_8));
                    v.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
                    id.setSafe(1, "2".getBytes(StandardCharsets.UTF_8));
                    v.setNull(1);
                    id.setSafe(2, "3".getBytes(StandardCharsets.UTF_8));
                    v.setSafe(2, "z".getBytes(StandardCharsets.UTF_8));
                    root.setRowCount(3);
                    statement.setSqlQuery("INSERT INTO " + table + " VALUES (?, ?)");
                    statement.bind(root);
                    statement.executeUpdate();
                }
                assertEquals(
                        List.of(List.of("x"), Arrays.asList((Object) null), List.of("z")),
                        rows(conn, "SELECT v FROM " + table + " ORDER BY id"));
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("#402: INSERT … SELECT ?, ? routes parameters outside a VALUES list")
    void issue402InsertSelectWithParams(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_qp_402");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (uid Int32, name String) ENGINE = Memory");
            try {
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = utf8Root(allocator, "uid", "name")) {
                    fillUtf8Row(root, "7", "seven");
                    statement.setSqlQuery("INSERT INTO " + table + " SELECT ?, ?");
                    statement.bind(root);
                    statement.executeUpdate();
                }
                assertEquals(List.of(List.of(7L, "seven")),
                        rows(conn, "SELECT uid, name FROM " + table));
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("#2327: a UUID parameter filters by equality and via IN over an Array(UUID) param")
    void issue2327UuidParamAndInClause(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_qp_2327");
        UUID target = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        UUID other = UUID.fromString("2d1f626d-eb07-4c81-be3d-ac1173f0d018");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (id UUID, n Int64) ENGINE = Memory");
            try {
                execDdl(conn, "INSERT INTO " + table + " VALUES ('" + target + "', 1), ('" + other + "', 2)");

                // Equality via a metadata-carrying FixedSizeBinary(16) parameter.
                Schema uuidParam = new Schema(List.of(ClickHouseArrowTypes.arrowField("u", "UUID")));
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = VectorSchemaRoot.create(uuidParam, allocator)) {
                    ((FixedSizeBinaryVector) root.getVector("u")).setSafe(0, ByteBuffer.allocate(16)
                            .putLong(target.getMostSignificantBits())
                            .putLong(target.getLeastSignificantBits()).array());
                    root.setRowCount(1);
                    statement.setSqlQuery("SELECT n FROM " + table + " WHERE id = {u:UUID}");
                    statement.bind(root);
                    assertEquals(List.of(List.of(1L)), readRows(statement));
                }

                // IN over a typed Array(UUID) parameter carrying uuid strings.
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = stringListRoot(allocator, "ids",
                                target.toString(), other.toString())) {
                    statement.setSqlQuery(
                            "SELECT n FROM " + table + " WHERE id IN {ids:Array(UUID)} ORDER BY n");
                    statement.bind(root);
                    assertEquals(List.of(List.of(1L), List.of(2L)), readRows(statement));
                }
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- named {name:Type} -----------------------------------------------------------------------

    @Test
    @DisplayName("a {ts:DateTime64(6, 'UTC')} parameter filters with microsecond fidelity")
    void namedDateTime64Param(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_qp_dt64");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            execDdl(conn, "CREATE TABLE " + table + " (id Int64, ts DateTime64(6, 'UTC')) ENGINE = Memory");
            try {
                execDdl(conn, "INSERT INTO " + table + " VALUES "
                        + "(1, '2021-06-15 12:34:56.789123'), (2, '2021-06-15 12:34:56.789124')");
                Schema schema = new Schema(List.of(
                        ClickHouseArrowTypes.arrowField("ts", "DateTime64(6, 'UTC')")));
                try (AdbcStatement statement = conn.createStatement();
                        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    ((org.apache.arrow.vector.TimeStampMicroTZVector) root.getVector("ts")).setSafe(0,
                            java.time.Instant.parse("2021-06-15T12:34:56.789123Z").getEpochSecond()
                                    * 1_000_000L + 789_123L);
                    root.setRowCount(1);
                    statement.setSqlQuery("SELECT id FROM " + table + " WHERE ts = {ts:DateTime64(6, 'UTC')}");
                    statement.bind(root);
                    assertEquals(List.of(List.of(1L)), readRows(statement),
                            "one microsecond apart must distinguish the rows");
                }
            } finally {
                execDdl(conn, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("Array and Map parameters bind with their declared types")
    void namedCompositeParams(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection conn = database.connect()) {
            // Array(Int32) IN filter.
            Schema arrSchema = new Schema(List.of(ClickHouseArrowTypes.arrowField("xs", "Array(Int32)")));
            try (AdbcStatement statement = conn.createStatement();
                    VectorSchemaRoot root = VectorSchemaRoot.create(arrSchema, allocator)) {
                ListVector xs = (ListVector) root.getVector("xs");
                UnionListWriter writer = xs.getWriter();
                writer.setPosition(0);
                writer.startList();
                writer.writeInt(1);
                writer.writeInt(3);
                writer.endList();
                xs.setValueCount(1);
                root.setRowCount(1);
                statement.setSqlQuery(
                        "SELECT number FROM numbers(5) WHERE number IN {xs:Array(Int32)} ORDER BY number");
                statement.bind(root);
                assertEquals(List.of(List.of(1L), List.of(3L)), readRows(statement));
            }

            // Map(String, Int32): the map literal text travels through a typed named
            // parameter and the server materialises the same value as the map() literal.
            try (AdbcStatement statement = conn.createStatement();
                    VectorSchemaRoot root = utf8Root(allocator, "m")) {
                fillUtf8Row(root, "{'a':1,'b':2}");
                statement.setSqlQuery("SELECT {m:Map(String, Int32)} AS m");
                statement.bind(root);
                assertEquals(viaCore(core, "SELECT map('a', 1, 'b', 2) AS m"), readRows(statement));
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a parameter drives a table-function argument")
    void tableFunctionArgumentParam(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "n")) {
            fillUtf8Row(root, "7");
            statement.setSqlQuery("SELECT count() AS c FROM numbers({n:UInt64})");
            statement.bind(root);
            assertEquals(List.of(List.of(7L)), readRows(statement));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("parameters work inside a CTE and alongside a SETTINGS clause")
    void cteAndSettingsWithParams(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect()) {
            try (AdbcStatement statement = conn.createStatement();
                    VectorSchemaRoot root = utf8Root(allocator, "v")) {
                fillUtf8Row(root, "41");
                statement.setSqlQuery(
                        "WITH base AS (SELECT {v:Int64} AS v) SELECT v + 1 FROM base SETTINGS max_threads = 1");
                statement.bind(root);
                assertEquals(List.of(List.of(42L)), readRows(statement));
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a value the server cannot cast to the declared type fails as IO with a server code")
    void uncastableValueFailsCleanly(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "n")) {
            fillUtf8Row(root, "not-a-number");
            statement.setSqlQuery("SELECT {n:UInt8} AS n");
            statement.bind(root);
            AdbcException ex = assertThrows(AdbcException.class, statement::executeQuery);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
            assertNotEquals(0, ex.getVendorCode(), "the server's cast error code must surface");
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("the same statement re-executes with fresh bindings (reuse shape of #2299)")
    void statementReusableWithFreshBindings(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement();
                VectorSchemaRoot root = utf8Root(allocator, "v")) {
            statement.setSqlQuery("SELECT {v:Int64} * 2 AS r");
            fillUtf8Row(root, "10");
            statement.bind(root);
            assertEquals(List.of(List.of(20L)), readRows(statement));
            fillUtf8Row(root, "21");
            statement.bind(root);
            assertEquals(List.of(List.of(42L)), readRows(statement));
        } finally {
            database.close();
        }
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }

    /** Executes the bound statement and drains it into canonicalized rows. */
    private static List<List<Object>> readRows(AdbcStatement statement) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        try (AdbcStatement.QueryResult result = statement.executeQuery()) {
            ArrowReader reader = result.getReader();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                for (int r = 0; r < root.getRowCount(); r++) {
                    List<Object> row = new ArrayList<>();
                    for (FieldVector vector : root.getFieldVectors()) {
                        row.add(Canonicalizer.canonical(ArrowToBlock.toJavaValue(vector, r)));
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    /** Runs unparameterized SQL on a fresh statement and drains canonicalized rows. */
    private static List<List<Object>> rows(AdbcConnection conn, String sql) throws Exception {
        try (AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery(sql);
            return readRows(statement);
        }
    }

    /** A root of nullable Utf8 fields with the given names. */
    private static VectorSchemaRoot utf8Root(BufferAllocator allocator, String... names) {
        Schema schema = new Schema(java.util.Arrays.stream(names)
                .map(n -> new Field(n, FieldType.nullable(new ArrowType.Utf8()), null))
                .toList());
        return VectorSchemaRoot.create(schema, allocator);
    }

    /** Fills row 0 with string cells and sets rowCount = 1. */
    private static void fillUtf8Row(VectorSchemaRoot root, String... values) {
        for (int i = 0; i < values.length; i++) {
            ((VarCharVector) root.getFieldVectors().get(i))
                    .setSafe(0, values[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(1);
    }

    /** A one-row root with a single Utf8-list field carrying the given strings. */
    private static VectorSchemaRoot stringListRoot(
            BufferAllocator allocator, String name, String... values) {
        Field child = new Field("item", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field list = new Field(name, FieldType.notNullable(new ArrowType.List()), List.of(child));
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(list)), allocator);
        ListVector vector = (ListVector) root.getVector(name);
        UnionListWriter writer = vector.getWriter();
        writer.setPosition(0);
        writer.startList();
        for (String value : values) {
            writer.writeVarChar(new org.apache.arrow.vector.util.Text(value));
        }
        writer.endList();
        vector.setValueCount(1);
        root.setRowCount(1);
        return root;
    }
}
