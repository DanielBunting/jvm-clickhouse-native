package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Dedicated assertions for ClickHouse types whose Arrow representation is a deliberate choice
 * (not pinned by the core's own tests): UInt64 raw bits, UUID/IPv4/IPv6 binary forms,
 * FixedString padding, Enum-as-integer, and DateTime64 unit/zone. The generic equivalence
 * suite ({@link AdbcCoreEquivalenceIT}) deliberately excludes these.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcTypeRepresentationIT extends AdbcIntegrationTest {

    @FunctionalInterface
    private interface RootCheck {
        void check(VectorSchemaRoot root) throws Exception;
    }

    private void firstRow(BufferAllocator allocator, String sql, RootCheck check) throws Exception {
        firstRow(allocator, List.of(), sql, check);
    }

    /**
     * Runs each {@code setup} statement (e.g. {@code SET allow_experimental_*}) on the same native
     * session before the {@code sql} SELECT — ADBC binds one connection to one native session, so the
     * settings persist for the query — then checks the first batch's vectors.
     */
    private void firstRow(BufferAllocator allocator, List<String> setup, String sql, RootCheck check)
            throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            for (String s : setup) {
                try (AdbcStatement st = connection.createStatement()) {
                    st.setSqlQuery(s);
                    st.executeUpdate();
                }
            }
            try (AdbcStatement statement = connection.createStatement()) {
                statement.setSqlQuery(sql);
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    ArrowReader reader = result.getReader();
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    assertTrue(reader.loadNextBatch(), "expected a row for: " + sql);
                    check.check(root);
                }
            }
        } finally {
            database.close();
        }
    }

    private static String str(FieldVector v) {
        return new String(((VarCharVector) v).get(0), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Int128 / UInt128 → Utf8 decimal string")
    void wide128AsDecimalString(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toInt128('170141183460469231731687303715884105727') AS w", root -> {
            FieldVector v = root.getVector("w");
            assertInstanceOf(VarCharVector.class, v);
            assertEquals(new ArrowType.Utf8(), v.getField().getType());
            assertEquals("170141183460469231731687303715884105727", str(v));
        });
        firstRow(allocator, "SELECT toUInt128('340282366920938463463374607431768211455') AS w", root ->
                assertEquals("340282366920938463463374607431768211455", str(root.getVector("w"))));
    }

    @Test
    @DisplayName("Int256 / UInt256 → Utf8 decimal string")
    void wide256AsDecimalString(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toInt256('-12345678901234567890123456789') AS w", root -> {
            assertInstanceOf(VarCharVector.class, root.getVector("w"));
            assertEquals("-12345678901234567890123456789", str(root.getVector("w")));
        });
        firstRow(allocator, "SELECT toUInt256('98765432109876543210987654321') AS w", root ->
                assertEquals("98765432109876543210987654321", str(root.getVector("w"))));
    }

    @Test
    @DisplayName("BFloat16 → Float32")
    void bfloat16AsFloat(BufferAllocator allocator) throws Exception {
        firstRow(allocator, List.of("SET allow_experimental_bfloat16_type = 1"),
                "SELECT CAST(1.5, 'BFloat16') AS b", root -> {
                    FieldVector v = root.getVector("b");
                    assertInstanceOf(Float4Vector.class, v);
                    assertEquals(1.5f, ((Float4Vector) v).get(0));
                });
    }

    @Test
    @DisplayName("JSON → Utf8")
    void jsonAsString(BufferAllocator allocator) throws Exception {
        firstRow(allocator, List.of("SET allow_experimental_json_type = 1"),
                "SELECT CAST('{\"a\":1}', 'JSON') AS j", root -> {
                    FieldVector v = root.getVector("j");
                    assertInstanceOf(VarCharVector.class, v);
                    assertEquals(new ArrowType.Utf8(), v.getField().getType());
                    assertTrue(str(v).contains("\"a\""), "JSON string should carry path 'a': " + str(v));
                });
    }

    @Test
    @DisplayName("Dynamic → Utf8 (value rendering)")
    void dynamicAsString(BufferAllocator allocator) throws Exception {
        firstRow(allocator, List.of("SET allow_experimental_dynamic_type = 1"),
                "SELECT CAST(42, 'Dynamic') AS d", root -> {
                    assertInstanceOf(VarCharVector.class, root.getVector("d"));
                    assertEquals("42", str(root.getVector("d")));
                });
    }

    @Test
    @DisplayName("Variant → Utf8 (value rendering)")
    void variantAsString(BufferAllocator allocator) throws Exception {
        firstRow(allocator, List.of("SET allow_experimental_variant_type = 1"),
                "SELECT CAST('hello', 'Variant(UInt64, String)') AS v", root -> {
                    assertInstanceOf(VarCharVector.class, root.getVector("v"));
                    assertEquals("hello", str(root.getVector("v")));
                });
    }

    @Test
    @DisplayName("Time → Duration(SECOND)")
    void timeAsDuration(BufferAllocator allocator) throws Exception {
        firstRow(allocator, List.of("SET enable_time_time64_type = 1"),
                "SELECT CAST('12:30:45', 'Time') AS t", root -> {
                    FieldVector v = root.getVector("t");
                    assertInstanceOf(DurationVector.class, v);
                    assertEquals(
                            Duration.ofHours(12).plusMinutes(30).plusSeconds(45),
                            ((DurationVector) v).getObject(0));
                });
    }

    @Test
    @DisplayName("Time64(3) → Duration(MILLISECOND)")
    void time64AsDuration(BufferAllocator allocator) throws Exception {
        firstRow(allocator, List.of("SET enable_time_time64_type = 1"),
                "SELECT CAST('12:30:45.123', 'Time64(3)') AS t", root -> {
                    FieldVector v = root.getVector("t");
                    assertInstanceOf(DurationVector.class, v);
                    assertEquals(
                            Duration.ofHours(12).plusMinutes(30).plusSeconds(45).plusMillis(123),
                            ((DurationVector) v).getObject(0));
                });
    }

    @Test
    @DisplayName("Calendar Interval → Interval(YEAR_MONTH) carrying total months")
    void intervalMonthAsYearMonth(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT INTERVAL 3 MONTH AS i", root -> {
            FieldVector v = root.getVector("i");
            assertInstanceOf(IntervalYearVector.class, v);
            assertEquals(3, ((IntervalYearVector) v).get(0));
        });
        // A calendar Interval expressed in years still lands as total months.
        firstRow(allocator, "SELECT INTERVAL 2 YEAR AS i", root ->
                assertEquals(24, ((IntervalYearVector) root.getVector("i")).get(0)));
    }

    @Test
    @DisplayName("Time Interval → Duration")
    void intervalSecondAsDuration(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT INTERVAL 5 SECOND AS i", root -> {
            FieldVector v = root.getVector("i");
            assertInstanceOf(DurationVector.class, v);
            assertEquals(Duration.ofSeconds(5), ((DurationVector) v).getObject(0));
        });
    }

    @Test
    @DisplayName("Array(Nothing) (empty array) → List with a Null child vector")
    void nothingAsNullChild(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT [] AS n", root -> {
            FieldVector v = root.getVector("n");
            assertInstanceOf(ListVector.class, v);
            assertInstanceOf(NullVector.class, ((ListVector) v).getDataVector());
        });
    }

    @Test
    @DisplayName("UInt64 surfaces as an unsigned 64-bit Int carrying raw bits")
    void uint64RawBits(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toUInt64(18446744073709551615) AS u", root -> {
            FieldVector v = root.getVector("u");
            assertInstanceOf(UInt8Vector.class, v);
            assertEquals(new ArrowType.Int(64, false), v.getField().getType());
            // 2^64 - 1 carried as raw bits is -1 read back as a signed long.
            assertEquals(-1L, ((UInt8Vector) v).get(0));
        });
    }

    @Test
    @DisplayName("UUID → FixedSizeBinary(16), big-endian")
    void uuidFixedBinary(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toUUID('00112233-4455-6677-8899-aabbccddeeff') AS id", root -> {
            FieldVector v = root.getVector("id");
            assertInstanceOf(FixedSizeBinaryVector.class, v);
            assertEquals(new ArrowType.FixedSizeBinary(16), v.getField().getType());
            assertArrayEquals(
                    new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                            (byte) 0x88, (byte) 0x99, (byte) 0xaa, (byte) 0xbb,
                            (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff},
                    ((FixedSizeBinaryVector) v).get(0));
        });
    }

    @Test
    @DisplayName("IPv4 → unsigned Int(32) in network byte order")
    void ipv4AsUnsignedInt(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toIPv4('1.2.3.4') AS ip", root -> {
            FieldVector v = root.getVector("ip");
            assertInstanceOf(UInt4Vector.class, v);
            assertEquals(new ArrowType.Int(32, false), v.getField().getType());
            assertEquals(0x01020304, ((UInt4Vector) v).get(0));
        });
    }

    @Test
    @DisplayName("IPv6 → FixedSizeBinary(16)")
    void ipv6FixedBinary(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toIPv6('::1') AS ip", root -> {
            FieldVector v = root.getVector("ip");
            assertInstanceOf(FixedSizeBinaryVector.class, v);
            byte[] bytes = ((FixedSizeBinaryVector) v).get(0);
            assertEquals(16, bytes.length);
            assertEquals(1, bytes[15]);
        });
    }

    @Test
    @DisplayName("FixedString(N) → FixedSizeBinary(N), NUL-padded")
    void fixedStringPadded(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toFixedString('ab', 4) AS f", root -> {
            FieldVector v = root.getVector("f");
            assertInstanceOf(FixedSizeBinaryVector.class, v);
            assertEquals(new ArrowType.FixedSizeBinary(4), v.getField().getType());
            assertArrayEquals(new byte[] {'a', 'b', 0, 0}, ((FixedSizeBinaryVector) v).get(0));
        });
    }

    @Test
    @DisplayName("Enum8 → signed Int(8) carrying the underlying value")
    void enum8AsInt(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT CAST('b', 'Enum8(''a'' = 1, ''b'' = 2)') AS e", root -> {
            FieldVector v = root.getVector("e");
            assertInstanceOf(TinyIntVector.class, v);
            assertEquals(new ArrowType.Int(8, true), v.getField().getType());
            assertEquals((byte) 2, ((TinyIntVector) v).get(0));
        });
    }

    @Test
    @DisplayName("DateTime64(9, 'UTC') → Timestamp(NANO, 'UTC')")
    void dateTime64NanoZone(BufferAllocator allocator) throws Exception {
        firstRow(allocator, "SELECT toDateTime64('2021-06-15 12:34:56.123456789', 9, 'UTC') AS dt", root -> {
            FieldVector v = root.getVector("dt");
            assertInstanceOf(TimeStampNanoTZVector.class, v);
            ArrowType.Timestamp type = (ArrowType.Timestamp) v.getField().getType();
            assertEquals("UTC", type.getTimezone());
            // 12:34:56.123456789 UTC on 2021-06-15 in epoch nanoseconds.
            assertEquals(1623760496123456789L, ((TimeStampNanoTZVector) v).get(0));
        });
    }
}
