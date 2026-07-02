package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.BoolCodec;
import io.github.danielbunting.clickhouse.types.codec.FixedStringCodec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Ipv4Codec;
import io.github.danielbunting.clickhouse.types.codec.Ipv6Codec;
import io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.MapColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.NullableColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.TupleColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UuidCodec;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Offline composite/reference-type coverage for the Block→Arrow bridge: Array (empty, nested,
 * nullable elements), Map (empty, array values), Tuple, LowCardinality(Nullable) null paths,
 * FixedString padding, IPv4-mapped-IPv6 normalisation, UUID byte order. The ADBC analogue of the
 * JDBC module's {@code ChResultSetComplexTypeTest}/{@code ChArrayTest}. Values are read back via
 * the production {@link ArrowToBlock#toJavaValue}: integer leaves surface as {@code Long}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class BlockToArrowComplexTest {

    // ---- Array ---------------------------------------------------------------------------------

    @Test
    @DisplayName("Array(Int32) round-trips values and the empty array")
    void arrayOfInts(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "Array(Int32)",
                new ArrayColumnCodec(new Int32Codec()),
                new Object[] {List.of(1, 2, 3), List.of()});
        assertEquals(List.of(1L, 2L, 3L), rows.get(0));
        assertEquals(List.of(), rows.get(1), "an empty array is empty, not null");
    }

    @Test
    @DisplayName("Array(Array(Int32)) nests element offsets correctly")
    void nestedArrays(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "Array(Array(Int32))",
                new ArrayColumnCodec(new ArrayColumnCodec(new Int32Codec())),
                new Object[] {List.of(List.of(1), List.of(2, 3)), List.of()});
        assertEquals(List.of(List.of(1L), List.of(2L, 3L)), rows.get(0));
        assertEquals(List.of(), rows.get(1));
    }

    @Test
    @DisplayName("Array(Nullable(Int32)) carries per-element nulls through the child validity bitmap")
    void arrayOfNullableElements(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "Array(Nullable(Int32))",
                new ArrayColumnCodec(new NullableColumnCodec(new Int32Codec())),
                new Object[] {Arrays.asList(1, null, 3)});
        assertEquals(Arrays.asList(1L, null, 3L), rows.get(0));
    }

    // ---- Map -----------------------------------------------------------------------------------
    // Map columns cannot be built offline: MapColumnCodec.set feeds the write-side `pending`
    // structure while get() reads the wire-side offsets/entries, so a set→get round trip yields
    // an empty map. Map coverage lives in the live equivalence suites (AdbcCoreEquivalenceIT
    // "map"; AdbcDataTypeExtrasIT mapWithArrayValues).

    // ---- Tuple ---------------------------------------------------------------------------------

    @Test
    @DisplayName("Tuple(Int32, String) surfaces as a positional struct")
    void tupleAsStruct(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "Tuple(Int32, String)",
                new TupleColumnCodec(
                        new ColumnCodec[] {new Int32Codec(), new StringColumnCodec()},
                        new String[] {null, null}),
                new Object[] {List.of(7, "x")});
        assertEquals(List.of(7L, "x"), rows.get(0));
    }

    @Test
    @DisplayName("Tuple containing an Array nests through struct children")
    void tupleWithArrayElement(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "Tuple(String, Array(Int32))",
                new TupleColumnCodec(
                        new ColumnCodec[] {new StringColumnCodec(), new ArrayColumnCodec(new Int32Codec())},
                        new String[] {null, null}),
                new Object[] {List.of("k", List.of(4, 5))});
        assertEquals(List.of("k", List.of(4L, 5L)), rows.get(0));
    }

    // ---- LowCardinality ------------------------------------------------------------------------

    @Test
    @DisplayName("LowCardinality(String) delivers plain strings (no dictionary encoding)")
    void lowCardinalityPlainStrings(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "LowCardinality(String)",
                new LowCardinalityColumnCodec(new StringColumnCodec()),
                new Object[] {"x", "y", "x"});
        assertEquals(List.of("x", "y", "x"), rows);
    }

    @Test
    @DisplayName("LowCardinality(Nullable(String)) nulls come from the dictionary, not a null-map")
    void lowCardinalityNullablePath(BufferAllocator allocator) {
        // This is the one column shape whose null-ness lives inside the codec's boxed value
        // (Column.nulls() is null), exercising BlockToArrow's isLowCardinalityNullable arm.
        List<?> rows = roundTripRows(allocator, "LowCardinality(Nullable(String))",
                new LowCardinalityColumnCodec(new NullableColumnCodec(new StringColumnCodec())),
                new Object[] {"a", null, "b"});
        assertEquals(Arrays.asList("a", null, "b"), rows);
    }

    // ---- fixed-width binary leaves ---------------------------------------------------------------

    @Test
    @DisplayName("FixedString(10) is right-padded with NULs to the declared width")
    void fixedStringPadding(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "FixedString(10)",
                new FixedStringCodec(10), new Object[] {"ABC"});
        byte[] bytes = (byte[]) rows.get(0);
        assertEquals(10, bytes.length, "the vector width is the declared N");
        assertEquals("ABC", new String(bytes, 0, 3, java.nio.charset.StandardCharsets.UTF_8));
        for (int i = 3; i < 10; i++) {
            assertEquals(0, bytes[i], "padding byte " + i + " must be NUL");
        }
    }

    @Test
    @DisplayName("UUID encodes big-endian: most-significant then least-significant long")
    void uuidByteOrder(BufferAllocator allocator) {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        List<?> rows = roundTripRows(allocator, "UUID", new UuidCodec(), new Object[] {uuid});
        ByteBuffer buf = ByteBuffer.wrap((byte[]) rows.get(0));
        assertEquals(uuid.getMostSignificantBits(), buf.getLong());
        assertEquals(uuid.getLeastSignificantBits(), buf.getLong());
    }

    @Test
    @DisplayName("an IPv4-mapped address in an IPv6 column re-widens to the 16-byte ::ffff: form")
    void ipv4MappedIpv6Normalised(BufferAllocator allocator) throws Exception {
        // toIPv6('1.2.3.4') decodes via the JDK to an Inet4Address (4 bytes); the bridge must
        // re-widen it so the FixedSizeBinary(16) vector always holds canonical 16-byte values.
        InetAddress v4 = InetAddress.getByName("1.2.3.4");
        List<?> rows = roundTripRows(allocator, "IPv6", new Ipv6Codec(), new Object[] {v4});
        byte[] bytes = (byte[]) rows.get(0);
        assertEquals(16, bytes.length);
        for (int i = 0; i < 10; i++) {
            assertEquals(0, bytes[i]);
        }
        assertEquals((byte) 0xFF, bytes[10]);
        assertEquals((byte) 0xFF, bytes[11]);
        assertEquals((byte) 1, bytes[12]);
        assertEquals((byte) 4, bytes[15]);
    }

    @Test
    @DisplayName("a native IPv6 address keeps its 16 bytes verbatim")
    void ipv6Verbatim(BufferAllocator allocator) throws Exception {
        InetAddress v6 = InetAddress.getByName("2001:db8::1");
        List<?> rows = roundTripRows(allocator, "IPv6", new Ipv6Codec(), new Object[] {v6});
        assertTrue(Arrays.equals(v6.getAddress(), (byte[]) rows.get(0)));
    }

    @Test
    @DisplayName("IPv4 surfaces as an unsigned 32-bit integer")
    void ipv4AsUnsignedInt(BufferAllocator allocator) throws Exception {
        InetAddress v4 = InetAddress.getByName("192.168.0.1");
        List<?> rows = roundTripRows(allocator, "IPv4", new Ipv4Codec(), new Object[] {v4});
        assertEquals(0xC0A80001L, rows.get(0), "192.168.0.1 as an unsigned int");
    }

    @Test
    @DisplayName("Bool columns surface as Java booleans")
    void boolColumn(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "Bool", new BoolCodec(),
                new Object[] {true, false});
        assertEquals(List.of(true, false), rows);
    }

    @Test
    @DisplayName("a Nullable scalar null cell yields a null Java value, not a filler")
    void nullableScalarNullCell(BufferAllocator allocator) {
        Schema schema = ClickHouseArrowTypes.schema(List.of("c"), List.of("Nullable(Int64)"));
        Block block = TestBlocks.blockOf(TestBlocks.int64Column(
                "c", new long[] {0, 42}, new boolean[] {true, false}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            assertNull(ArrowToBlock.toJavaValue(root.getVector("c"), 0));
            assertEquals(42L, ArrowToBlock.toJavaValue(root.getVector("c"), 1));
        }
    }

    // ---- bridge failure modes and remaining conversion arms -----------------------------------

    @Test
    @DisplayName("an Arrow vector family with no read-path conversion fails clearly (top-level)")
    void unsupportedTopLevelVectorFailsClearly(BufferAllocator allocator) {
        // Hand-build a root whose vector the bridge does not map (LargeVarChar is never
        // produced by ClickHouseArrowTypes), then drive fill() at it directly.
        Schema schema = new Schema(List.of(new org.apache.arrow.vector.types.pojo.Field(
                "c",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8()),
                null)));
        Block block = TestBlocks.blockOf(TestBlocks.stringColumn("c", new String[] {"x"}, null));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            UnsupportedOperationException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    UnsupportedOperationException.class, () -> BlockToArrow.fill(root, block));
            assertTrue(ex.getMessage().contains("c"), ex.getMessage());
        }
    }

    @Test
    @DisplayName("an unmapped vector nested inside a container fails clearly too")
    void unsupportedNestedVectorFailsClearly(BufferAllocator allocator) {
        org.apache.arrow.vector.types.pojo.Field child =
                new org.apache.arrow.vector.types.pojo.Field(
                        "item",
                        org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                                new org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8()),
                        null);
        Schema schema = new Schema(List.of(new org.apache.arrow.vector.types.pojo.Field(
                "a",
                org.apache.arrow.vector.types.pojo.FieldType.notNullable(
                        new org.apache.arrow.vector.types.pojo.ArrowType.List()),
                List.of(child))));
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "a", "Array(String)", new ArrayColumnCodec(new StringColumnCodec()),
                new Object[] {List.of("x")}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            org.junit.jupiter.api.Assertions.assertThrows(
                    UnsupportedOperationException.class, () -> BlockToArrow.fill(root, block));
        }
    }

    @Test
    @DisplayName("FixedString cells hold raw byte[] as well as String — and reject anything else")
    void fixedStringAcceptsBytesRejectsOthers(BufferAllocator allocator) {
        List<?> rows = roundTripRows(allocator, "FixedString(4)",
                new FixedStringCodec(4), new Object[] {new byte[] {1, 2, 3, 4}});
        assertTrue(Arrays.equals(new byte[] {1, 2, 3, 4}, (byte[]) rows.get(0)));

        Schema schema = ClickHouseArrowTypes.schema(List.of("c"), List.of("FixedString(4)"));
        Block bad = TestBlocks.blockOf(TestBlocks.column(
                "c", "FixedString(4)", new FixedStringCodec(4), new Object[] {42}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            org.junit.jupiter.api.Assertions.assertThrows(
                    UnsupportedOperationException.class, () -> BlockToArrow.fill(root, bad),
                    "a numeric cell has no FixedSizeBinary byte form");
        }
    }

    @Test
    @DisplayName("instants nested inside containers scale to micro/nano ticks exactly")
    void nestedInstantsScaleTicks(BufferAllocator allocator) {
        java.time.Instant instant = java.time.Instant.parse("2021-06-15T12:34:56.789123456Z");
        List<?> micro = roundTripRows(allocator, "Array(DateTime64(6, 'UTC'))",
                new ArrayColumnCodec(new io.github.danielbunting.clickhouse.types.codec.DateTime64Codec(
                        6, java.time.ZoneId.of("UTC"))),
                new Object[] {List.of(instant)});
        assertEquals(List.of(java.time.Instant.parse("2021-06-15T12:34:56.789123Z")),
                micro.get(0), "micro containers truncate to microseconds");

        List<?> nano = roundTripRows(allocator, "Array(DateTime64(9, 'UTC'))",
                new ArrayColumnCodec(new io.github.danielbunting.clickhouse.types.codec.DateTime64Codec(
                        9, java.time.ZoneId.of("UTC"))),
                new Object[] {List.of(instant)});
        assertEquals(List.of(instant), nano.get(0), "nano containers keep full resolution");
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Fills one column of {@code values} into Arrow and reads every row back as Java values. */
    private static List<?> roundTripRows(
            BufferAllocator allocator, String type, ColumnCodec<?> codec, Object[] values) {
        Schema schema = ClickHouseArrowTypes.schema(List.of("c"), List.of(type));
        Block block = TestBlocks.blockOf(TestBlocks.column("c", type, codec, values));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            java.util.ArrayList<Object> rows = new java.util.ArrayList<>(values.length);
            for (int r = 0; r < values.length; r++) {
                rows.add(ArrowToBlock.toJavaValue(root.getVector("c"), r));
            }
            return rows;
        }
    }
}
