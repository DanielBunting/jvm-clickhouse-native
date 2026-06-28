package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.protocol.Block;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Slice 1: the offline Block/Column → Arrow bridge and the type-mapping table. */
@ExtendWith(ArrowAllocatorExtension.class)
class BlockToArrowTest {

    @Test
    void mixedPrimitiveStringAndNullableColumns(BufferAllocator allocator) {
        Block block = TestBlocks.blockOf(
                TestBlocks.int64Column("id", new long[] {10, 20, 30}, new boolean[] {false, true, false}),
                TestBlocks.float64Column("v", new double[] {1.5, 2.5, 3.5}),
                TestBlocks.stringColumn("s", new String[] {"a", "", "c"}, new boolean[] {false, true, false}));

        Schema schema = ClickHouseArrowTypes.schema(
                List.of("id", "v", "s"),
                List.of("Nullable(Int64)", "Float64", "Nullable(String)"));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);

            assertEquals(3, root.getRowCount());

            BigIntVector id = (BigIntVector) root.getVector("id");
            assertFalse(id.isNull(0));
            assertEquals(10L, id.get(0));
            assertTrue(id.isNull(1), "row 1 should be null from the null-map");
            assertEquals(30L, id.get(2));

            Float8Vector v = (Float8Vector) root.getVector("v");
            assertEquals(1.5, v.get(0));
            assertEquals(3.5, v.get(2));

            VarCharVector s = (VarCharVector) root.getVector("s");
            assertEquals("a", new String(s.get(0), StandardCharsets.UTF_8));
            assertTrue(s.isNull(1));
            assertEquals("c", new String(s.get(2), StandardCharsets.UTF_8));
        }
    }

    @Test
    void refillReusesVectorsAcrossBlocks(BufferAllocator allocator) {
        Schema schema = ClickHouseArrowTypes.schema(List.of("id"), List.of("Int64"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, TestBlocks.blockOf(
                    TestBlocks.int64Column("id", new long[] {1, 2, 3, 4}, null)));
            assertEquals(4, root.getRowCount());

            // A second, shorter block must not leave stale rows behind.
            BlockToArrow.fill(root, TestBlocks.blockOf(
                    TestBlocks.int64Column("id", new long[] {7}, null)));
            assertEquals(1, root.getRowCount());
            assertEquals(7L, ((BigIntVector) root.getVector("id")).get(0));
        }
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("typeMappingRows")
    void typeMappingTable(String clickHouseType, ArrowType expected, boolean nullable) {
        Field field = ClickHouseArrowTypes.arrowField("c", clickHouseType);
        assertEquals(expected, field.getType());
        assertEquals(nullable, field.isNullable(), "nullability of " + clickHouseType);
    }

    static java.util.stream.Stream<Arguments> typeMappingRows() {
        return java.util.stream.Stream.of(
                Arguments.of("Int8", new ArrowType.Int(8, true), false),
                Arguments.of("Int16", new ArrowType.Int(16, true), false),
                Arguments.of("Int32", new ArrowType.Int(32, true), false),
                Arguments.of("Int64", new ArrowType.Int(64, true), false),
                Arguments.of("UInt8", new ArrowType.Int(8, false), false),
                Arguments.of("UInt16", new ArrowType.Int(16, false), false),
                Arguments.of("UInt32", new ArrowType.Int(32, false), false),
                Arguments.of("UInt64", new ArrowType.Int(64, false), false),
                Arguments.of("Float32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false),
                Arguments.of("Float64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), false),
                Arguments.of("Bool", new ArrowType.Bool(), false),
                Arguments.of("String", new ArrowType.Utf8(), false),
                Arguments.of("FixedString(5)", new ArrowType.FixedSizeBinary(5), false),
                Arguments.of("Date", new ArrowType.Date(DateUnit.DAY), false),
                Arguments.of("Date32", new ArrowType.Date(DateUnit.DAY), false),
                Arguments.of("DateTime", new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), false),
                Arguments.of("DateTime64(3)", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), false),
                Arguments.of("DateTime64(6)", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), false),
                Arguments.of("Decimal(10, 2)", new ArrowType.Decimal(10, 2, 128), false),
                Arguments.of("Decimal(40, 2)", new ArrowType.Decimal(40, 2, 256), false),
                Arguments.of("UUID", new ArrowType.FixedSizeBinary(16), false),
                Arguments.of("IPv4", new ArrowType.Int(32, false), false),
                Arguments.of("IPv6", new ArrowType.FixedSizeBinary(16), false),
                Arguments.of("Enum8('a' = 1, 'b' = 2)", new ArrowType.Int(8, true), false),
                Arguments.of("Enum16('a' = 1)", new ArrowType.Int(16, true), false),
                Arguments.of("LowCardinality(String)", new ArrowType.Utf8(), false),
                Arguments.of("Nullable(Int32)", new ArrowType.Int(32, true), true),
                Arguments.of("LowCardinality(Nullable(String))", new ArrowType.Utf8(), true),
                Arguments.of("Array(Int32)", new ArrowType.List(), false),
                Arguments.of("Map(String, Int32)", new ArrowType.Map(false), false),
                Arguments.of("Tuple(Int32, String)", new ArrowType.Struct(), false));
    }

    @Test
    void arrayFieldHasTypedChild() {
        Field field = ClickHouseArrowTypes.arrowField("a", "Array(Nullable(Int32))");
        assertEquals(new ArrowType.List(), field.getType());
        assertEquals(1, field.getChildren().size());
        Field child = field.getChildren().get(0);
        assertEquals(new ArrowType.Int(32, true), child.getType());
        assertTrue(child.isNullable(), "Array(Nullable(Int32)) element must be nullable");
    }

    @Test
    void mapFieldHasKeyValueStruct() {
        Field field = ClickHouseArrowTypes.arrowField("m", "Map(String, Int32)");
        assertEquals(new ArrowType.Map(false), field.getType());
        Field entries = field.getChildren().get(0);
        assertEquals(new ArrowType.Struct(), entries.getType());
        assertEquals("key", entries.getChildren().get(0).getName());
        assertEquals(new ArrowType.Utf8(), entries.getChildren().get(0).getType());
        assertEquals("value", entries.getChildren().get(1).getName());
        assertEquals(new ArrowType.Int(32, true), entries.getChildren().get(1).getType());
    }

    @Test
    void tupleFieldHasPositionalStructChildren() {
        Field field = ClickHouseArrowTypes.arrowField("t", "Tuple(Int32, String)");
        assertEquals(new ArrowType.Struct(), field.getType());
        assertArrayEquals(
                new String[] {"f0", "f1"},
                field.getChildren().stream().map(Field::getName).toArray());
    }
}
