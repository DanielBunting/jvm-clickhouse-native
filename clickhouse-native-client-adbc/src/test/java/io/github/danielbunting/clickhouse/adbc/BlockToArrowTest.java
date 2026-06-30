package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.codec.BFloat16Codec;
import io.github.danielbunting.clickhouse.types.codec.Int128Codec;
import io.github.danielbunting.clickhouse.types.codec.IntervalCodec;
import io.github.danielbunting.clickhouse.types.codec.NothingCodec;
import io.github.danielbunting.clickhouse.types.codec.Time64Codec;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
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
                Arguments.of("Tuple(Int32, String)", new ArrowType.Struct(), false),
                // Wide integers carry their exact value as a base-10 string.
                Arguments.of("Int128", new ArrowType.Utf8(), false),
                Arguments.of("UInt128", new ArrowType.Utf8(), false),
                Arguments.of("Int256", new ArrowType.Utf8(), false),
                Arguments.of("UInt256", new ArrowType.Utf8(), false),
                Arguments.of("BFloat16", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false),
                Arguments.of("JSON", new ArrowType.Utf8(), false),
                Arguments.of("Dynamic", new ArrowType.Utf8(), false),
                Arguments.of("Variant(Int64, String)", new ArrowType.Utf8(), false),
                Arguments.of("Nothing", new ArrowType.Null(), false),
                // Time/Time64 and non-calendar Intervals → Duration; calendar Intervals → Interval(YEAR_MONTH).
                Arguments.of("Time", new ArrowType.Duration(TimeUnit.SECOND), false),
                Arguments.of("Time64(3)", new ArrowType.Duration(TimeUnit.MILLISECOND), false),
                Arguments.of("Time64(9)", new ArrowType.Duration(TimeUnit.NANOSECOND), false),
                Arguments.of("IntervalSecond", new ArrowType.Duration(TimeUnit.SECOND), false),
                Arguments.of("IntervalDay", new ArrowType.Duration(TimeUnit.SECOND), false),
                Arguments.of("IntervalNanosecond", new ArrowType.Duration(TimeUnit.NANOSECOND), false),
                Arguments.of("IntervalMonth", new ArrowType.Interval(IntervalUnit.YEAR_MONTH), false),
                Arguments.of("IntervalYear", new ArrowType.Interval(IntervalUnit.YEAR_MONTH), false));
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

    @Test
    void int128ColumnReadsAsDecimalString(BufferAllocator allocator) {
        String max = "170141183460469231731687303715884105727"; // 2^127 - 1
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "w", "Int128", new Int128Codec(), new Object[] {BigInteger.ZERO, new BigInteger(max)}));
        Schema schema = ClickHouseArrowTypes.schema(List.of("w"), List.of("Int128"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            VarCharVector w = (VarCharVector) root.getVector("w");
            assertEquals("0", new String(w.get(0), StandardCharsets.UTF_8));
            assertEquals(max, new String(w.get(1), StandardCharsets.UTF_8));
        }
    }

    @Test
    void bfloat16ColumnReadsAsFloat(BufferAllocator allocator) {
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "b", "BFloat16", new BFloat16Codec(), new Object[] {1.5f, -2.0f}));
        Schema schema = ClickHouseArrowTypes.schema(List.of("b"), List.of("BFloat16"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            Float4Vector b = (Float4Vector) root.getVector("b");
            assertEquals(1.5f, b.get(0));
            assertEquals(-2.0f, b.get(1));
        }
    }

    @Test
    void time64ColumnReadsAsDuration(BufferAllocator allocator) {
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "t", "Time64(3)", new Time64Codec(3),
                new Object[] {Duration.ofMillis(1500), Duration.ofSeconds(90)}));
        Schema schema = ClickHouseArrowTypes.schema(List.of("t"), List.of("Time64(3)"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            DurationVector t = (DurationVector) root.getVector("t");
            assertEquals(TimeUnit.MILLISECOND, ((ArrowType.Duration) t.getField().getType()).getUnit());
            assertEquals(Duration.ofMillis(1500), t.getObject(0));
            assertEquals(Duration.ofSeconds(90), t.getObject(1));
        }
    }

    @Test
    void intervalMonthColumnReadsAsTotalMonths(BufferAllocator allocator) {
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "i", "IntervalMonth", new IntervalCodec(IntervalCodec.Unit.MONTH),
                new Object[] {Period.ofMonths(14), Period.ofYears(2)}));
        Schema schema = ClickHouseArrowTypes.schema(List.of("i"), List.of("IntervalMonth"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            IntervalYearVector i = (IntervalYearVector) root.getVector("i");
            assertEquals(14, i.get(0));
            assertEquals(24, i.get(1));
        }
    }

    @Test
    void nothingColumnReadsAsAllNull(BufferAllocator allocator) {
        Block block = TestBlocks.blockOf(TestBlocks.column(
                "n", "Nothing", new NothingCodec(), new Object[] {null, null, null}));
        Schema schema = ClickHouseArrowTypes.schema(List.of("n"), List.of("Nothing"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            NullVector n = (NullVector) root.getVector("n");
            assertEquals(3, n.getValueCount());
            assertTrue(n.isNull(0));
            assertTrue(n.isNull(2));
        }
    }
}
