package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.BFloat16Codec;
import io.github.danielbunting.clickhouse.types.codec.DecimalCodec;
import io.github.danielbunting.clickhouse.types.codec.Enum16Codec;
import io.github.danielbunting.clickhouse.types.codec.Enum8Codec;
import io.github.danielbunting.clickhouse.types.codec.Float32Codec;
import io.github.danielbunting.clickhouse.types.codec.Int128Codec;
import io.github.danielbunting.clickhouse.types.codec.Int16Codec;
import io.github.danielbunting.clickhouse.types.codec.Int8Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt16Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt256Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt8Codec;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The numeric/decimal/enum half of the Arrow→Java decoder matrix ({@link ArrowToBlock#toJavaValue},
 * the ADBC analogue of the JDBC module's {@code JdbcValuesTest} coercion layer): signedness and
 * width edges, unsigned raw-bits semantics, float widening, Decimal at every bit width, enum
 * ordinals and wide-int strings. Temporal and composite shapes are covered by
 * {@link BlockToArrowTemporalTest}/{@link BlockToArrowComplexTest}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class ArrowToBlockValuesTest {

    @Test
    @DisplayName("signed ints surface as Long at every width, including the negative extremes")
    void signedIntWidthEdges(BufferAllocator allocator) {
        assertEquals(List.of((long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE),
                roundTrip(allocator, "Int8", new Int8Codec(),
                        new Object[] {(int) Byte.MIN_VALUE, (int) Byte.MAX_VALUE}));
        assertEquals(List.of((long) Short.MIN_VALUE, (long) Short.MAX_VALUE),
                roundTrip(allocator, "Int16", new Int16Codec(),
                        new Object[] {(int) Short.MIN_VALUE, (int) Short.MAX_VALUE}));
        assertEquals(List.of(Long.MIN_VALUE, Long.MAX_VALUE),
                roundTrip(allocator, "Int64", new io.github.danielbunting.clickhouse.types.codec.Int64Codec(),
                        new Object[] {Long.MIN_VALUE, Long.MAX_VALUE}));
    }

    @Test
    @DisplayName("unsigned ints surface as their unsigned value up to UInt32")
    void unsignedIntsUpTo32Bits(BufferAllocator allocator) {
        assertEquals(List.of(0L, 255L),
                roundTrip(allocator, "UInt8", new UInt8Codec(), new Object[] {0, 255}));
        assertEquals(List.of(65535L),
                roundTrip(allocator, "UInt16", new UInt16Codec(), new Object[] {65535}));
        assertEquals(List.of(4294967295L),
                roundTrip(allocator, "UInt32", new UInt32Codec(), new Object[] {4294967295L}));
    }

    @Test
    @DisplayName("UInt64 above Long.MAX_VALUE carries raw bits (wraps negative as a signed long)")
    void uint64RawBits(BufferAllocator allocator) {
        // 18446744073709551615 = 2^64-1 → raw bits -1L. The unsigned reading is recoverable
        // via Long.toUnsignedString; pinned as the documented representation choice.
        List<?> read = roundTrip(allocator, "UInt64", new UInt64Codec(),
                new Object[] {new BigInteger("18446744073709551615")});
        assertEquals(-1L, read.get(0));
        assertEquals("18446744073709551615", Long.toUnsignedString((Long) read.get(0)));
    }

    @Test
    @DisplayName("Float32 widens to Double through the decoder")
    void float32WidensToDouble(BufferAllocator allocator) {
        List<?> read = roundTrip(allocator, "Float32", new Float32Codec(), new Object[] {1.5f});
        assertInstanceOf(Double.class, read.get(0));
        assertEquals(1.5, (Double) read.get(0));
    }

    @Test
    @DisplayName("BFloat16 widens exactly to Float32 (a bf16 is a truncated f32)")
    void bfloat16Widens(BufferAllocator allocator) {
        assertEquals(List.of(1.5, -2.0),
                roundTrip(allocator, "BFloat16", new BFloat16Codec(), new Object[] {1.5f, -2.0f}));
    }

    @Test
    @DisplayName("Decimal round-trips exactly at 128-bit widths, keeping the column scale")
    void decimal128Exact(BufferAllocator allocator) {
        List<?> read = roundTrip(allocator, "Decimal(18, 4)", new DecimalCodec(18, 4),
                new Object[] {new BigDecimal("12345.6789"), new BigDecimal("-0.0001")});
        assertEquals(0, new BigDecimal("12345.6789").compareTo((BigDecimal) read.get(0)));
        assertEquals(0, new BigDecimal("-0.0001").compareTo((BigDecimal) read.get(1)));
    }

    @Test
    @DisplayName("Decimal(76, s) uses the 256-bit vector and stays exact")
    void decimal256Exact(BufferAllocator allocator) {
        BigDecimal huge = new BigDecimal("1234567890123456789012345678901234567890.123456789012345678");
        List<?> read = roundTrip(allocator, "Decimal(76, 18)", new DecimalCodec(76, 18),
                new Object[] {huge});
        assertEquals(0, huge.compareTo((BigDecimal) read.get(0)));
    }

    @Test
    @DisplayName("Enum8/Enum16 surface their numeric ordinal, not the label")
    void enumsSurfaceOrdinals(BufferAllocator allocator) {
        assertEquals(List.of(1L, 2L),
                roundTrip(allocator, "Enum8('a' = 1, 'b' = 2)",
                        new Enum8Codec(Map.of(1, "a", 2, "b")), new Object[] {"a", "b"}));
        assertEquals(List.of(100L),
                roundTrip(allocator, "Enum16('x' = 100)",
                        new Enum16Codec(Map.of(100, "x")), new Object[] {"x"}));
    }

    @Test
    @DisplayName("wide integers surface as exact base-10 strings")
    void wideIntsAsDecimalStrings(BufferAllocator allocator) {
        String intMax = "170141183460469231731687303715884105727";     // 2^127 - 1
        String uintMax = "115792089237316195423570985008687907853269984665640564039457584007913129639935"; // 2^256 - 1
        assertEquals(List.of("-1", intMax),
                roundTrip(allocator, "Int128", new Int128Codec(),
                        new Object[] {BigInteger.valueOf(-1), new BigInteger(intMax)}));
        assertEquals(List.of(uintMax),
                roundTrip(allocator, "UInt256", new UInt256Codec(),
                        new Object[] {new BigInteger(uintMax)}));
    }

    @Test
    @DisplayName("nulls decode to null for every vector family, not a zero filler")
    void nullsDecodeToNull(BufferAllocator allocator) {
        Schema schema = ClickHouseArrowTypes.schema(
                List.of("i", "s"), List.of("Nullable(Int64)", "Nullable(String)"));
        Block block = TestBlocks.blockOf(
                TestBlocks.int64Column("i", new long[] {0}, new boolean[] {true}),
                TestBlocks.stringColumn("s", new String[] {null}, new boolean[] {true}));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BlockToArrow.fill(root, block);
            assertEquals(null, ArrowToBlock.toJavaValue(root.getVector("i"), 0));
            assertEquals(null, ArrowToBlock.toJavaValue(root.getVector("s"), 0));
        }
    }

    @Test
    @DisplayName("an Arrow vector family with no ingest conversion raises NOT_IMPLEMENTED")
    void unsupportedVectorRaisesNotImplemented(BufferAllocator allocator) {
        try (org.apache.arrow.vector.VarBinaryVector unsupported =
                new org.apache.arrow.vector.VarBinaryVector("b", allocator)) {
            unsupported.setSafe(0, new byte[] {1});
            unsupported.setValueCount(1);
            org.apache.arrow.adbc.core.AdbcException ex = assertThrows(
                    org.apache.arrow.adbc.core.AdbcException.class,
                    () -> ArrowToBlock.toJavaValue(unsupported, 0));
            assertEquals(org.apache.arrow.adbc.core.AdbcStatusCode.NOT_IMPLEMENTED, ex.getStatus());
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Fills one column of {@code values} into Arrow and decodes each row back. */
    private static List<?> roundTrip(
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
