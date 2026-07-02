package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.Float32Codec;
import io.github.danielbunting.clickhouse.types.codec.Float64Codec;
import io.github.danielbunting.clickhouse.types.codec.Int16Codec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Int64Codec;
import io.github.danielbunting.clickhouse.types.codec.Int8Codec;
import io.github.danielbunting.clickhouse.types.codec.NullableColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt8Codec;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Server-free unit tests proving {@code ChResultSet.getInt/getLong/getDouble/getFloat}
 * read through the non-boxing primitive accessors ({@code Column.longAt/doubleAt})
 * and still preserve {@code wasNull} and range semantics. Each column is built
 * directly from a codec and a primitive backing array, then exposed via a fake
 * {@link QueryResult}.
 */
class ChResultSetPrimitiveTest {

    // ------------------------------------------------------------------
    // Fake QueryResult / single-block plumbing
    // ------------------------------------------------------------------

    private static final class FakeResult implements QueryResult {
        private final Block block;

        FakeResult(Block block) {
            this.block = block;
        }

        @Override
        public List<String> columnNames() {
            return block.columns().stream().map(Column::name).toList();
        }

        @Override
        public List<String> columnTypes() {
            return block.columns().stream().map(Column::type).toList();
        }

        @Override
        public Iterator<Block> blocks() {
            return List.of(block).iterator();
        }

        @Override
        public void close() {
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Column col(String name, String type, ColumnCodec codec, Object values, int rows) {
        Column c = new Column(name, type);
        c.codec(codec);
        c.values(values);
        c.rowCount(rows);
        return c;
    }

    private static ChResultSet single(Column... cols) throws SQLException {
        Block b = new Block();
        int rows = cols.length == 0 ? 0 : cols[0].rowCount();
        for (Column c : cols) {
            b.addColumn(c);
        }
        b.rowCount(rows);
        ChResultSet rs = new ChResultSet(new FakeResult(b));
        assertTrue(rs.next());
        return rs;
    }

    // ------------------------------------------------------------------
    // Per-type primitive reads
    // ------------------------------------------------------------------

    @Test
    void int8_getInt_getLong() throws SQLException {
        Int8Codec codec = new Int8Codec();
        byte[] a = codec.allocate(1);
        a[0] = -7;
        ChResultSet rs = single(col("c", "Int8", codec, a, 1));
        assertEquals(-7, rs.getInt(1));
        assertEquals(-7L, rs.getLong(1));
        assertEquals((byte) -7, rs.getByte(1));
        assertFalse(rs.wasNull());
    }

    @Test
    void int16_getInt() throws SQLException {
        Int16Codec codec = new Int16Codec();
        short[] a = codec.allocate(1);
        a[0] = 12345;
        ChResultSet rs = single(col("c", "Int16", codec, a, 1));
        assertEquals(12345, rs.getInt(1));
        assertEquals(12345L, rs.getLong(1));
    }

    @Test
    void int32_getInt_getLong() throws SQLException {
        Int32Codec codec = new Int32Codec();
        int[] a = codec.allocate(1);
        a[0] = Integer.MIN_VALUE;
        ChResultSet rs = single(col("c", "Int32", codec, a, 1));
        assertEquals(Integer.MIN_VALUE, rs.getInt(1));
        assertEquals((long) Integer.MIN_VALUE, rs.getLong(1));
    }

    @Test
    void int64_getLong_minMax() throws SQLException {
        Int64Codec codec = new Int64Codec();
        long[] a = codec.allocate(2);
        a[0] = Long.MIN_VALUE;
        a[1] = Long.MAX_VALUE;
        ChResultSet rs = single(col("c", "Int64", codec, a, 2));
        assertEquals(Long.MIN_VALUE, rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(Long.MAX_VALUE, rs.getLong(1));
    }

    @Test
    void int64_getInt_outOfRangeThrows() throws SQLException {
        Int64Codec codec = new Int64Codec();
        long[] a = codec.allocate(1);
        a[0] = Long.MAX_VALUE;
        ChResultSet rs = single(col("c", "Int64", codec, a, 1));
        assertThrows(SQLException.class, () -> rs.getInt(1));
    }

    @Test
    void uint8_getInt() throws SQLException {
        UInt8Codec codec = new UInt8Codec();
        int[] a = codec.allocate(1);
        a[0] = 255;
        ChResultSet rs = single(col("c", "UInt8", codec, a, 1));
        assertEquals(255, rs.getInt(1));
        assertEquals(255L, rs.getLong(1));
    }

    @Test
    void uint32_getLong_atMax() throws SQLException {
        UInt32Codec codec = new UInt32Codec();
        long[] a = codec.allocate(1);
        a[0] = (1L << 32) - 1;
        ChResultSet rs = single(col("c", "UInt32", codec, a, 1));
        assertEquals((1L << 32) - 1, rs.getLong(1));
    }

    @Test
    void uint64_getLong_rawBitsHighBitSet() throws SQLException {
        UInt64Codec codec = new UInt64Codec();
        long[] a = codec.allocate(1);
        a[0] = 0x8000_0000_0000_0001L;
        ChResultSet rs = single(col("c", "UInt64", codec, a, 1));
        // Raw-bits semantics: getLong returns the negative signed long as-is.
        assertEquals(0x8000_0000_0000_0001L, rs.getLong(1));
        assertEquals("9223372036854775809", Long.toUnsignedString(rs.getLong(1)));
    }

    @Test
    void uint64_boxedAccessors_returnUnsignedBigInteger() throws SQLException {
        UInt64Codec codec = new UInt64Codec();
        long[] a = codec.allocate(1);
        a[0] = 0x8000_0000_0000_0001L; // 2^63 + 1, high bit set -> negative as a signed long
        ChResultSet rs = single(col("c", "UInt64", codec, a, 1));
        java.math.BigInteger expected = new java.math.BigInteger("9223372036854775809");
        // The logical/boxed accessors surface the true unsigned value (Types.NUMERIC).
        assertEquals(expected, rs.getObject(1), "getObject returns the unsigned BigInteger");
        assertEquals(java.math.BigInteger.class, rs.getObject(1).getClass());
        assertEquals("9223372036854775809", rs.getString(1), "getString is unsigned");
        assertEquals(new java.math.BigDecimal(expected), rs.getBigDecimal(1), "getBigDecimal is unsigned");
        // getLong keeps the raw-bits contract (covered above) — the two paths intentionally differ.
        assertEquals(0x8000_0000_0000_0001L, rs.getLong(1));
    }

    @Test
    void uint64_smallValue_getObjectIsBigIntegerNotNegative() throws SQLException {
        UInt64Codec codec = new UInt64Codec();
        long[] a = codec.allocate(1);
        a[0] = 42L;
        ChResultSet rs = single(col("c", "UInt64", codec, a, 1));
        assertEquals(new java.math.BigInteger("42"), rs.getObject(1),
                "small UInt64 values also box as BigInteger for a consistent column class");
    }

    @Test
    void float32_getDouble_getFloat() throws SQLException {
        Float32Codec codec = new Float32Codec();
        float[] a = codec.allocate(1);
        a[0] = 1.5f;
        ChResultSet rs = single(col("c", "Float32", codec, a, 1));
        assertEquals(1.5f, rs.getFloat(1), 0.0f);
        assertEquals(1.5d, rs.getDouble(1), 0.0);
    }

    @Test
    void float64_getDouble() throws SQLException {
        Float64Codec codec = new Float64Codec();
        double[] a = codec.allocate(1);
        a[0] = Math.PI;
        ChResultSet rs = single(col("c", "Float64", codec, a, 1));
        assertEquals(Math.PI, rs.getDouble(1), 0.0);
    }

    @Test
    void int32_getDouble_widens() throws SQLException {
        Int32Codec codec = new Int32Codec();
        int[] a = codec.allocate(1);
        a[0] = 42;
        ChResultSet rs = single(col("c", "Int32", codec, a, 1));
        assertEquals(42.0, rs.getDouble(1), 0.0);
    }

    // ------------------------------------------------------------------
    // Null handling on the primitive path
    // ------------------------------------------------------------------

    @Test
    void nullableInt64_nullCell_wasNullAndZero() throws SQLException {
        NullableColumnCodec codec = new NullableColumnCodec(new Int64Codec());
        Object[] a = codec.allocate(2);
        a[0] = null;
        a[1] = 99L;
        Column c = new Column("c", "Nullable(Int64)");
        c.codec(codec);
        c.values(a);
        c.nulls(new boolean[]{true, false});
        c.rowCount(2);

        // Nullable codec's javaType is Long -> primitive integer path is used, but
        // the null-map short-circuits to wasNull/0 before touching longAt.
        ChResultSet rs = single(c);
        assertEquals(0L, rs.getLong(1));
        assertTrue(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(99L, rs.getLong(1));
        assertFalse(rs.wasNull());
    }

    /**
     * Wide integers (Int128/Int256/UInt128/UInt256) through the narrow accessors
     * (reference: client-v2 AbstractBinaryReaderTest#testReadingNumbers /
     * #testGetBigDecimalForIntegerWidths8To256): in-range values narrow cleanly via
     * getInt/getLong, out-of-range values throw rather than truncate, and
     * getBigDecimal is exact at the extremes.
     */
    @org.junit.jupiter.api.Test
    void wideInts_narrowAccessorsRangeCheckAndBigDecimalIsExact() throws SQLException {
        java.math.BigInteger int128Max = java.math.BigInteger.TWO.pow(127)
                .subtract(java.math.BigInteger.ONE);
        java.math.BigInteger uint256Max = java.math.BigInteger.TWO.pow(256)
                .subtract(java.math.BigInteger.ONE);

        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("small128", "Int128",
                        new io.github.danielbunting.clickhouse.types.codec.Int128Codec(),
                        java.math.BigInteger.valueOf(42)),
                RsFixtures.complexCol("max128", "Int128",
                        new io.github.danielbunting.clickhouse.types.codec.Int128Codec(),
                        int128Max),
                RsFixtures.complexCol("neg256", "Int256",
                        new io.github.danielbunting.clickhouse.types.codec.Int256Codec(),
                        java.math.BigInteger.valueOf(Long.MIN_VALUE)),
                RsFixtures.complexCol("maxU256", "UInt256",
                        new io.github.danielbunting.clickhouse.types.codec.UInt256Codec(),
                        uint256Max));
        assertTrue(rs.next());

        // In-range narrows cleanly.
        assertEquals(42, rs.getInt(1));
        assertEquals(42L, rs.getLong(1));
        assertEquals(Long.MIN_VALUE, rs.getLong(3), "Long.MIN_VALUE fits getLong exactly");

        // Out-of-range never truncates silently (the exception TYPE is the known bug below).
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> rs.getInt(2));
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> rs.getLong(2));
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> rs.getInt(3),
                "Long.MIN_VALUE overflows int");
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> rs.getLong(4));

        // getBigDecimal is exact at every width extreme.
        assertEquals(new java.math.BigDecimal(int128Max), rs.getBigDecimal(2));
        assertEquals(new java.math.BigDecimal(uint256Max), rs.getBigDecimal(4));
        assertEquals(java.math.BigDecimal.valueOf(Long.MIN_VALUE), rs.getBigDecimal(3));
    }

    /**
     * KNOWN BUG — asserts the CORRECT behavior and currently FAILS.
     *
     * <p>Narrowing an out-of-range WIDE integer (Int128/Int256/UInt128/UInt256, backed
     * by BigInteger) via {@code getInt}/{@code getLong} leaks a raw
     * {@link ArithmeticException} from {@code BigInteger.intValueExact()} instead of the
     * JDBC-spec {@link SQLException} that the Int64 narrowing path throws (see
     * {@code int64_getInt_outOfRangeThrows}). The exception surface must be uniform.
     *
     * <p>Fix approach: catch {@code ArithmeticException} in the BigInteger branch of
     * {@code JdbcValues}' int/long narrowing and rethrow as {@code SQLException}, exactly
     * like the long-based range check.
     */
    @org.junit.jupiter.api.Test
    void knownBug_wideIntOverflowMustThrowSqlExceptionNotArithmetic() throws SQLException {
        java.math.BigInteger int128Max = java.math.BigInteger.TWO.pow(127)
                .subtract(java.math.BigInteger.ONE);
        ChResultSet rs = RsFixtures.open(
                RsFixtures.complexCol("max128", "Int128",
                        new io.github.danielbunting.clickhouse.types.codec.Int128Codec(),
                        int128Max));
        assertTrue(rs.next());
        org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> rs.getInt(1),
                "JDBC contract: conversion overflow surfaces as SQLException");
    }
}
