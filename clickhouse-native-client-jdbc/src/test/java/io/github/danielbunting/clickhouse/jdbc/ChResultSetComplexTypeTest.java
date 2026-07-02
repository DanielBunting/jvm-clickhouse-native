package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.DateTimeCodec;
import io.github.danielbunting.clickhouse.types.codec.DecimalCodec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Ipv6Codec;
import io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UuidCodec;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Array;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Server-free unit tests for how {@link ChResultSet} exposes composite and reference
 * ClickHouse types: {@code getObject} boxing, ClickHouse-literal {@code getString}
 * (issue clickhouse-java#2723), and the {@link java.sql.Array} view from {@code getArray}.
 * Map columns are not covered here — their read path uses module-internal offsets that
 * a hand-built column cannot populate; Map is exercised in the integration suite.
 */
class ChResultSetComplexTypeTest {

    // ---- getObject boxing ---------------------------------------------------

    @Test
    void arrayColumn_getObject_returnsList() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Int32)", new ArrayColumnCodec(new Int32Codec()), List.of(1, 2, 3)));
        assertTrue(rs.next());
        Object v = rs.getObject(1);
        assertInstanceOf(List.class, v);
        assertEquals(List.of(1, 2, 3), v);
    }

    @Test
    void arrayOfNullableString_preservesInteriorNulls() throws Exception {
        List<String> withNull = Arrays.asList("a", null, "b");
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Nullable(String))", new ArrayColumnCodec(new StringColumnCodec()), withNull));
        assertTrue(rs.next());
        assertEquals(withNull, rs.getObject(1));
    }

    @Test
    void uuidColumn_getObject_returnsUuid() throws Exception {
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("u", "UUID", new UuidCodec(), uuid));
        assertTrue(rs.next());
        assertEquals(uuid, rs.getObject(1));
    }

    @Test
    void dateTimeColumn_getObject_returnsInstant() throws Exception {
        Instant when = Instant.parse("2026-05-30T13:45:07Z");
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("t", "DateTime", new DateTimeCodec(null), when));
        assertTrue(rs.next());
        assertEquals(when, rs.getObject(1));
    }

    @Test
    void decimalColumn_getObject_returnsBigDecimal() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "d", "Decimal(10, 2)", new DecimalCodec(10, 2), new BigDecimal("12.50")));
        assertTrue(rs.next());
        assertEquals(new BigDecimal("12.50"), rs.getObject(1));
    }

    @Test
    void ipv6Column_getObject_returnsInetAddress() throws Exception {
        InetAddress addr = InetAddress.getByName("2001:db8::1");
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("ip", "IPv6", new Ipv6Codec(), addr));
        assertTrue(rs.next());
        assertEquals(addr, rs.getObject(1));
    }

    @Test
    void lowCardinalityColumn_getObject_returnsInnerValue() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "lc", "LowCardinality(String)", new LowCardinalityColumnCodec(new StringColumnCodec()), "hello"));
        assertTrue(rs.next());
        assertEquals("hello", rs.getObject(1));
    }

    // ---- getString ClickHouse literal (issue #2723) -------------------------

    @Test
    void getString_onIntArray_rendersClickHouseLiteral() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Int32)", new ArrayColumnCodec(new Int32Codec()), List.of(1, 2, 3)));
        assertTrue(rs.next());
        assertEquals("[1, 2, 3]", rs.getString(1));
    }

    @Test
    void getString_onStringArray_quotesElements() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(String)", new ArrayColumnCodec(new StringColumnCodec()),
                List.of("field1", "field2", "field3")));
        assertTrue(rs.next());
        assertEquals("['field1', 'field2', 'field3']", rs.getString(1));
    }

    /** Regression for clickhouse-java#2723: getString on a nested array did not NPE / mis-render. */
    @Test
    void getString_onNestedArray_rendersNested() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Array(Int32))", new ArrayColumnCodec(new ArrayColumnCodec(new Int32Codec())),
                List.of(List.of(1, 2, 3), List.of(4, 5, 6))));
        assertTrue(rs.next());
        assertEquals("[[1, 2, 3], [4, 5, 6]]", rs.getString(1));
    }

    // ---- getArray (java.sql.Array) ------------------------------------------

    @Test
    void getArray_returnsObjectArrayAndBaseType() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Int32)", new ArrayColumnCodec(new Int32Codec()), List.of(1, 2, 3)));
        assertTrue(rs.next());
        Array arr = rs.getArray(1);
        assertEquals("Int32", arr.getBaseTypeName());
        assertEquals(java.sql.Types.INTEGER, arr.getBaseType());
        assertArrayEquals(new Object[] {1, 2, 3}, (Object[]) arr.getArray());
    }

    /** Regression for #2723: getArray on a nested array yields nested Object[]. */
    @Test
    void getArray_onNestedArray_returnsNestedObjectArrays() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Array(Int32))", new ArrayColumnCodec(new ArrayColumnCodec(new Int32Codec())),
                List.of(List.of(1, 2), List.of(3))));
        assertTrue(rs.next());
        Array arr = rs.getArray(1);
        assertEquals("Array(Int32)", arr.getBaseTypeName());
        Object[] outer = (Object[]) arr.getArray();
        assertEquals(2, outer.length);
        assertArrayEquals(new Object[] {1, 2}, (Object[]) outer[0]);
        assertArrayEquals(new Object[] {3}, (Object[]) outer[1]);
    }

    @Test
    void getArray_slice() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "a", "Array(Int32)", new ArrayColumnCodec(new Int32Codec()), List.of(10, 20, 30, 40)));
        assertTrue(rs.next());
        Array arr = rs.getArray(1);
        assertArrayEquals(new Object[] {20, 30}, (Object[]) arr.getArray(2, 2));
    }

    @Test
    void getArray_onNullCell_returnsNullAndSetsWasNull() throws Exception {
        // A null array cell via a Nullable wrapper on the column.
        ArrayColumnCodec codec = new ArrayColumnCodec(new Int32Codec());
        Object backing = codec.allocate(1);
        io.github.danielbunting.clickhouse.types.Column c =
                new io.github.danielbunting.clickhouse.types.Column("a", "Array(Int32)");
        c.codec(codec);
        c.values(backing);
        c.nulls(new boolean[] {true});
        c.rowCount(1);
        ChResultSet rs = RsFixtures.open(c);
        assertTrue(rs.next());
        assertNull(rs.getArray(1));
        assertTrue(rs.wasNull());
    }

    @Test
    void getArray_onNonArrayColumn_throws() throws Exception {
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol("s", "String", new StringColumnCodec(), "x"));
        assertTrue(rs.next());
        org.junit.jupiter.api.Assertions.assertThrows(java.sql.SQLException.class, () -> rs.getArray(1));
        assertFalse(rs.wasNull());
    }

    /**
     * A whole-column Nested value — announced as {@code Array(Tuple(...))} — renders
     * through {@code getString} as a ClickHouse-style nested literal with quoted string
     * members (reference: ClickHouseNestedValueTest#testMultipleValues, the
     * SQL-expression rendering facet; the plain nested-array rendering is covered
     * above by {@code getString_onNestedArray_rendersNested}).
     */
    @Test
    void getString_onNestedWholeColumn_rendersTupleLiterals() throws Exception {
        @SuppressWarnings("unchecked")
        io.github.danielbunting.clickhouse.types.ColumnCodec<Object> codec =
                (io.github.danielbunting.clickhouse.types.ColumnCodec<Object>)
                        new io.github.danielbunting.clickhouse.types.DefaultTypeParser()
                                .parse("Array(Tuple(a UInt32, b String))");
        ChResultSet rs = RsFixtures.open(RsFixtures.complexCol(
                "n", "Array(Tuple(a UInt32, b String))", codec,
                (Object) List.of(List.of(10L, "x"), List.of(20L, "y"))));
        assertTrue(rs.next());
        assertEquals("[[10, 'x'], [20, 'y']]", rs.getString(1),
                "numbers render bare, string members single-quoted");
    }
}
