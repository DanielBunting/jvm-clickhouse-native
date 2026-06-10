package io.github.danielbunting.clickhouse.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.DateCodec;
import io.github.danielbunting.clickhouse.types.codec.DateTime64Codec;
import io.github.danielbunting.clickhouse.types.codec.DateTimeCodec;
import io.github.danielbunting.clickhouse.types.codec.DecimalCodec;
import io.github.danielbunting.clickhouse.types.codec.Enum16Codec;
import io.github.danielbunting.clickhouse.types.codec.Enum8Codec;
import io.github.danielbunting.clickhouse.types.codec.FixedStringCodec;
import io.github.danielbunting.clickhouse.types.codec.Float32Codec;
import io.github.danielbunting.clickhouse.types.codec.Float64Codec;
import io.github.danielbunting.clickhouse.types.codec.Int16Codec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Int64Codec;
import io.github.danielbunting.clickhouse.types.codec.Int8Codec;
import io.github.danielbunting.clickhouse.types.codec.NullableColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt16Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec;
import io.github.danielbunting.clickhouse.types.codec.UInt8Codec;
import io.github.danielbunting.clickhouse.types.codec.UuidCodec;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link DefaultTypeParser} type-string tokenization. */
class DefaultTypeParserTest {

    private final DefaultTypeParser parser = new DefaultTypeParser();

    @Test
    void parsesNumericLeaves() {
        assertInstanceOf(Int8Codec.class, parser.parse("Int8"));
        assertInstanceOf(Int16Codec.class, parser.parse("Int16"));
        assertInstanceOf(Int32Codec.class, parser.parse("Int32"));
        assertInstanceOf(Int64Codec.class, parser.parse("Int64"));
        assertInstanceOf(UInt8Codec.class, parser.parse("UInt8"));
        assertInstanceOf(UInt16Codec.class, parser.parse("UInt16"));
        assertInstanceOf(UInt32Codec.class, parser.parse("UInt32"));
        assertInstanceOf(UInt64Codec.class, parser.parse("UInt64"));
        assertInstanceOf(Float32Codec.class, parser.parse("Float32"));
        assertInstanceOf(Float64Codec.class, parser.parse("Float64"));
    }

    @Test
    void parsesStringAndUuid() {
        assertInstanceOf(StringColumnCodec.class, parser.parse("String"));
        assertInstanceOf(UuidCodec.class, parser.parse("UUID"));
    }

    @Test
    void parsesDateTypes() {
        assertInstanceOf(DateCodec.class, parser.parse("Date"));
        assertInstanceOf(DateTimeCodec.class, parser.parse("DateTime"));
        assertInstanceOf(DateTimeCodec.class, parser.parse("DateTime('Europe/London')"));
        assertInstanceOf(DateTime64Codec.class, parser.parse("DateTime64(3)"));
        assertInstanceOf(DateTime64Codec.class, parser.parse("DateTime64(9, 'UTC')"));
    }

    @Test
    void parsesFixedString() {
        assertInstanceOf(FixedStringCodec.class, parser.parse("FixedString(16)"));
    }

    @Test
    void parsesDecimal() {
        assertInstanceOf(DecimalCodec.class, parser.parse("Decimal(18, 4)"));
    }

    @Test
    void parsesEnums() {
        assertInstanceOf(Enum8Codec.class, parser.parse("Enum8('a' = 1, 'b' = 2)"));
        assertInstanceOf(Enum16Codec.class, parser.parse("Enum16('x' = 100, 'y' = 200)"));
    }

    @Test
    void parsesArray() {
        ColumnCodec<?> codec = parser.parse("Array(UInt32)");
        assertInstanceOf(ArrayColumnCodec.class, codec);
    }

    @Test
    void nullableWrapsInnerCodec() {
        // Nullable(T) -> NullableColumnCodec(T) so nested null-maps are decoded;
        // a top-level Nullable column is stripped by the block layer instead.
        ColumnCodec<?> u = parser.parse("Nullable(UInt32)");
        assertInstanceOf(NullableColumnCodec.class, u);
        assertInstanceOf(UInt32Codec.class, ((NullableColumnCodec) u).inner());
        ColumnCodec<?> s = parser.parse("Nullable(String)");
        assertInstanceOf(NullableColumnCodec.class, s);
        assertInstanceOf(StringColumnCodec.class, ((NullableColumnCodec) s).inner());
    }

    @Test
    void nullableArrayResolvesToNullableOfArray() {
        // "Nullable(Array(UInt32))" -> Nullable(Array(UInt32)).
        ColumnCodec<?> codec = parser.parse("Nullable(Array(UInt32))");
        assertInstanceOf(NullableColumnCodec.class, codec);
        assertInstanceOf(ArrayColumnCodec.class, ((NullableColumnCodec) codec).inner());
    }

    @Test
    void arrayOfNullableResolvesToArrayOfNullable() {
        // "Array(Nullable(UInt32))" -> Array whose inner element codec is Nullable(UInt32).
        ColumnCodec<?> codec = parser.parse("Array(Nullable(UInt32))");
        assertInstanceOf(ArrayColumnCodec.class, codec);
        assertInstanceOf(NullableColumnCodec.class, ((ArrayColumnCodec) codec).inner());
    }

    @Test
    void nestedArrayResolves() {
        ColumnCodec<?> codec = parser.parse("Array(Array(String))");
        assertInstanceOf(ArrayColumnCodec.class, codec);
    }

    @Test
    void lowCardinalityResolvesToCodec() {
        // LowCardinality now resolves to a dedicated codec that decodes the native
        // dictionary+index wire format (it used to be silently unwrapped to the inner codec).
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec.class,
                parser.parse("LowCardinality(String)"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.LowCardinalityColumnCodec.class,
                parser.parse("LowCardinality(Nullable(String))"));
    }

    @Test
    void newlySupportedTypesResolve() {
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.Int128Codec.class, parser.parse("Int128"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.Int256Codec.class, parser.parse("Int256"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.UInt128Codec.class, parser.parse("UInt128"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.UInt256Codec.class, parser.parse("UInt256"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.BoolCodec.class, parser.parse("Bool"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.Date32Codec.class, parser.parse("Date32"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.Ipv4Codec.class, parser.parse("IPv4"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.Ipv6Codec.class, parser.parse("IPv6"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.TupleColumnCodec.class,
                parser.parse("Tuple(UInt32, String)"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.TupleColumnCodec.class,
                parser.parse("Tuple(a UInt32, b String)"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.MapColumnCodec.class,
                parser.parse("Map(String, UInt32)"));
        // SimpleAggregateFunction transparently resolves to its underlying type's codec.
        assertInstanceOf(UInt64Codec.class, parser.parse("SimpleAggregateFunction(sum, UInt64)"));
        // Round 2: geo, temporal, semi-structured, interval, nothing, nested.
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.TupleColumnCodec.class,
                parser.parse("Point"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec.class,
                parser.parse("Ring"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.ArrayColumnCodec.class,
                parser.parse("Nested(a UInt32, b String)"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.TimeCodec.class, parser.parse("Time"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.Time64Codec.class, parser.parse("Time64(3)"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.BFloat16Codec.class, parser.parse("BFloat16"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.IntervalCodec.class, parser.parse("IntervalDay"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.NothingCodec.class, parser.parse("Nothing"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.VariantColumnCodec.class,
                parser.parse("Variant(UInt32, String)"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.DynamicColumnCodec.class,
                parser.parse("Dynamic"));
        assertInstanceOf(io.github.danielbunting.clickhouse.types.codec.JsonColumnCodec.class,
                parser.parse("JSON"));
    }

    @Test
    void isNullableDetectsWrapper() {
        assertTrue(DefaultTypeParser.isNullable("Nullable(UInt32)"));
        assertTrue(DefaultTypeParser.isNullable("  Nullable(String)  "));
        assertFalse(DefaultTypeParser.isNullable("UInt32"));
        assertFalse(DefaultTypeParser.isNullable("Array(Nullable(UInt32))"));
        assertFalse(DefaultTypeParser.isNullable(null));
    }

    @Test
    void unwrapNullableStripsSingleWrapper() {
        assertEquals("UInt32", DefaultTypeParser.unwrapNullable("Nullable(UInt32)"));
        assertEquals("Array(UInt32)", DefaultTypeParser.unwrapNullable("Nullable(Array(UInt32))"));
        assertEquals("UInt32", DefaultTypeParser.unwrapNullable("UInt32"));
    }

    @Test
    void unsupportedTypeThrows() {
        assertThrows(ClickHouseException.class, () -> parser.parse("Bogus"));
        assertThrows(ClickHouseException.class, () -> parser.parse(""));
        assertThrows(ClickHouseException.class, () -> parser.parse(null));
    }

    /**
     * Standard ClickHouse types this client does not (yet) implement must fail
     * fast at parse time with a clear {@link ClickHouseException}, rather than
     * silently resolving to a wrong codec and corrupting the wire stream. This
     * pins the supported-type boundary; remove a type here when support lands.
     */
    @Test
    void unsupportedExtendedTypesThrow() {
        for (String type : new String[] {
                "Geometry",                       // Variant-based; not in 25.6
                "AggregateFunction(sum, UInt64)", // opaque state -> specific actionable throw
                "Expression",                     // lambda; never a stored/result column
                "Set",                            // IN-operand; never a stored/result column
                "Bogus", "NotAType(7)" }) {       // plain garbage
            assertThrows(ClickHouseException.class, () -> parser.parse(type),
                    "Unsupported type must throw ClickHouseException: " + type);
        }
    }
}
