package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.UnsupportedTypeException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The type-mapping contract of {@link ClickHouseArrowTypes} beyond the flat forward table in
 * {@link BlockToArrowTest#typeMappingTable}: timezone/precision handling, Decimal aliases,
 * nullability placement inside containers, source-type metadata, the reverse
 * {@code clickHouseType(Field)} mapping used by ingest CREATE, and the supported-type boundary.
 * The ADBC analogue of the JDBC module's {@code ChResultSetMetaDataTest}.
 */
class ClickHouseArrowTypesTest {

    private static final String CH_TYPE_META = "clickhouse.type";

    // ---- timezone & precision -------------------------------------------------------------

    @Test
    @DisplayName("DateTime carries its column timezone onto the Arrow Timestamp")
    void dateTimeCarriesTimezone() {
        Field field = ClickHouseArrowTypes.arrowField("t", "DateTime('Europe/London')");
        assertEquals(new ArrowType.Timestamp(TimeUnit.SECOND, "Europe/London"), field.getType());
    }

    @Test
    @DisplayName("DateTime64 carries both the precision-derived unit and the timezone")
    void dateTime64CarriesUnitAndTimezone() {
        Field field = ClickHouseArrowTypes.arrowField("t", "DateTime64(6, 'Asia/Tokyo')");
        assertEquals(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Tokyo"), field.getType());
    }

    @ParameterizedTest(name = "DateTime64({0}) -> {1}")
    @MethodSource("precisionRows")
    @DisplayName("DateTime64 precision maps to the nearest Arrow unit at or finer than p")
    void precisionToTimeUnitBoundaries(int precision, TimeUnit expected) {
        Field field = ClickHouseArrowTypes.arrowField("t", "DateTime64(" + precision + ")");
        assertEquals(expected, ((ArrowType.Timestamp) field.getType()).getUnit());
    }

    static Stream<Arguments> precisionRows() {
        return Stream.of(
                Arguments.of(0, TimeUnit.SECOND),
                Arguments.of(1, TimeUnit.MILLISECOND),
                Arguments.of(3, TimeUnit.MILLISECOND),
                Arguments.of(4, TimeUnit.MICROSECOND),
                Arguments.of(6, TimeUnit.MICROSECOND),
                Arguments.of(7, TimeUnit.NANOSECOND),
                Arguments.of(9, TimeUnit.NANOSECOND));
    }

    @ParameterizedTest(name = "{0} -> bitWidth {1}")
    @MethodSource("decimalWidthRows")
    @DisplayName("canonical Decimal(p, s) picks the 128- or 256-bit vector by precision")
    void decimalBitWidths(String type, int precision, int scale, int bitWidth) {
        // The DecimalN(s) alias forms (Decimal32(4), …) never reach the client: ClickHouse
        // normalises them to Decimal(p, s) in DESCRIBE/system.columns and on the wire, and
        // DefaultTypeParser rejects the alias spelling with UnsupportedTypeException.
        assertEquals(new ArrowType.Decimal(precision, scale, bitWidth),
                ClickHouseArrowTypes.arrowField("d", type).getType());
    }

    static Stream<Arguments> decimalWidthRows() {
        return Stream.of(
                Arguments.of("Decimal(9, 4)", 9, 4, 128),
                Arguments.of("Decimal(18, 8)", 18, 8, 128),
                Arguments.of("Decimal(38, 18)", 38, 18, 128),
                Arguments.of("Decimal(39, 18)", 39, 18, 256),
                Arguments.of("Decimal(76, 18)", 76, 18, 256));
    }

    // ---- nullability placement inside containers ------------------------------------------------

    @Test
    @DisplayName("Map(String, Nullable(Int32)) marks only the value child nullable")
    void mapNullableValueChild() {
        Field field = ClickHouseArrowTypes.arrowField("m", "Map(String, Nullable(Int32))");
        Field entries = field.getChildren().get(0);
        Field key = entries.getChildren().get(0);
        Field value = entries.getChildren().get(1);
        assertTrue(!key.isNullable(), "map keys are never nullable");
        assertTrue(value.isNullable(), "Nullable(Int32) value must be nullable");
    }

    @Test
    @DisplayName("named Tuple elements keep their declared names")
    void namedTupleElements() {
        Field field = ClickHouseArrowTypes.arrowField("t", "Tuple(a Int32, b String)");
        assertEquals(List.of("a", "b"),
                field.getChildren().stream().map(Field::getName).toList());
    }

    @Test
    @DisplayName("Array(Array(Nullable(String))) nests the nullability at the innermost leaf")
    void deepNestedNullability() {
        Field field = ClickHouseArrowTypes.arrowField("a", "Array(Array(Nullable(String)))");
        Field inner = field.getChildren().get(0);
        assertEquals(new ArrowType.List(), inner.getType());
        Field leaf = inner.getChildren().get(0);
        assertTrue(leaf.isNullable());
        assertEquals(ArrowType.Utf8.INSTANCE, leaf.getType());
    }

    @Test
    @DisplayName("Nullable(Array(...)) is not a thing ClickHouse allows, but Array itself is non-null")
    void arrayFieldNotNullableByDefault() {
        assertTrue(!ClickHouseArrowTypes.arrowField("a", "Array(Int32)").isNullable());
    }

    // ---- source-type metadata --------------------------------------------------------------------

    @Test
    @DisplayName("every mapped field preserves the exact source type string in metadata")
    void sourceTypePreservedInMetadata() {
        for (String type : new String[] {
                "UUID", "IPv6", "Int128", "LowCardinality(Nullable(String))",
                "DateTime64(3, 'UTC')", "Map(String, Array(Int32))"}) {
            Field field = ClickHouseArrowTypes.arrowField("c", type);
            assertEquals(type, field.getMetadata().get(CH_TYPE_META),
                    "the lossy structural mapping must be recoverable via metadata");
        }
    }

    @Test
    @DisplayName("clickHouseType prefers the metadata source type over the structural reverse map")
    void reverseMappingPrefersMetadata() {
        // UUID and IPv6 both map to FixedSizeBinary(16); only the metadata disambiguates.
        assertEquals("UUID",
                ClickHouseArrowTypes.clickHouseType(ClickHouseArrowTypes.arrowField("c", "UUID")));
        assertEquals("IPv6",
                ClickHouseArrowTypes.clickHouseType(ClickHouseArrowTypes.arrowField("c", "IPv6")));
    }

    // ---- structural reverse mapping (fields built outside this library) ---------------------------

    @ParameterizedTest(name = "{1} <- {0}")
    @MethodSource("structuralReverseRows")
    @DisplayName("the structural reverse map covers every ingest-supported scalar")
    void structuralReverseMapping(ArrowType arrowType, String expected, boolean nullable) {
        Field field = new Field("c", new FieldType(nullable, arrowType, null), null);
        assertEquals(expected, ClickHouseArrowTypes.clickHouseType(field));
    }

    static Stream<Arguments> structuralReverseRows() {
        return Stream.of(
                Arguments.of(new ArrowType.Int(8, true), "Int8", false),
                Arguments.of(new ArrowType.Int(64, false), "UInt64", false),
                Arguments.of(new ArrowType.Int(32, true), "Nullable(Int32)", true),
                Arguments.of(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), "Float32", false),
                Arguments.of(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), "Float64", false),
                Arguments.of(new ArrowType.Bool(), "Bool", false),
                Arguments.of(new ArrowType.Utf8(), "String", false),
                Arguments.of(new ArrowType.FixedSizeBinary(10), "FixedString(10)", false),
                Arguments.of(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY), "Date32", false),
                Arguments.of(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), "DateTime('UTC')", false),
                Arguments.of(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), "DateTime64(3, 'UTC')", false),
                Arguments.of(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), "DateTime64(9, 'UTC')", false),
                Arguments.of(new ArrowType.Decimal(10, 2, 128), "Decimal(10, 2)", false),
                Arguments.of(new ArrowType.Duration(TimeUnit.SECOND), "Time", false),
                Arguments.of(new ArrowType.Duration(TimeUnit.MICROSECOND), "Time64(6)", false),
                Arguments.of(new ArrowType.Interval(IntervalUnit.YEAR_MONTH), "IntervalMonth", false));
    }

    @Test
    @DisplayName("the structural reverse map rebuilds container types recursively")
    void structuralReverseContainers() {
        Field intChild = new Field("item", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field list = new Field("a", FieldType.notNullable(new ArrowType.List()), List.of(intChild));
        assertEquals("Array(Int32)", ClickHouseArrowTypes.clickHouseType(list));

        Field key = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field value = new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(key, value));
        Field map = new Field("m", FieldType.notNullable(new ArrowType.Map(false)), List.of(entries));
        assertEquals("Map(String, Nullable(Int64))", ClickHouseArrowTypes.clickHouseType(map));

        Field f0 = new Field("x", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field f1 = new Field("y", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field struct = new Field("t", FieldType.notNullable(new ArrowType.Struct()), List.of(f0, f1));
        assertEquals("Tuple(`x` Int32, `y` String)", ClickHouseArrowTypes.clickHouseType(struct));
    }

    @Test
    @DisplayName("nullability on a container is dropped in the reverse map (ClickHouse forbids it)")
    void reverseMapDropsContainerNullability() {
        Field child = new Field("item", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field list = new Field("a", FieldType.nullable(new ArrowType.List()), List.of(child));
        assertEquals("Array(Int32)", ClickHouseArrowTypes.clickHouseType(list));
    }

    // ---- supported-type boundary & argument validation --------------------------------------------

    @Test
    @DisplayName("an undecodable ClickHouse type raises UnsupportedTypeException at mapping time")
    void aggregateFunctionIsUnsupported() {
        assertThrows(UnsupportedTypeException.class,
                () -> ClickHouseArrowTypes.arrowField("c", "AggregateFunction(sum, UInt64)"));
    }

    @Test
    @DisplayName("schema() rejects mismatched name/type list lengths")
    void schemaRejectsMismatchedLengths() {
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseArrowTypes.schema(List.of("a", "b"), List.of("Int32")));
    }

    @Test
    @DisplayName("an unmappable Arrow type raises UnsupportedOperationException in the reverse map")
    void reverseMapUnsupportedArrowType() {
        Field field = new Field("c",
                FieldType.notNullable(new ArrowType.Binary()), null);
        assertThrows(UnsupportedOperationException.class,
                () -> ClickHouseArrowTypes.clickHouseType(field));
    }
}
