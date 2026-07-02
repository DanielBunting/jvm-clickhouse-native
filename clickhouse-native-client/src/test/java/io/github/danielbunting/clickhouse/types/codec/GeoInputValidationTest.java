package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.DefaultTypeParser;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Write-path input validation for geo values (reference: client-v2
 * SerializerUtilsTest#testGeometrySerializationRejectsUnsupportedValue /
 * #testGeometrySerializationRejectsMalformedList). A {@code Point} resolves to the
 * {@code Tuple(Float64, Float64)} codec, so a malformed point must be rejected by the
 * tuple codec's own arity/shape checks rather than emitting a corrupt column.
 */
class GeoInputValidationTest {

    private final DefaultTypeParser parser = new DefaultTypeParser();

    @SuppressWarnings("unchecked")
    private static <T> ColumnCodec<Object> erase(ColumnCodec<T> codec) {
        return (ColumnCodec<Object>) codec;
    }

    @Test
    void point_withThreeCoordinates_isRejected() {
        ColumnCodec<Object> point = erase(parser.parse("Point"));
        Object arr = point.allocate(1);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> point.set(arr, 0, List.of(1.0, 2.0, 3.0)));
        assertTrue(ex.getMessage().contains("expects 2 components"),
                "arity check names the mismatch: " + ex.getMessage());
    }

    @Test
    void point_withScalarValue_isRejected() {
        ColumnCodec<Object> point = erase(parser.parse("Point"));
        Object arr = point.allocate(1);
        for (Object bad : new Object[] {"not-a-point", 42, 1.5}) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> point.set(arr, 0, bad), String.valueOf(bad));
            assertTrue(ex.getMessage().contains("must be a java.util.List or Object[]"),
                    "shape check names the expected input: " + ex.getMessage());
        }
    }

    @Test
    void point_wellFormedListRoundTripsThroughSetGet() {
        ColumnCodec<Object> point = erase(parser.parse("Point"));
        Object arr = point.allocate(1);
        point.set(arr, 0, List.of(1.5, 2.5));
        assertEquals(List.of(1.5, 2.5), point.get(arr, 0));
    }
}
