package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.DefaultTypeParser;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Freshly allocated columns serve a usable default for every supported type
 * (reference: ClickHouseColumnTest#testNewBasicValues — {@code newValue()} for every
 * data type yields a non-null value). Here the equivalent contract is
 * {@code allocate(rowCount)} + {@code get(0)} before any {@code set}: the call must not
 * throw for any parseable type, and the fixed-width families must yield their zero
 * default (matching what the server materializes for a defaulted column).
 */
class CodecDefaultValuesTest {

    private final DefaultTypeParser parser = new DefaultTypeParser();

    @SuppressWarnings("unchecked")
    private ColumnCodec<Object> codec(String type) {
        return (ColumnCodec<Object>) parser.parse(type);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
            "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
            "Float32", "Float64", "BFloat16", "Bool",
            "String", "FixedString(4)", "UUID",
            "Date", "Date32", "DateTime", "DateTime64(3)", "Time", "Time64(3)",
            "Decimal(9, 2)", "Decimal(38, 10)",
            "IPv4", "IPv6",
            "Enum8('a' = 1)", "Enum16('a' = 1)",
            "Array(UInt32)", "Map(String, UInt32)", "Tuple(UInt32, String)",
            "Nullable(UInt32)", "LowCardinality(String)",
            "Point", "Ring", "LineString", "Polygon",
            "Interval" + "Day",
    })
    void freshAllocationServesRowZeroWithoutThrowing(String type) {
        ColumnCodec<Object> c = codec(type);
        Object arr = c.allocate(1);
        assertDoesNotThrow(() -> c.get(arr, 0), type);
    }

    /** Spot-checks: the fixed-width families default to their exact zero values. */
    @Test
    void zeroDefaultsForFixedWidthFamilies() {
        assertEquals(0L, codec("Int64").get(codec("Int64").allocate(1), 0));
        assertEquals(0, codec("Int32").get(codec("Int32").allocate(1), 0));
        assertEquals(0.0, codec("Float64").get(codec("Float64").allocate(1), 0));
        assertEquals(false, codec("Bool").get(codec("Bool").allocate(1), 0));
        assertEquals(LocalDate.ofEpochDay(0), codec("Date").get(codec("Date").allocate(1), 0));
        assertEquals(Duration.ZERO, codec("Time").get(codec("Time").allocate(1), 0));
        assertEquals(0, ((BigDecimal) codec("Decimal(9, 2)")
                .get(codec("Decimal(9, 2)").allocate(1), 0)).compareTo(BigDecimal.ZERO));
    }
}
