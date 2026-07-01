package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import org.junit.jupiter.api.Test;

/**
 * Range-validation tests for {@link DateCodec} and {@link Date32Codec}.
 *
 * <p>ClickHouse's {@code Date} is an unsigned 16-bit day count
 * (1970-01-01..2149-06-06) and {@code Date32} a signed 32-bit day count whose
 * supported display range is 1900-01-01..2299-12-31. Writing a {@link LocalDate}
 * outside those bounds must fail fast rather than silently truncating/wrapping the
 * stored day offset (matching clickhouse-java's {@code BinaryStreamUtils} checks).
 */
class DateRangeValidationTest {

    // ----- Date (UInt16, 1970-01-01..2149-06-06) -----

    @Test
    void date_rejectsPreEpochDate() {
        DateCodec codec = new DateCodec();
        int[] arr = codec.allocate(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.set(arr, 0, LocalDate.of(1969, 12, 31)),
                "Date cannot represent dates before 1970-01-01");
    }

    @Test
    void date_rejectsDateBeyondMax() {
        DateCodec codec = new DateCodec();
        int[] arr = codec.allocate(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.set(arr, 0, LocalDate.of(2149, 6, 7)),
                "Date cannot represent dates after 2149-06-06 (day 65535)");
    }

    @Test
    void date_acceptsBoundaryValues() {
        DateCodec codec = new DateCodec();
        int[] arr = codec.allocate(1);
        assertDoesNotThrow(() -> codec.set(arr, 0, LocalDate.of(1970, 1, 1)));
        assertDoesNotThrow(() -> codec.set(arr, 0, LocalDate.of(2149, 6, 6)));
    }

    // ----- Date32 (Int32, 1900-01-01..2299-12-31) -----

    @Test
    void date32_rejectsDateBelowMin() {
        Date32Codec codec = new Date32Codec();
        int[] arr = codec.allocate(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.set(arr, 0, LocalDate.of(1899, 12, 31)),
                "Date32 cannot represent dates before 1900-01-01");
    }

    @Test
    void date32_rejectsDateAboveMax() {
        Date32Codec codec = new Date32Codec();
        int[] arr = codec.allocate(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.set(arr, 0, LocalDate.of(2300, 1, 1)),
                "Date32 cannot represent dates after 2299-12-31");
    }

    @Test
    void date32_rejectsOutOfRangeStringLiteral() {
        Date32Codec codec = new Date32Codec();
        int[] arr = codec.allocate(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> codec.set(arr, 0, "1800-01-01"),
                "String-parsed dates are range-checked too");
    }

    @Test
    void date32_acceptsBoundaryValues() {
        Date32Codec codec = new Date32Codec();
        int[] arr = codec.allocate(1);
        assertDoesNotThrow(() -> codec.set(arr, 0, LocalDate.of(1900, 1, 1)));
        assertDoesNotThrow(() -> codec.set(arr, 0, LocalDate.of(2299, 12, 31)));
        // Pre-epoch but in range: negative day offset must still be accepted.
        assertDoesNotThrow(() -> codec.set(arr, 0, LocalDate.of(1969, 12, 31)));
    }
}
