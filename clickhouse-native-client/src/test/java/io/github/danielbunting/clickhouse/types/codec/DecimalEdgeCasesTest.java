package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Edge-case unit tests for {@link DecimalCodec} that go beyond the happy-path
 * round-trips in {@code B6CodecsTest}:
 * <ul>
 *   <li>HALF_UP rounding on {@code set} when input scale exceeds the column scale</li>
 *   <li>negative values across all four wire widths</li>
 *   <li>scale-boundary rescaling (value scale &lt; column scale is padded)</li>
 *   <li>{@code Decimal256} at P=76 with a large scale</li>
 * </ul>
 *
 * <p>Behavioral facts grounded in the implementation:
 * <ul>
 *   <li>{@code set(BigDecimal)} does {@code d.setScale(scale, HALF_UP)} — extra
 *       fractional digits are rounded, not truncated.</li>
 *   <li>{@code set(Number)} routes through {@code new BigDecimal(n.toString())}
 *       then the same HALF_UP rescale.</li>
 *   <li>{@code get()} returns a {@link BigDecimal} with exactly the column scale
 *       ({@code BigDecimal.valueOf(raw, scale)} or {@code new BigDecimal(raw, scale)}),
 *       so {@code result.scale() == columnScale} always.</li>
 *   <li>A value whose magnitude needs more than {@code precision} significant digits is
 *       rejected on {@code set} with an {@link IllegalArgumentException}.</li>
 * </ul>
 */
class DecimalEdgeCasesTest {

    private static byte[] writeOne(DecimalCodec codec, BigDecimal v) {
        Object src = codec.allocate(1);
        codec.set(src, 0, v);
        return Bytes.capture(w -> codec.write(w, src, 1));
    }

    private static BigDecimal readOne(DecimalCodec codec, byte[] wire) throws IOException {
        Object dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        return (BigDecimal) codec.get(dest, 0);
    }

    // =========================================================================
    // HALF_UP rounding on set (input scale > column scale)
    // =========================================================================

    @Test
    void halfUp_roundsLastDigitUp() throws IOException {
        // Decimal(9,2): 1.005 -> HALF_UP -> 1.01 (unscaled 101)
        DecimalCodec codec = new DecimalCodec(9, 2);
        byte[] wire = writeOne(codec, new BigDecimal("1.005"));
        BigDecimal back = readOne(codec, wire);
        assertEquals(2, back.scale());
        assertEquals(new BigDecimal("1.01"), back);
    }

    @Test
    void halfUp_roundsHalfAwayFromZeroForNegatives() throws IOException {
        // HALF_UP rounds magnitude away from zero: -1.005 -> -1.01
        DecimalCodec codec = new DecimalCodec(9, 2);
        BigDecimal back = readOne(codec, writeOne(codec, new BigDecimal("-1.005")));
        assertEquals(new BigDecimal("-1.01"), back);
    }

    @Test
    void halfUp_belowHalfRoundsDown() throws IOException {
        // 1.004 -> 1.00 ; 1.0049 -> 1.00
        DecimalCodec codec = new DecimalCodec(9, 2);
        assertEquals(new BigDecimal("1.00"), readOne(codec, writeOne(codec, new BigDecimal("1.004"))));
        assertEquals(new BigDecimal("1.00"), readOne(codec, writeOne(codec, new BigDecimal("1.0049"))));
    }

    // =========================================================================
    // Scale-boundary rescaling (value scale < column scale -> zero-padded)
    // =========================================================================

    @Test
    void lowerScaleInput_paddedToColumnScale() throws IOException {
        // Decimal(9,4) given "1.5" stores unscaled 15000 and returns scale-4 "1.5000".
        DecimalCodec codec = new DecimalCodec(9, 4);
        BigDecimal back = readOne(codec, writeOne(codec, new BigDecimal("1.5")));
        assertEquals(4, back.scale());
        assertEquals(new BigDecimal("1.5000"), back);
    }

    @Test
    void scaleZero_integerOnly() throws IOException {
        DecimalCodec codec = new DecimalCodec(9, 0);
        // 7.5 with scale 0 -> HALF_UP -> 8
        BigDecimal back = readOne(codec, writeOne(codec, new BigDecimal("7.5")));
        assertEquals(0, back.scale());
        assertEquals(new BigDecimal("8"), back);
    }

    // =========================================================================
    // Negative values at each wire width
    // =========================================================================

    @Test
    void negative_decimal32WireAndRoundTrip() throws IOException {
        // Decimal(9,2) -0.01 -> unscaled -1 -> Int32 LE = [0xFF,0xFF,0xFF,0xFF]
        DecimalCodec codec = new DecimalCodec(9, 2);
        byte[] wire = writeOne(codec, new BigDecimal("-0.01"));
        assertEquals(4, wire.length);
        for (byte b : wire) {
            assertEquals(0xFF, b & 0xFF);
        }
        assertEquals(new BigDecimal("-0.01"), readOne(codec, wire));
    }

    @Test
    void negative_decimal128RoundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(38, 5);
        BigDecimal v = new BigDecimal("-12345678901234567890.12345");
        BigDecimal back = readOne(codec, writeOne(codec, v));
        assertEquals(0, v.compareTo(back));
    }

    @Test
    void negative_decimal64RoundTrip() throws IOException {
        // Decimal(18,6): a large negative value backed by Int64 (8-byte wire).
        DecimalCodec codec = new DecimalCodec(18, 6);
        BigDecimal v = new BigDecimal("-123456789012.654321");
        byte[] wire = writeOne(codec, v);
        assertEquals(8, wire.length);
        BigDecimal back = readOne(codec, wire);
        assertEquals(6, back.scale());
        assertEquals(0, v.compareTo(back));
    }

    // =========================================================================
    // Decimal256 at P=76 with large scale
    // =========================================================================

    @Test
    void decimal256_p76_largeScaleRoundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(76, 38);
        assertEquals(32, codec.byteWidth());
        // 38 integer digits + 38 fractional digits = 76 significant digits (P=76).
        BigDecimal v = new BigDecimal(
                "12345678901234567890123456789012345678."
              + "12345678901234567890123456789012345678");
        BigDecimal back = readOne(codec, writeOne(codec, v));
        assertEquals(38, back.scale());
        assertEquals(0, v.compareTo(back));
    }

    @Test
    void decimal256_negativeLargeRoundTrip() throws IOException {
        DecimalCodec codec = new DecimalCodec(76, 20);
        BigDecimal v = new BigDecimal(
                "-98765432109876543210987654321098765432.10987654321098765432");
        BigDecimal back = readOne(codec, writeOne(codec, v));
        assertEquals(0, v.compareTo(back));
    }

    // =========================================================================
    // Overflow / open questions (see plan)
    // =========================================================================

    @Test
    void exceedingPrecision_longBacked_throwsIllegalArgument() {
        // Decimal(18,0): a 20-digit value exceeds the 18-digit precision. The codec now
        // rejects it with a clear IllegalArgumentException (rather than the opaque
        // ArithmeticException that longValueExact previously surfaced).
        DecimalCodec codec = new DecimalCodec(18, 0);
        Object arr = codec.allocate(1);
        assertThrows(IllegalArgumentException.class,
                () -> codec.set(arr, 0, new BigDecimal("99999999999999999999"))); // 20 digits > P=18
    }

    @Test
    void exceedingPrecision_bigIntegerBacked_throwsIllegalArgument() {
        // Decimal(38,0): a 39-digit value exceeds P=38 even though the 16-byte Int128 wire
        // could physically hold it — the precision is the authority.
        DecimalCodec codec = new DecimalCodec(38, 0);
        Object arr = codec.allocate(1);
        BigDecimal thirtyNineNines = new BigDecimal("9".repeat(39));
        assertThrows(IllegalArgumentException.class, () -> codec.set(arr, 0, thirtyNineNines));
    }

    @Test
    void maxInPrecision_isAccepted() throws IOException {
        // The largest in-range Decimal(9,2) magnitude is 9999999.99 (9 significant digits).
        DecimalCodec codec = new DecimalCodec(9, 2);
        BigDecimal v = new BigDecimal("9999999.99");
        assertEquals(0, v.compareTo(readOne(codec, writeOne(codec, v))));
    }

    @Test
    void nullSetsZero() throws IOException {
        // set(null) stores BigDecimal.ZERO at the column scale (the codec is null-agnostic;
        // a Nullable wrapper handles actual SQL NULLs one level up).
        DecimalCodec codec = new DecimalCodec(9, 3);
        Object arr = codec.allocate(1);
        codec.set(arr, 0, null);
        BigDecimal back = (BigDecimal) codec.get(arr, 0);
        assertEquals(3, back.scale());
        assertEquals(0, BigDecimal.ZERO.compareTo(back));
    }

    /**
     * Fractional float boundaries convert exactly (reference: NumberConverterTest
     * #testToBigDecimalPreservesFractionalFloatBoundaries): the codec routes Numbers
     * through {@code new BigDecimal(value.toString())}, so {@code 0.0001f} lands as
     * exactly 0.0001 — not the widened binary-double artifact
     * {@code 0.000099999999...} that {@code new BigDecimal((double) f)} would produce.
     */
    @org.junit.jupiter.api.Test
    void floatBoundary_convertsViaToString_notBinaryDouble() {
        DecimalCodec codec = new DecimalCodec(9, 4);
        Object arr = codec.allocate(2);
        codec.set(arr, 0, 0.0001f);
        codec.set(arr, 1, 1.1f);
        assertEquals(new BigDecimal("0.0001"), codec.get(arr, 0));
        assertEquals(new BigDecimal("1.1000"), codec.get(arr, 1));
    }
}
