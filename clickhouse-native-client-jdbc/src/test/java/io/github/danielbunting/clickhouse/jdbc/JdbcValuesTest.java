package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Server-free unit tests for {@link JdbcValues}: the boxed-value coercions used by
 * {@code ChResultSet} accessors and the {@code int sqlType(String)} mapping used by
 * {@code ChResultSetMetaData}. No ClickHouse server is required, so this class is not
 * tagged {@code integration}.
 */
class JdbcValuesTest {

    // -----------------------------------------------------------------------
    // boolean
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("toBoolean")
    class ToBoolean {

        @Test
        void nullIsFalse() throws SQLException {
            assertFalse(JdbcValues.toBoolean(null));
        }

        @Test
        void booleanPassthrough() throws SQLException {
            assertTrue(JdbcValues.toBoolean(Boolean.TRUE));
            assertFalse(JdbcValues.toBoolean(Boolean.FALSE));
        }

        @Test
        void numberNonZeroIsTrue() throws SQLException {
            assertTrue(JdbcValues.toBoolean(1));
            assertTrue(JdbcValues.toBoolean(-5L));
            assertTrue(JdbcValues.toBoolean(0.5d));
            assertFalse(JdbcValues.toBoolean(0));
            assertFalse(JdbcValues.toBoolean(0.0d));
        }

        @Test
        void stringForms() throws SQLException {
            assertTrue(JdbcValues.toBoolean("true"));
            assertTrue(JdbcValues.toBoolean("TRUE"));
            assertTrue(JdbcValues.toBoolean("1"));
            assertFalse(JdbcValues.toBoolean("false"));
            assertFalse(JdbcValues.toBoolean("0"));
            assertFalse(JdbcValues.toBoolean(""));
        }

        @Test
        void unparseableStringThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toBoolean("yes"));
        }
    }

    // -----------------------------------------------------------------------
    // integral coercions with range checking
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("integral coercions")
    class Integral {

        @Test
        void nullsAreZero() throws SQLException {
            assertEquals((byte) 0, JdbcValues.toByte(null));
            assertEquals((short) 0, JdbcValues.toShort(null));
            assertEquals(0, JdbcValues.toInt(null));
            assertEquals(0L, JdbcValues.toLong(null));
        }

        @Test
        void byteInRange() throws SQLException {
            assertEquals((byte) 42, JdbcValues.toByte(42));
            assertEquals(Byte.MIN_VALUE, JdbcValues.toByte((int) Byte.MIN_VALUE));
            assertEquals(Byte.MAX_VALUE, JdbcValues.toByte((int) Byte.MAX_VALUE));
        }

        @Test
        void byteOutOfRangeThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toByte(200));
            assertThrows(SQLException.class, () -> JdbcValues.toByte(-200));
        }

        @Test
        void shortInRange() throws SQLException {
            assertEquals((short) 1000, JdbcValues.toShort(1000));
            assertEquals(Short.MAX_VALUE, JdbcValues.toShort((int) Short.MAX_VALUE));
        }

        @Test
        void shortOutOfRangeThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toShort(40000));
        }

        @Test
        void intInRangeFromLongBoxedUInt32() throws SQLException {
            // A UInt32 boxes as Long in the core; small values must read back via toInt.
            assertEquals(123456, JdbcValues.toInt(123456L));
        }

        @Test
        void intOutOfRangeThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toInt(3_000_000_000L));
        }

        @Test
        void longFromVariousNumbers() throws SQLException {
            assertEquals(9_000_000_000L, JdbcValues.toLong(9_000_000_000L));
            assertEquals(7L, JdbcValues.toLong(7));
            assertEquals(3L, JdbcValues.toLong((short) 3));
            assertEquals(1L, JdbcValues.toLong((byte) 1));
        }

        @Test
        void longFromBooleanAndBigInteger() throws SQLException {
            assertEquals(1L, JdbcValues.toLong(Boolean.TRUE));
            assertEquals(0L, JdbcValues.toLong(Boolean.FALSE));
            assertEquals(42L, JdbcValues.toLong(BigInteger.valueOf(42)));
        }

        @Test
        void longFromNumericString() throws SQLException {
            assertEquals(55L, JdbcValues.toLong("55"));
            assertEquals(2L, JdbcValues.toLong("2.9")); // double fallback truncates
        }

        @Test
        void longFromFloatingTruncates() throws SQLException {
            assertEquals(3L, JdbcValues.toLong(3.99d));
        }

        /**
         * Decimal columns box as BigDecimal; an integral value converts exactly,
         * while a fractional or out-of-long-range value is a JDBC conversion error
         * (SQLException), not a raw ArithmeticException.
         */
        @Test
        void longFromBigDecimalExactOrThrows() throws SQLException {
            assertEquals(42L, JdbcValues.toLong(new BigDecimal("42")));
            assertEquals(Long.MAX_VALUE,
                    JdbcValues.toLong(new BigDecimal("9223372036854775807")));
            assertThrows(SQLException.class,
                    () -> JdbcValues.toLong(new BigDecimal("1.5")),
                    "fractional Decimal cannot narrow to long");
            assertThrows(SQLException.class,
                    () -> JdbcValues.toLong(new BigDecimal("9223372036854775808")),
                    "out-of-range Decimal cannot narrow to long");
        }

        @Test
        void nonNumericStringThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toLong("abc"));
        }
    }

    // -----------------------------------------------------------------------
    // floating coercions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("floating coercions")
    class Floating {

        @Test
        void nullsAreZero() throws SQLException {
            assertEquals(0f, JdbcValues.toFloat(null));
            assertEquals(0d, JdbcValues.toDouble(null));
        }

        @Test
        void floatFromNumberAndString() throws SQLException {
            assertEquals(1.5f, JdbcValues.toFloat(1.5f));
            assertEquals(2.0f, JdbcValues.toFloat(2));
            assertEquals(3.25f, JdbcValues.toFloat("3.25"));
        }

        @Test
        void doubleFromNumberAndString() throws SQLException {
            assertEquals(1.5d, JdbcValues.toDouble(1.5d));
            assertEquals(2.0d, JdbcValues.toDouble(2L));
            assertEquals(3.25d, JdbcValues.toDouble("3.25"));
        }

        @Test
        void unparseableThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toFloat("x"));
            assertThrows(SQLException.class, () -> JdbcValues.toDouble("x"));
        }
    }

    // -----------------------------------------------------------------------
    // BigDecimal
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("toBigDecimal")
    class ToBigDecimal {

        @Test
        void nullIsNull() throws SQLException {
            assertNull(JdbcValues.toBigDecimal(null));
        }

        @Test
        void passthrough() throws SQLException {
            BigDecimal bd = new BigDecimal("12.340");
            assertEquals(bd, JdbcValues.toBigDecimal(bd));
        }

        @Test
        void fromIntegralIsExact() throws SQLException {
            assertEquals(BigDecimal.valueOf(42L), JdbcValues.toBigDecimal(42L));
            assertEquals(BigDecimal.valueOf(7L), JdbcValues.toBigDecimal(7));
        }

        @Test
        void fromBigInteger() throws SQLException {
            assertEquals(new BigDecimal(BigInteger.TEN), JdbcValues.toBigDecimal(BigInteger.TEN));
        }

        @Test
        void fromString() throws SQLException {
            assertEquals(new BigDecimal("1.23"), JdbcValues.toBigDecimal("1.23"));
        }

        @Test
        void unparseableThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toBigDecimal("nope"));
        }
    }

    // -----------------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("toString")
    class ToStringCoercion {

        @Test
        void nullIsNull() {
            assertNull(JdbcValues.toString(null));
        }

        @Test
        void stringPassthrough() {
            assertEquals("hello", JdbcValues.toString("hello"));
        }

        @Test
        void numbersAndOthers() {
            assertEquals("42", JdbcValues.toString(42L));
            assertEquals("3.14", JdbcValues.toString(3.14d));
            assertEquals("true", JdbcValues.toString(Boolean.TRUE));
        }
    }

    // -----------------------------------------------------------------------
    // temporal
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("toDate / toTimestamp")
    class Temporal {

        @Test
        void datesNull() throws SQLException {
            assertNull(JdbcValues.toDate(null));
            assertNull(JdbcValues.toTimestamp(null));
        }

        @Test
        void localDateToSqlDate() throws SQLException {
            LocalDate ld = LocalDate.of(2026, 5, 30);
            Date d = JdbcValues.toDate(ld);
            assertEquals(Date.valueOf(ld), d);
        }

        @Test
        void instantToTimestamp() throws SQLException {
            Instant i = Instant.parse("2026-05-30T12:34:56Z");
            Timestamp ts = JdbcValues.toTimestamp(i);
            assertEquals(Timestamp.from(i), ts);
        }

        /**
         * Nanosecond fidelity (reference: DataTypeUtilsTests
         * #testToLocalDateTimeNanosPreservedWithTimeZone): sub-second precision must
         * survive the Instant->Timestamp conversion to the full nine digits.
         */
        @Test
        void instantToTimestampPreservesNanos() throws SQLException {
            Instant i = Instant.parse("2026-05-30T12:34:56.123456789Z");
            Timestamp ts = JdbcValues.toTimestamp(i);
            assertEquals(123456789, ts.getNanos(), "all nine fractional digits preserved");
            assertEquals(Timestamp.from(i), ts);
        }

        @Test
        void timestampPassthrough() throws SQLException {
            Timestamp ts = Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"));
            assertEquals(ts, JdbcValues.toTimestamp(ts));
        }

        @Test
        void unsupportedTypeThrows() {
            assertThrows(SQLException.class, () -> JdbcValues.toDate(new Object()));
            assertThrows(SQLException.class, () -> JdbcValues.toTimestamp(new Object()));
        }
    }

    // -----------------------------------------------------------------------
    // sqlType
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("sqlType")
    class SqlTypeMapping {

        @Test
        void nullIsOther() {
            assertEquals(Types.OTHER, JdbcValues.sqlType(null));
        }

        @Test
        void integers() {
            // Unsigned types widen to the next signed JDBC type that holds their full
            // range, matching upstream clickhouse-java (jdbc-v2 JdbcUtils).
            assertEquals(Types.TINYINT, JdbcValues.sqlType("Int8"));
            assertEquals(Types.SMALLINT, JdbcValues.sqlType("UInt8"));
            assertEquals(Types.SMALLINT, JdbcValues.sqlType("Int16"));
            assertEquals(Types.INTEGER, JdbcValues.sqlType("UInt16"));
            assertEquals(Types.INTEGER, JdbcValues.sqlType("Int32"));
            assertEquals(Types.BIGINT, JdbcValues.sqlType("UInt32"));
            assertEquals(Types.BIGINT, JdbcValues.sqlType("Int64"));
            assertEquals(Types.NUMERIC, JdbcValues.sqlType("UInt64"));
        }

        @Test
        void wideIntegersAreNumeric() {
            // 128/256-bit ints exceed long and box as BigInteger, so they map to NUMERIC
            // (BigInteger), matching upstream clickhouse-java.
            assertEquals(Types.NUMERIC, JdbcValues.sqlType("Int128"));
            assertEquals(Types.NUMERIC, JdbcValues.sqlType("UInt128"));
            assertEquals(Types.NUMERIC, JdbcValues.sqlType("Int256"));
            assertEquals(Types.NUMERIC, JdbcValues.sqlType("UInt256"));
        }

        @Test
        void floats() {
            assertEquals(Types.REAL, JdbcValues.sqlType("Float32"));
            assertEquals(Types.DOUBLE, JdbcValues.sqlType("Float64"));
        }

        @Test
        void stringLike() {
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("String"));
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("FixedString(16)"));
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("Enum8('a' = 1)"));
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("Enum16('a' = 1)"));
        }

        @Test
        void temporal() {
            assertEquals(Types.DATE, JdbcValues.sqlType("Date"));
            assertEquals(Types.DATE, JdbcValues.sqlType("Date32"));
            assertEquals(Types.TIMESTAMP, JdbcValues.sqlType("DateTime"));
            assertEquals(Types.TIMESTAMP, JdbcValues.sqlType("DateTime('UTC')"));
            assertEquals(Types.TIMESTAMP, JdbcValues.sqlType("DateTime64(3)"));
            assertEquals(Types.TIMESTAMP, JdbcValues.sqlType("DateTime64(3, 'UTC')"));
        }

        @Test
        void decimal() {
            assertEquals(Types.DECIMAL, JdbcValues.sqlType("Decimal(10, 2)"));
            assertEquals(Types.DECIMAL, JdbcValues.sqlType("Decimal64(4)"));
        }

        @Test
        void uuidIsOther() {
            assertEquals(Types.OTHER, JdbcValues.sqlType("UUID"));
        }

        @Test
        void arrayIsArray() {
            assertEquals(Types.ARRAY, JdbcValues.sqlType("Array(UInt32)"));
            assertEquals(Types.ARRAY, JdbcValues.sqlType("Array(Nullable(String))"));
        }

        @Test
        void nullableUnwraps() {
            assertEquals(Types.INTEGER, JdbcValues.sqlType("Nullable(Int32)"));
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("Nullable(String)"));
            assertEquals(Types.TIMESTAMP, JdbcValues.sqlType("Nullable(DateTime64(3))"));
        }

        @Test
        void lowCardinalityUnwraps() {
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("LowCardinality(String)"));
            assertEquals(Types.VARCHAR, JdbcValues.sqlType("LowCardinality(Nullable(String))"));
        }

        @Test
        void leadingTrailingWhitespaceTolerated() {
            assertEquals(Types.INTEGER, JdbcValues.sqlType("  Int32  "));
        }

        @Test
        void unknownIsOther() {
            assertEquals(Types.OTHER, JdbcValues.sqlType("Tuple(UInt8, String)"));
        }
    }

    // -----------------------------------------------------------------------
    // clickHouseLiteral / isComposite (getString rendering of composites)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("clickHouseLiteral")
    class ClickHouseLiteral {

        @Test
        void nullRendersAsKeyword() {
            assertEquals("NULL", JdbcValues.clickHouseLiteral(null));
        }

        @Test
        void numbersAndBooleansAreBare() {
            assertEquals("42", JdbcValues.clickHouseLiteral(42));
            assertEquals("3.5", JdbcValues.clickHouseLiteral(3.5));
            assertEquals("true", JdbcValues.clickHouseLiteral(Boolean.TRUE));
        }

        @Test
        void stringsAreQuotedAndEscaped() {
            assertEquals("'hi'", JdbcValues.clickHouseLiteral("hi"));
            assertEquals("'O\\'Brien'", JdbcValues.clickHouseLiteral("O'Brien"));
            assertEquals("'a\\\\b'", JdbcValues.clickHouseLiteral("a\\b"));
        }

        @Test
        void listsRenderWithSpacedSeparator() {
            assertEquals("[1, 2, 3]", JdbcValues.clickHouseLiteral(java.util.List.of(1, 2, 3)));
            assertEquals("['a', 'b']", JdbcValues.clickHouseLiteral(java.util.List.of("a", "b")));
            assertEquals("[]", JdbcValues.clickHouseLiteral(java.util.List.of()));
        }

        @Test
        void nestedListsRecurse() {
            assertEquals("[[1, 2], [3]]",
                    JdbcValues.clickHouseLiteral(java.util.List.of(java.util.List.of(1, 2), java.util.List.of(3))));
        }

        @Test
        void mapsRenderKeyColonValue() {
            java.util.Map<String, Integer> m = new java.util.LinkedHashMap<>();
            m.put("a", 1);
            m.put("b", 2);
            assertEquals("{'a': 1, 'b': 2}", JdbcValues.clickHouseLiteral(m));
        }

        @Test
        void javaArraysRenderLikeLists() {
            assertEquals("[1, 2]", JdbcValues.clickHouseLiteral(new int[] {1, 2}));
            assertEquals("['x', 'y']", JdbcValues.clickHouseLiteral(new String[] {"x", "y"}));
        }
    }

    @Nested
    @DisplayName("isComposite")
    class IsComposite {

        @Test
        void trueForListMapAndNonByteArray() {
            assertTrue(JdbcValues.isComposite(java.util.List.of(1)));
            assertTrue(JdbcValues.isComposite(new java.util.HashMap<>()));
            assertTrue(JdbcValues.isComposite(new int[] {1}));
        }

        @Test
        void falseForScalarsNullAndByteArray() {
            assertFalse(JdbcValues.isComposite(null));
            assertFalse(JdbcValues.isComposite("s"));
            assertFalse(JdbcValues.isComposite(42));
            assertFalse(JdbcValues.isComposite(new byte[] {1, 2}));
        }
    }
}
