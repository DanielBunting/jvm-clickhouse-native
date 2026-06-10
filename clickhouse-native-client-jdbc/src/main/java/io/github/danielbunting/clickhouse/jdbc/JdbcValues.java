package io.github.danielbunting.clickhouse.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;

/**
 * Shared, server-free coercion helpers that bridge the core client's boxed
 * values (as produced by {@code io.github.danielbunting.clickhouse.types.Column#value(int)})
 * to the primitive and object returns expected by the {@code java.sql.ResultSet}
 * accessor family, plus a mapping from raw ClickHouse type strings to
 * {@link java.sql.Types} constants.
 *
 * <p>The core boxes ClickHouse types as follows:
 * UInt8/16 &rarr; {@code Integer}; UInt32/UInt64/Int64 &rarr; {@code Long};
 * Int8 &rarr; {@code Byte}; Int16 &rarr; {@code Short}; Int32 &rarr; {@code Integer};
 * Float32 &rarr; {@code Float}; Float64 &rarr; {@code Double};
 * String/FixedString/Enum &rarr; {@code String}; Date &rarr; {@link LocalDate};
 * DateTime/DateTime64 &rarr; {@link Instant}; Decimal &rarr; {@link BigDecimal};
 * UUID &rarr; {@link java.util.UUID}; Array &rarr; {@link java.util.List};
 * Nullable &rarr; {@code null} or the inner boxed value.
 *
 * <p>All methods are {@code static}; this class is not instantiable. The numeric
 * coercions accept any {@link Number} (so a {@code Long}-boxed UInt32 still reads
 * back through {@link #toInt(Object)}), taking care over range where a JDBC
 * accessor narrows. A {@code null} input coerces to the JDBC zero/default for a
 * primitive accessor and to {@code null} for object accessors.
 */
final class JdbcValues {

    private JdbcValues() {
    }

    // -----------------------------------------------------------------------
    // Numeric coercions
    // -----------------------------------------------------------------------

    /**
     * Coerces a boxed value to a {@code boolean}: {@code null} &rarr; {@code false},
     * a {@link Number} &rarr; {@code value != 0}, a {@link String} &rarr; parsed as
     * {@code "true"}/{@code "1"} (case-insensitive), a {@link Boolean} passthrough.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced boolean
     * @throws SQLException if the value cannot be interpreted as a boolean
     */
    static boolean toBoolean(Object value) throws SQLException {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number n) {
            return n.doubleValue() != 0.0;
        }
        if (value instanceof String s) {
            String t = s.trim();
            if (t.equalsIgnoreCase("true") || t.equals("1")) {
                return true;
            }
            if (t.equalsIgnoreCase("false") || t.equals("0") || t.isEmpty()) {
                return false;
            }
            throw new SQLException("Cannot coerce string '" + s + "' to boolean");
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to boolean");
    }

    /**
     * Coerces a boxed value to a {@code byte} with range checking.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced byte ({@code 0} if {@code null})
     * @throws SQLException if the value is non-numeric or out of {@code byte} range
     */
    static byte toByte(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        long l = toLongChecked(value, "byte");
        if (l < Byte.MIN_VALUE || l > Byte.MAX_VALUE) {
            throw new SQLException("Value " + l + " out of range for byte");
        }
        return (byte) l;
    }

    /**
     * Coerces a boxed value to a {@code short} with range checking.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced short ({@code 0} if {@code null})
     * @throws SQLException if the value is non-numeric or out of {@code short} range
     */
    static short toShort(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        long l = toLongChecked(value, "short");
        if (l < Short.MIN_VALUE || l > Short.MAX_VALUE) {
            throw new SQLException("Value " + l + " out of range for short");
        }
        return (short) l;
    }

    /**
     * Coerces a boxed value to an {@code int} with range checking.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced int ({@code 0} if {@code null})
     * @throws SQLException if the value is non-numeric or out of {@code int} range
     */
    static int toInt(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        long l = toLongChecked(value, "int");
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new SQLException("Value " + l + " out of range for int");
        }
        return (int) l;
    }

    /**
     * Coerces a boxed value to a {@code long}.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced long ({@code 0} if {@code null})
     * @throws SQLException if the value cannot be interpreted as a long
     */
    static long toLong(Object value) throws SQLException {
        if (value == null) {
            return 0L;
        }
        return toLongChecked(value, "long");
    }

    /**
     * Coerces a boxed value to a {@code float}.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced float ({@code 0f} if {@code null})
     * @throws SQLException if the value cannot be interpreted as a float
     */
    static float toFloat(Object value) throws SQLException {
        if (value == null) {
            return 0f;
        }
        if (value instanceof Number n) {
            return n.floatValue();
        }
        if (value instanceof String s) {
            try {
                return Float.parseFloat(s.trim());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot coerce string '" + s + "' to float", e);
            }
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to float");
    }

    /**
     * Coerces a boxed value to a {@code double}.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced double ({@code 0d} if {@code null})
     * @throws SQLException if the value cannot be interpreted as a double
     */
    static double toDouble(Object value) throws SQLException {
        if (value == null) {
            return 0d;
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value instanceof String s) {
            try {
                return Double.parseDouble(s.trim());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot coerce string '" + s + "' to double", e);
            }
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to double");
    }

    /**
     * Coerces a boxed value to a {@link BigDecimal}.
     *
     * @param value the boxed value, may be {@code null}
     * @return the coerced {@link BigDecimal}, or {@code null} if the input is {@code null}
     * @throws SQLException if the value cannot be interpreted as a decimal
     */
    static BigDecimal toBigDecimal(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        if (value instanceof java.math.BigInteger bi) {
            return new BigDecimal(bi);
        }
        if (value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Number n) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        if (value instanceof String s) {
            try {
                return new BigDecimal(s.trim());
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot coerce string '" + s + "' to BigDecimal", e);
            }
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to BigDecimal");
    }

    // -----------------------------------------------------------------------
    // String / temporal coercions
    // -----------------------------------------------------------------------

    /**
     * Coerces any boxed value to its {@link String} form via {@link String#valueOf(Object)}.
     *
     * @param value the boxed value, may be {@code null}
     * @return the string form, or {@code null} if the input is {@code null}
     */
    static String toString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        return String.valueOf(value);
    }

    /**
     * Coerces a {@link LocalDate} (the core's boxing of {@code Date}) to a
     * {@link java.sql.Date}. A {@link java.sql.Date} or {@link Instant} input is
     * also accepted for robustness.
     *
     * @param value the boxed value, may be {@code null}
     * @return a {@link java.sql.Date}, or {@code null} if the input is {@code null}
     * @throws SQLException if the value cannot be interpreted as a date
     */
    static Date toDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Date d) {
            return d;
        }
        if (value instanceof LocalDate ld) {
            return Date.valueOf(ld);
        }
        if (value instanceof Instant i) {
            return new Date(i.toEpochMilli());
        }
        if (value instanceof java.util.Date ud) {
            return new Date(ud.getTime());
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to java.sql.Date");
    }

    /**
     * Coerces an {@link Instant} (the core's boxing of {@code DateTime}/{@code DateTime64})
     * to a {@link Timestamp}. A {@link Timestamp} or {@link LocalDate} input is also
     * accepted for robustness.
     *
     * @param value the boxed value, may be {@code null}
     * @return a {@link Timestamp}, or {@code null} if the input is {@code null}
     * @throws SQLException if the value cannot be interpreted as a timestamp
     */
    static Timestamp toTimestamp(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp ts) {
            return ts;
        }
        if (value instanceof Instant i) {
            return Timestamp.from(i);
        }
        if (value instanceof LocalDate ld) {
            return Timestamp.valueOf(ld.atStartOfDay());
        }
        if (value instanceof java.util.Date ud) {
            return new Timestamp(ud.getTime());
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to java.sql.Timestamp");
    }

    // -----------------------------------------------------------------------
    // CH type string -> java.sql.Types
    // -----------------------------------------------------------------------

    /**
     * Maps a raw ClickHouse type string to the closest {@link java.sql.Types}
     * constant. {@code Nullable(T)} unwraps to {@code sqlType(T)};
     * {@code LowCardinality(T)} likewise unwraps. {@code Array(T)} maps to
     * {@link Types#ARRAY}. Unknown types fall back to {@link Types#OTHER}.
     *
     * @param chType the raw ClickHouse type string (may have leading/trailing space)
     * @return the corresponding {@code java.sql.Types} constant
     */
    static int sqlType(String chType) {
        if (chType == null) {
            return Types.OTHER;
        }
        String t = chType.trim();

        if (t.startsWith("Nullable(") && t.endsWith(")")) {
            return sqlType(t.substring("Nullable(".length(), t.length() - 1));
        }
        if (t.startsWith("LowCardinality(") && t.endsWith(")")) {
            return sqlType(t.substring("LowCardinality(".length(), t.length() - 1));
        }
        if (t.startsWith("Array(")) {
            return Types.ARRAY;
        }
        if (t.startsWith("FixedString")) {
            return Types.VARCHAR;
        }
        if (t.startsWith("Decimal")) {
            return Types.DECIMAL;
        }
        if (t.startsWith("DateTime")) {
            // DateTime, DateTime64(...), DateTime('TZ'), DateTime64(3,'TZ')
            return Types.TIMESTAMP;
        }
        if (t.startsWith("Enum")) {
            // Enum8 / Enum16 surface as their string label.
            return Types.VARCHAR;
        }

        return switch (t) {
            case "UInt8", "Int16", "UInt16" -> Types.SMALLINT;
            case "Int8" -> Types.TINYINT;
            case "Int32" -> Types.INTEGER;
            case "UInt32", "Int64", "UInt64" -> Types.BIGINT;
            case "Int128", "UInt128", "Int256", "UInt256" -> Types.DECIMAL;
            case "Float32" -> Types.REAL;
            case "Float64" -> Types.DOUBLE;
            case "String" -> Types.VARCHAR;
            case "Date", "Date32" -> Types.DATE;
            case "UUID" -> Types.OTHER;
            default -> Types.OTHER;
        };
    }

    // -----------------------------------------------------------------------
    // Primitive range checks (non-boxing read path)
    // -----------------------------------------------------------------------

    /**
     * Narrows a primitive {@code long} (already read with no boxing) to a
     * {@code byte}, with the same range check the boxed {@link #toByte(Object)} path
     * applies.
     *
     * @param l the source value
     * @return the value as a {@code byte}
     * @throws SQLException if {@code l} is out of {@code byte} range
     */
    static byte byteFromLong(long l) throws SQLException {
        if (l < Byte.MIN_VALUE || l > Byte.MAX_VALUE) {
            throw new SQLException("Value " + l + " out of range for byte");
        }
        return (byte) l;
    }

    /**
     * Narrows a primitive {@code long} to a {@code short} with range checking.
     *
     * @param l the source value
     * @return the value as a {@code short}
     * @throws SQLException if {@code l} is out of {@code short} range
     */
    static short shortFromLong(long l) throws SQLException {
        if (l < Short.MIN_VALUE || l > Short.MAX_VALUE) {
            throw new SQLException("Value " + l + " out of range for short");
        }
        return (short) l;
    }

    /**
     * Narrows a primitive {@code long} to an {@code int} with range checking.
     *
     * @param l the source value
     * @return the value as an {@code int}
     * @throws SQLException if {@code l} is out of {@code int} range
     */
    static int intFromLong(long l) throws SQLException {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new SQLException("Value " + l + " out of range for int");
        }
        return (int) l;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Coerces a numeric or numeric-string boxed value to a {@code long}, with no
     * range checking (the caller narrows). Floating-point inputs are truncated.
     *
     * @param value  the non-null boxed value
     * @param target the name of the JDBC target type, for error messages
     * @return the value as a {@code long}
     * @throws SQLException if the value is not numeric
     */
    private static long toLongChecked(Object value, String target) throws SQLException {
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        if (value instanceof Boolean b) {
            return b ? 1L : 0L;
        }
        if (value instanceof java.math.BigInteger bi) {
            return bi.longValueExact();
        }
        if (value instanceof BigDecimal bd) {
            return bd.longValueExact();
        }
        if (value instanceof Number n) {
            return (long) n.doubleValue();
        }
        if (value instanceof String s) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                try {
                    return (long) Double.parseDouble(s.trim());
                } catch (NumberFormatException e2) {
                    throw new SQLException("Cannot coerce string '" + s + "' to " + target, e);
                }
            }
        }
        throw new SQLException("Cannot coerce " + value.getClass().getName() + " to " + target);
    }
}
