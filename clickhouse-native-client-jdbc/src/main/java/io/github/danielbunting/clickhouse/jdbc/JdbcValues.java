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
     * Renders a value the way ClickHouse displays it, so {@code getString} on a
     * composite column (Array/Map/Tuple) returns a faithful literal rather than a
     * Java {@code toString}. Lists render as {@code [e1, e2]}, maps as
     * {@code {k: v}}, strings are single-quoted (with {@code \} and {@code '}
     * escaped), numbers/booleans are bare, and other scalars are quoted via their
     * {@code toString}. Recurses for nested composites.
     *
     * @param value the boxed value, may be {@code null}
     * @return the ClickHouse-style literal, or {@code null} if the input is {@code null}
     */
    static String clickHouseLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof java.util.List<?> list) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(clickHouseLiteral(list.get(i)));
            }
            return sb.append(']').toString();
        }
        if (value instanceof java.util.Map<?, ?> map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (java.util.Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(clickHouseLiteral(e.getKey())).append(": ").append(clickHouseLiteral(e.getValue()));
            }
            return sb.append('}').toString();
        }
        if (value.getClass().isArray() && !(value instanceof byte[])) {
            int n = java.lang.reflect.Array.getLength(value);
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(clickHouseLiteral(java.lang.reflect.Array.get(value, i)));
            }
            return sb.append(']').toString();
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        String s = String.valueOf(value);
        StringBuilder sb = new StringBuilder(s.length() + 2).append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '\'') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.append('\'').toString();
    }

    /**
     * True when {@code value} is a composite (List/Map/non-byte array) that should be
     * rendered via {@link #clickHouseLiteral(Object)} by {@code getString}.
     *
     * @param value the boxed value, may be {@code null}
     * @return whether the value is a renderable composite
     */
    static boolean isComposite(Object value) {
        return value instanceof java.util.List
                || value instanceof java.util.Map
                || (value != null && value.getClass().isArray() && !(value instanceof byte[]));
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

        // Unsigned types widen to the next signed JDBC type that holds their full range;
        // types that overflow long (UInt64 and the 128/256-bit ints) become NUMERIC
        // (surfaced as BigInteger). Mirrors upstream clickhouse-java jdbc-v2 JdbcUtils.
        return switch (t) {
            case "Int8" -> Types.TINYINT;
            case "UInt8", "Int16" -> Types.SMALLINT;
            case "UInt16", "Int32" -> Types.INTEGER;
            case "UInt32", "Int64" -> Types.BIGINT;
            case "UInt64", "Int128", "UInt128", "Int256", "UInt256" -> Types.NUMERIC;
            case "Float32" -> Types.REAL;
            case "Float64" -> Types.DOUBLE;
            case "String" -> Types.VARCHAR;
            case "Date", "Date32" -> Types.DATE;
            case "UUID" -> Types.OTHER;
            default -> Types.OTHER;
        };
    }

    /**
     * Returns the JDBC precision (total number of significant digits) for a ClickHouse
     * type, or {@code 0} when precision is not meaningful for the type. Only
     * {@code Decimal(P, S)} carries an explicit precision; {@code Nullable(...)} and
     * {@code LowCardinality(...)} wrappers are stripped first. Total and non-throwing.
     *
     * @param chType the raw ClickHouse type string
     * @return the precision, or {@code 0} if not applicable
     */
    static int precision(String chType) {
        if (chType == null) {
            return 0;
        }
        String t = unwrap(chType.trim());
        if (t.startsWith("Decimal(")) {
            String[] args = typeArgs(t);
            if (args.length >= 1) {
                return parseIntOrZero(args[0]);
            }
        }
        return 0;
    }

    /**
     * Returns the JDBC scale (number of fractional digits) for a ClickHouse type, or
     * {@code 0} when scale is not meaningful. {@code Decimal(P, S)} yields {@code S};
     * {@code DateTime64(p)} and {@code Time64(p)} yield {@code p} (their fractional-second
     * precision). {@code Nullable(...)}/{@code LowCardinality(...)} wrappers are stripped
     * first. Total and non-throwing.
     *
     * @param chType the raw ClickHouse type string
     * @return the scale, or {@code 0} if not applicable
     */
    static int scale(String chType) {
        if (chType == null) {
            return 0;
        }
        String t = unwrap(chType.trim());
        if (t.startsWith("Decimal(")) {
            String[] args = typeArgs(t);
            if (args.length >= 2) {
                return parseIntOrZero(args[1]);
            }
        }
        if (t.startsWith("DateTime64(") || t.startsWith("Time64(")) {
            String[] args = typeArgs(t);
            if (args.length >= 1) {
                return parseIntOrZero(args[0]);
            }
        }
        return 0;
    }

    /**
     * Reinterprets a raw {@code UInt64} bit pattern (which {@code UInt64Codec} boxes as a
     * signed {@code long}) as its true unsigned value. Values with the high bit set — which
     * would otherwise appear negative — become the correct positive {@link java.math.BigInteger}
     * in {@code [0, 2^64-1]}.
     *
     * @param bits the raw 64-bit pattern
     * @return the unsigned value as a {@link java.math.BigInteger}
     */
    static java.math.BigInteger unsignedLong(long bits) {
        java.math.BigInteger low = java.math.BigInteger.valueOf(bits & Long.MAX_VALUE);
        return bits < 0 ? low.setBit(Long.SIZE - 1) : low;
    }

    /** Recursively strips {@code Nullable(...)} / {@code LowCardinality(...)} wrappers. */
    private static String unwrap(String t) {
        if (t.startsWith("Nullable(") && t.endsWith(")")) {
            return unwrap(t.substring("Nullable(".length(), t.length() - 1).trim());
        }
        if (t.startsWith("LowCardinality(") && t.endsWith(")")) {
            return unwrap(t.substring("LowCardinality(".length(), t.length() - 1).trim());
        }
        return t;
    }

    /**
     * Splits the comma-separated parameters of a parameterised type
     * ({@code "Decimal(10, 2)"} -&gt; {@code ["10", " 2"]}). Returns an empty array when
     * there are no parentheses. The parameters relevant here (precision, scale, timezone)
     * contain no nested commas, so a plain split suffices.
     */
    private static String[] typeArgs(String t) {
        int open = t.indexOf('(');
        int close = t.lastIndexOf(')');
        if (open < 0 || close <= open) {
            return new String[0];
        }
        String inner = t.substring(open + 1, close).trim();
        if (inner.isEmpty()) {
            return new String[0];
        }
        return inner.split(",");
    }

    private static int parseIntOrZero(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
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
