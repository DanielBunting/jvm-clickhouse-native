package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link ChResultSetMetaData} — the JDBC {@link ResultSetMetaData} view over a
 * core {@code QueryResult}'s column names and raw ClickHouse type strings. This class previously
 * had no dedicated test. Purely in-memory (package-private constructor); no server.
 *
 * <p>Covers the JDBC type mapping per ClickHouse type, {@code Nullable(...)} nullability
 * inference, signedness, {@code getColumnClassName}, and 1-based index-range validation.
 */
class ChResultSetMetaDataTest {

    // col:      1        2                  3               4            5                6
    private static final List<String> NAMES = List.of("id", "name", "amount", "tags", "ts", "sig");
    private static final List<String> TYPES = List.of(
            "UInt32", "Nullable(String)", "Decimal(10, 2)", "Array(Int8)", "DateTime64(3)", "Int64");

    private static ChResultSetMetaData meta() {
        return new ChResultSetMetaData(NAMES, TYPES);
    }

    @Test
    void columnCountAndNamesAreOneBased() throws SQLException {
        ChResultSetMetaData m = meta();
        assertEquals(6, m.getColumnCount());
        assertEquals("id", m.getColumnName(1));
        assertEquals("name", m.getColumnLabel(2));
        assertEquals("sig", m.getColumnName(6));
    }

    @Test
    void rawTypeNameIsPreserved() throws SQLException {
        assertEquals("Nullable(String)", meta().getColumnTypeName(2));
        assertEquals("DateTime64(3)", meta().getColumnTypeName(5));
    }

    @Test
    void jdbcTypeMappingPerClickHouseType() throws SQLException {
        ChResultSetMetaData m = meta();
        assertEquals(Types.BIGINT, m.getColumnType(1), "UInt32 -> BIGINT");
        assertEquals(Types.VARCHAR, m.getColumnType(2), "Nullable(String) unwraps to VARCHAR");
        assertEquals(Types.DECIMAL, m.getColumnType(3), "Decimal(...) -> DECIMAL");
        assertEquals(Types.ARRAY, m.getColumnType(4), "Array(...) -> ARRAY");
        assertEquals(Types.TIMESTAMP, m.getColumnType(5), "DateTime64(...) -> TIMESTAMP");
        assertEquals(Types.BIGINT, m.getColumnType(6), "Int64 -> BIGINT");
    }

    @Test
    void nullabilityInferredFromNullableWrapper() throws SQLException {
        ChResultSetMetaData m = meta();
        assertEquals(ResultSetMetaData.columnNoNulls, m.isNullable(1), "UInt32 is non-null");
        assertEquals(ResultSetMetaData.columnNullable, m.isNullable(2), "Nullable(String) is nullable");
    }

    @Test
    void signednessFollowsClickHouseNumericFamily() throws SQLException {
        ChResultSetMetaData m = meta();
        assertFalse(m.isSigned(1), "UInt32 is unsigned");
        assertTrue(m.isSigned(3), "Decimal is signed");
        assertFalse(m.isSigned(4), "Array is not a signed numeric");
        assertTrue(m.isSigned(6), "Int64 is signed");
    }

    @Test
    void columnClassNameMatchesMappedJdbcType() throws SQLException {
        ChResultSetMetaData m = meta();
        assertEquals(Long.class.getName(), m.getColumnClassName(1), "BIGINT -> Long");
        assertEquals(String.class.getName(), m.getColumnClassName(2), "VARCHAR -> String");
        assertEquals(BigDecimal.class.getName(), m.getColumnClassName(3), "DECIMAL -> BigDecimal");
        assertEquals(List.class.getName(), m.getColumnClassName(4), "ARRAY -> List");
        assertEquals(java.sql.Timestamp.class.getName(), m.getColumnClassName(5), "TIMESTAMP -> Timestamp");
    }

    @Test
    void precisionAndScaleDerivedFromDecimalType() throws SQLException {
        // Decimal(10, 2): JDBC precision is the total digit count, scale the fractional digits.
        ChResultSetMetaData m = meta();
        assertEquals(10, m.getPrecision(3), "Decimal(10, 2) -> precision 10");
        assertEquals(2, m.getScale(3), "Decimal(10, 2) -> scale 2");
    }

    @Test
    void scaleDerivedFromDateTime64Precision() throws SQLException {
        // DateTime64(3): the (3) is the fractional-seconds precision -> JDBC scale 3.
        assertEquals(3, meta().getScale(5), "DateTime64(3) -> scale 3");
    }

    @Test
    void unsignedAndWideIntsMapToWidenedTypesWithBigIntegerClass() throws SQLException {
        // Parity with clickhouse-java: unsigned/wide ints widen to a JDBC type that holds
        // their full range; UInt64 and the 128/256-bit ints report NUMERIC -> BigInteger.
        ChResultSetMetaData m = new ChResultSetMetaData(
                List.of("a", "b", "c", "d", "e"),
                List.of("UInt16", "UInt64", "Int128", "UInt256", "Nullable(UInt64)"));
        assertEquals(Types.INTEGER, m.getColumnType(1), "UInt16 -> INTEGER");
        assertEquals(Types.NUMERIC, m.getColumnType(2), "UInt64 -> NUMERIC");
        assertEquals(Types.NUMERIC, m.getColumnType(3), "Int128 -> NUMERIC");
        assertEquals(Types.NUMERIC, m.getColumnType(4), "UInt256 -> NUMERIC");
        assertEquals(Types.NUMERIC, m.getColumnType(5), "Nullable(UInt64) unwraps to NUMERIC");

        assertEquals(Integer.class.getName(), m.getColumnClassName(1));
        assertEquals(java.math.BigInteger.class.getName(), m.getColumnClassName(2),
                "NUMERIC maps to BigInteger, not Object");
        assertEquals(java.math.BigInteger.class.getName(), m.getColumnClassName(3));
    }

    @Test
    void indexOutOfRangeThrows() {
        ChResultSetMetaData m = meta();
        assertThrows(SQLException.class, () -> m.getColumnName(0), "column 0 is invalid (1-based)");
        assertThrows(SQLException.class, () -> m.getColumnName(7), "column count+1 is invalid");
        assertThrows(SQLException.class, () -> m.getColumnType(0));
    }

    @Test
    void wrapperContract() throws SQLException {
        ChResultSetMetaData m = meta();
        assertTrue(m.isWrapperFor(ResultSetMetaData.class));
        assertSame(m, m.unwrap(ResultSetMetaData.class));
        assertFalse(m.isWrapperFor(String.class));
        assertThrows(SQLException.class, () -> m.unwrap(String.class));
    }
}
