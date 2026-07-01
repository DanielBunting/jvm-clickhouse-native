package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ChArray}: element-type derivation, {@code Object[]}
 * materialisation (including nested arrays), slicing, base-type mapping, and the
 * unsupported {@code getResultSet} surface.
 */
class ChArrayTest {

    @Test
    void elementTypeStripsWrappers() {
        assertEquals("Int32", ChArray.elementType("Array(Int32)"));
        assertEquals("Nullable(String)", ChArray.elementType("Array(Nullable(String))"));
        assertEquals("Int32", ChArray.elementType("Nullable(Array(Int32))"));
        assertEquals("Array(Int32)", ChArray.elementType("Array(Array(Int32))"));
        assertEquals("String", ChArray.elementType("String"));
    }

    @Test
    void getArrayAndBaseType() throws SQLException {
        ChArray arr = new ChArray(List.of(1, 2, 3), "Int32");
        assertEquals("Int32", arr.getBaseTypeName());
        assertEquals(java.sql.Types.INTEGER, arr.getBaseType());
        assertArrayEquals(new Object[] {1, 2, 3}, (Object[]) arr.getArray());
    }

    @Test
    void nestedArrayMaterialisesNestedObjectArrays() {
        ChArray arr = new ChArray(List.of(List.of(1, 2), List.of(3)), "Array(Int32)");
        Object[] outer = (Object[]) arr.getArray();
        assertEquals(2, outer.length);
        assertArrayEquals(new Object[] {1, 2}, (Object[]) outer[0]);
        assertArrayEquals(new Object[] {3}, (Object[]) outer[1]);
    }

    @Test
    void slice() throws SQLException {
        ChArray arr = new ChArray(List.of(10, 20, 30, 40), "Int32");
        assertArrayEquals(new Object[] {10, 20}, (Object[]) arr.getArray(1, 2));
        assertArrayEquals(new Object[] {30, 40}, (Object[]) arr.getArray(3, 2));
        // count past the end clamps.
        assertArrayEquals(new Object[] {40}, (Object[]) arr.getArray(4, 10));
    }

    @Test
    void sliceOutOfRangeThrows() {
        ChArray arr = new ChArray(List.of(1, 2), "Int32");
        assertThrows(SQLException.class, () -> arr.getArray(0, 1));
        assertThrows(SQLException.class, () -> arr.getArray(5, 1));
    }

    @Test
    void freeIsNoOpAndResultSetUnsupported() throws SQLException {
        ChArray arr = new ChArray(List.of(1), "Int32");
        arr.free(); // no throw
        assertThrows(SQLFeatureNotSupportedException.class, arr::getResultSet);
    }

    @Test
    void toStringRendersClickHouseLiteral() {
        assertEquals("['a', 'b']", new ChArray(List.of("a", "b"), "String").toString());
    }
}
