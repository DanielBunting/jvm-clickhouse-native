package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for the {@link ParameterMetaData} exposed by {@link ChPreparedStatement}:
 * parameter count vs placeholders, the documented defaults, the {@code clearParameters}
 * count-preservation regression (clickhouse-java#2299), and the wrapper contract.
 */
class ChParameterMetaDataTest {

    private static ChPreparedStatement ps(String sql) {
        ChConnection c = new ChConnection(new FakeCore(), "jdbc:chnative://localhost:9000/default", new Properties());
        return new ChPreparedStatement(c, sql);
    }

    @Test
    void parameterCountMatchesPlaceholders() throws SQLException {
        ParameterMetaData md = ps("INSERT INTO t (a, b, c) VALUES (?, ?, ?)").getParameterMetaData();
        assertEquals(3, md.getParameterCount());
        // A '?' inside a string literal does not count.
        assertEquals(1, ps("SELECT ? WHERE note = 'really?'").getParameterMetaData().getParameterCount());
    }

    /** Regression for clickhouse-java#2299: clearParameters must not change the parameter count. */
    @Test
    void clearParametersPreservesCount() throws SQLException {
        ChPreparedStatement ps = ps("INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
        assertEquals(3, ps.getParameterMetaData().getParameterCount());
        ps.setString(1, "x");
        ps.setString(2, "y");
        ps.setInt(3, 1);
        ps.clearParameters();
        assertEquals(3, ps.getParameterMetaData().getParameterCount());
    }

    @Test
    void documentedDefaults() throws SQLException {
        ParameterMetaData md = ps("SELECT ?").getParameterMetaData();
        assertEquals(ParameterMetaData.parameterNullableUnknown, md.isNullable(1));
        assertTrue(md.isSigned(1));
        assertEquals(0, md.getPrecision(1));
        assertEquals(0, md.getScale(1));
        assertEquals(java.sql.Types.OTHER, md.getParameterType(1));
        assertEquals("String", md.getParameterTypeName(1));
        assertEquals(Object.class.getName(), md.getParameterClassName(1));
        assertEquals(ParameterMetaData.parameterModeIn, md.getParameterMode(1));
    }

    @Test
    void wrapperContract() throws SQLException {
        ParameterMetaData md = ps("SELECT ?").getParameterMetaData();
        assertTrue(md.isWrapperFor(ParameterMetaData.class));
        assertSame(md, md.unwrap(ParameterMetaData.class));
        assertFalse(md.isWrapperFor(String.class));
        assertThrows(SQLException.class, () -> md.unwrap(String.class));
    }
}
