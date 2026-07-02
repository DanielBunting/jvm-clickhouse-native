package io.github.danielbunting.clickhouse.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;

/**
 * A {@link java.sql.Array} over the core client's {@code List} boxing of an
 * {@code Array(T)} column. {@link #getArray()} materialises a Java {@code Object[]},
 * recursively converting nested {@code List}s (so {@code Array(Array(T))} yields an
 * {@code Object[]} of {@code Object[]}). The base type is the ClickHouse element
 * type string with the outer {@code Array(...)} (and any {@code Nullable}/
 * {@code LowCardinality} wrapper) stripped.
 *
 * <p>{@code getResultSet(...)} is not supported; array values are consumed via
 * {@link #getArray()}.
 */
final class ChArray implements Array {

    private final List<?> elements;
    private final String baseTypeName;

    /**
     * @param elements     the element values for this array cell (may contain nested lists)
     * @param baseTypeName the ClickHouse element type name (e.g. {@code "Int32"})
     */
    ChArray(List<?> elements, String baseTypeName) {
        this.elements = elements;
        this.baseTypeName = baseTypeName;
    }

    /**
     * Derives the element type of an {@code Array(T)} type string, unwrapping a
     * leading {@code Nullable(...)}/{@code LowCardinality(...)} first.
     *
     * @param chType the raw ClickHouse column type (e.g. {@code "Array(Nullable(String))"})
     * @return the element type (e.g. {@code "Nullable(String)"}), or {@code chType} if not an array
     */
    static String elementType(String chType) {
        String t = chType == null ? "" : chType.trim();
        if (t.startsWith("Nullable(") && t.endsWith(")")) {
            t = t.substring("Nullable(".length(), t.length() - 1).trim();
        }
        if (t.startsWith("LowCardinality(") && t.endsWith(")")) {
            t = t.substring("LowCardinality(".length(), t.length() - 1).trim();
        }
        if (t.startsWith("Array(") && t.endsWith(")")) {
            return t.substring("Array(".length(), t.length() - 1).trim();
        }
        return t;
    }

    private static Object[] toArray(List<?> list) {
        Object[] out = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object e = list.get(i);
            out[i] = (e instanceof List<?> inner) ? toArray(inner) : e;
        }
        return out;
    }

    @Override
    public String getBaseTypeName() {
        return baseTypeName;
    }

    @Override
    public int getBaseType() {
        return JdbcValues.sqlType(baseTypeName);
    }

    @Override
    public Object getArray() {
        return toArray(elements);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        int from = (int) (index - 1);
        if (index < 1 || from > elements.size() || count < 0) {
            throw new SQLException("Array slice out of range: index=" + index + ", count=" + count
                    + " (size " + elements.size() + ")");
        }
        int to = Math.min(elements.size(), from + count);
        return toArray(elements.subList(from, to));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    @Override
    public void free() {
        // no-op: backed by an already-materialised in-memory list
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw unsupported();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw unsupported();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw unsupported();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw unsupported();
    }

    @Override
    public String toString() {
        return JdbcValues.clickHouseLiteral(elements);
    }

    private static SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException(
                "Array.getResultSet is not supported by clickhouse-native-client JDBC; use getArray()");
    }
}
