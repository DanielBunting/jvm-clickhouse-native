package io.github.danielbunting.clickhouse.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * A {@link ResultSetMetaData} view over the column metadata carried by a core
 * {@code QueryResult}: positionally-aligned column names and raw ClickHouse type
 * strings. Type mapping to {@link java.sql.Types} is delegated to
 * {@link JdbcValues#sqlType(String)}; nullability is inferred from the
 * {@code Nullable(...)} wrapper in the raw type string.
 *
 * <p>Columns are addressed 1-based, per the JDBC contract.
 */
final class ChResultSetMetaData implements ResultSetMetaData {

    private final List<String> columnNames;
    private final List<String> columnTypes;

    /**
     * Creates metadata over the given positionally-aligned name and type lists.
     *
     * @param columnNames the column names (1-based when accessed via this class)
     * @param columnTypes the raw ClickHouse type strings, aligned with {@code columnNames}
     */
    ChResultSetMetaData(List<String> columnNames, List<String> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    /**
     * Validates and converts a 1-based JDBC column index to a 0-based list index.
     *
     * @param column the 1-based column index
     * @return the 0-based index
     * @throws SQLException if {@code column} is out of range
     */
    private int idx(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Column index out of range: " + column
                    + " (1.." + columnNames.size() + ")");
        }
        return column - 1;
    }

    @Override
    public int getColumnCount() {
        return columnNames.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(idx(column));
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnNames.get(idx(column));
    }

    /** Returns the raw ClickHouse type string for the column, e.g. {@code "Nullable(UInt32)"}. */
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columnTypes.get(idx(column));
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return JdbcValues.sqlType(columnTypes.get(idx(column)));
    }

    @Override
    public int isNullable(int column) throws SQLException {
        String type = columnTypes.get(idx(column)).trim();
        return type.startsWith("Nullable(") ? columnNullable : columnNoNulls;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = getColumnType(column);
        return switch (type) {
            case java.sql.Types.TINYINT -> Byte.class.getName();
            case java.sql.Types.SMALLINT -> Short.class.getName();
            case java.sql.Types.INTEGER -> Integer.class.getName();
            case java.sql.Types.BIGINT -> Long.class.getName();
            case java.sql.Types.REAL -> Float.class.getName();
            case java.sql.Types.DOUBLE -> Double.class.getName();
            case java.sql.Types.DECIMAL -> java.math.BigDecimal.class.getName();
            case java.sql.Types.DATE -> java.sql.Date.class.getName();
            case java.sql.Types.TIMESTAMP -> java.sql.Timestamp.class.getName();
            case java.sql.Types.ARRAY -> java.util.List.class.getName();
            case java.sql.Types.VARCHAR -> String.class.getName();
            default -> Object.class.getName();
        };
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        String type = columnTypes.get(idx(column)).trim();
        if (type.startsWith("Nullable(") && type.endsWith(")")) {
            type = type.substring("Nullable(".length(), type.length() - 1);
        }
        // CH unsigned integers and non-numeric types are reported unsigned.
        return type.startsWith("Int") || type.startsWith("Float") || type.startsWith("Decimal");
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        idx(column);
        return 0;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        idx(column);
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        idx(column);
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        idx(column);
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        idx(column);
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        idx(column);
        return "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        idx(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        idx(column);
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        idx(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        idx(column);
        return false;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        idx(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        idx(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        idx(column);
        return false;
    }

    // -----------------------------------------------------------------------
    // Wrapper
    // -----------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
