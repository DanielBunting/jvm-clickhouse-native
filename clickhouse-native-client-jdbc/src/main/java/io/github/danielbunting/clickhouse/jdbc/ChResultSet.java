package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A forward-only, read-only {@link ResultSet} over a core {@code QueryResult}.
 *
 * <p>The result is consumed lazily: {@link #next()} advances the row cursor within
 * the current {@link Block}; when a block is exhausted it pulls the next non-empty
 * block from {@code QueryResult.blocks()}, returning {@code false} once the iterator
 * is drained. Cell values are read through {@link Column#value(int)} (already
 * null-aware and boxed) and coerced to JDBC return types by {@link JdbcValues}.
 *
 * <p>{@link #wasNull()} reflects whether the most recently read column was SQL
 * {@code NULL}. Scrollable, updatable, streaming and LOB operations are not
 * supported and throw {@link SQLFeatureNotSupportedException}.
 */
final class ChResultSet implements ResultSet {

    private final QueryResult result;
    private final List<String> columnNames;
    private final Iterator<Block> blocks;

    private Block currentBlock;
    private int rowInBlock = -1;
    private boolean closed;
    private boolean wasNull;
    private boolean afterLast;

    /**
     * Wraps a core {@link QueryResult}. The block iterator is pulled lazily; no
     * data is read until {@link #next()} is first called.
     *
     * @param result the core query result to expose; must not be {@code null}
     */
    ChResultSet(QueryResult result) {
        this.result = result;
        this.columnNames = result.columnNames();
        this.blocks = result.blocks();
    }

    // -----------------------------------------------------------------------
    // Cursor
    // -----------------------------------------------------------------------

    @Override
    public boolean next() throws SQLException {
        ensureOpen();
        if (afterLast) {
            return false;
        }
        try {
            while (true) {
                if (currentBlock != null && rowInBlock + 1 < currentBlock.rowCount()) {
                    rowInBlock++;
                    return true;
                }
                if (!blocks.hasNext()) {
                    currentBlock = null;
                    afterLast = true;
                    return false;
                }
                Block next = blocks.next();
                if (next == null || next.isEmpty()) {
                    continue; // skip empty/terminator blocks
                }
                currentBlock = next;
                rowInBlock = -1;
                // loop continues; will advance into row 0 on the next iteration
            }
        } catch (io.github.danielbunting.clickhouse.ClickHouseException e) {
            // A server error can arrive MID-STREAM (e.g. max_result_rows overflow or a
            // max_execution_time abort) from the lazy block pull; per the JDBC contract
            // it must surface as an SQLException, not a raw unchecked ClickHouseException.
            afterLast = true;
            if (e instanceof io.github.danielbunting.clickhouse.ServerException se) {
                throw new SQLException(se.getMessage(), null, se.code(), se);
            }
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            currentBlock = null;
            result.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean wasNull() throws SQLException {
        ensureOpen();
        return wasNull;
    }

    // -----------------------------------------------------------------------
    // Value access — raw cell read
    // -----------------------------------------------------------------------

    /**
     * Reads the raw boxed cell value at the current row for a 1-based column
     * index, updating {@link #wasNull}.
     *
     * @param columnIndex the 1-based column index
     * @return the boxed value (possibly {@code null})
     * @throws SQLException if closed, no current row, or the index is out of range
     */
    private Object raw(int columnIndex) throws SQLException {
        ensureOpen();
        if (currentBlock == null || rowInBlock < 0) {
            throw new SQLException("No current row; call next() first");
        }
        if (columnIndex < 1 || columnIndex > currentBlock.columnCount()) {
            throw new SQLException("Column index out of range: " + columnIndex
                    + " (1.." + currentBlock.columnCount() + ")");
        }
        Column column = currentBlock.column(columnIndex - 1);
        Object value = column.value(rowInBlock);
        wasNull = (value == null);
        // UInt64 boxes as a raw signed long; the logical/boxed accessors (getObject,
        // getString, getBigDecimal, ...) that flow through here surface its true unsigned
        // value as a BigInteger, matching the Types.NUMERIC metadata. The primitive
        // getLong/getInt path reads col.longAt() directly and keeps the raw-bits contract.
        if (value instanceof Long l && column.codec() instanceof UInt64Codec) {
            return JdbcValues.unsignedLong(l);
        }
        return value;
    }

    /**
     * Resolves and bounds-checks the {@link Column} at a 1-based index, after the
     * usual open/cursor guards. Does not touch {@link #wasNull}.
     */
    private Column columnAt(int columnIndex) throws SQLException {
        ensureOpen();
        if (currentBlock == null || rowInBlock < 0) {
            throw new SQLException("No current row; call next() first");
        }
        if (columnIndex < 1 || columnIndex > currentBlock.columnCount()) {
            throw new SQLException("Column index out of range: " + columnIndex
                    + " (1.." + currentBlock.columnCount() + ")");
        }
        return currentBlock.column(columnIndex - 1);
    }

    /**
     * True when {@code column}'s codec maps to a numeric Java wrapper and therefore
     * exposes a real primitive {@link Column#longAt}/{@link Column#doubleAt} path
     * (the Int, UInt and Float families). Reference and temporal/enum columns
     * (String, BigDecimal, UUID, LocalDate, Instant) return {@code false} and fall
     * back to the boxed path so the existing {@link JdbcValues} coercions are preserved.
     */
    private static boolean isPrimitiveNumeric(Column column) {
        Class<?> jt = column.codec().javaType();
        return jt == Long.class || jt == Integer.class || jt == Short.class
                || jt == Byte.class || jt == Double.class || jt == Float.class;
    }

    /** True when {@code column}'s codec is integer-backed (exposes a real {@code getLong}). */
    private static boolean isPrimitiveInteger(Column column) {
        Class<?> jt = column.codec().javaType();
        return jt == Long.class || jt == Integer.class || jt == Short.class || jt == Byte.class;
    }

    /** True when {@code column}'s codec is float-backed (exposes a real {@code getDouble}). */
    private static boolean isPrimitiveFloat(Column column) {
        Class<?> jt = column.codec().javaType();
        return jt == Double.class || jt == Float.class;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        ensureOpen();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Unknown column label: " + columnLabel);
    }

    // -----------------------------------------------------------------------
    // Typed getters by index
    // -----------------------------------------------------------------------

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object v = raw(columnIndex);
        // Composite columns (Array/Map/Tuple) render as a ClickHouse literal rather
        // than a Java collection toString (see issue clickhouse-java#2723).
        if (JdbcValues.isComposite(v)) {
            return JdbcValues.clickHouseLiteral(v);
        }
        return JdbcValues.toString(v);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return JdbcValues.toBoolean(raw(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        if (isPrimitiveInteger(col)) {
            if (col.nulls() != null && col.nulls()[rowInBlock]) {
                wasNull = true;
                return 0;
            }
            wasNull = false;
            return JdbcValues.byteFromLong(col.longAt(rowInBlock));
        }
        return JdbcValues.toByte(raw(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        if (isPrimitiveInteger(col)) {
            if (col.nulls() != null && col.nulls()[rowInBlock]) {
                wasNull = true;
                return 0;
            }
            wasNull = false;
            return JdbcValues.shortFromLong(col.longAt(rowInBlock));
        }
        return JdbcValues.toShort(raw(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        if (isPrimitiveInteger(col)) {
            if (col.nulls() != null && col.nulls()[rowInBlock]) {
                wasNull = true;
                return 0;
            }
            wasNull = false;
            return JdbcValues.intFromLong(col.longAt(rowInBlock));
        }
        return JdbcValues.toInt(raw(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        if (isPrimitiveInteger(col)) {
            if (col.nulls() != null && col.nulls()[rowInBlock]) {
                wasNull = true;
                return 0L;
            }
            wasNull = false;
            return col.longAt(rowInBlock);
        }
        return JdbcValues.toLong(raw(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        if (isPrimitiveFloat(col)) {
            if (col.nulls() != null && col.nulls()[rowInBlock]) {
                wasNull = true;
                return 0f;
            }
            wasNull = false;
            return (float) col.doubleAt(rowInBlock);
        }
        return JdbcValues.toFloat(raw(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        if (isPrimitiveFloat(col) || isPrimitiveInteger(col)) {
            if (col.nulls() != null && col.nulls()[rowInBlock]) {
                wasNull = true;
                return 0d;
            }
            wasNull = false;
            // Integer-backed columns widen cleanly via getLong; float-backed via getDouble.
            return isPrimitiveFloat(col) ? col.doubleAt(rowInBlock) : (double) col.longAt(rowInBlock);
        }
        return JdbcValues.toDouble(raw(columnIndex));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = JdbcValues.toBigDecimal(raw(columnIndex));
        return bd == null ? null : bd.setScale(scale, java.math.RoundingMode.HALF_UP);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return JdbcValues.toBigDecimal(raw(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return JdbcValues.toDate(raw(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return JdbcValues.toTimestamp(raw(columnIndex));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return raw(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object v = raw(columnIndex);
        if (v == null) {
            return null;
        }
        if (v instanceof byte[] b) {
            return b;
        }
        if (v instanceof String s) {
            return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new SQLException("Cannot read " + v.getClass().getName() + " as byte[]");
    }

    // -----------------------------------------------------------------------
    // Typed getters by label — delegate to index via findColumn
    // -----------------------------------------------------------------------

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() {
        return new ChResultSetMetaData(result.columnNames(), result.columnTypes());
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // no-op
    }

    @Override
    public String getCursorName() throws SQLException {
        throw unsupported("getCursorName");
    }

    @Override
    public Statement getStatement() {
        return null;
    }

    @Override
    public int getType() {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw unsupported("setFetchDirection(non-forward)");
        }
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) {
        // hint ignored; blocks are server-sized
    }

    @Override
    public int getHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    // -----------------------------------------------------------------------
    // Cursor-position queries (forward-only support)
    // -----------------------------------------------------------------------

    @Override
    public boolean isBeforeFirst() {
        return currentBlock == null && rowInBlock < 0 && !afterLast;
    }

    @Override
    public boolean isAfterLast() {
        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw unsupported("isFirst");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw unsupported("isLast");
    }

    @Override
    public int getRow() {
        return 0;
    }

    // -----------------------------------------------------------------------
    // Object getters with type map / Calendar — supported via plain path
    // -----------------------------------------------------------------------

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object v = raw(columnIndex);
        if (v == null) {
            return null;
        }
        if (type.isInstance(v)) {
            return type.cast(v);
        }
        Object coerced = coerceTo(type, v);
        if (coerced != null) {
            return type.cast(coerced);
        }
        throw new SQLException("Cannot convert " + v.getClass().getName()
                + " to " + type.getName());
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    /**
     * Attempts a coercion of a boxed value to a requested target type using
     * {@link JdbcValues}. Returns {@code null} when no coercion is known.
     */
    private Object coerceTo(Class<?> type, Object v) throws SQLException {
        if (type == String.class) {
            return JdbcValues.toString(v);
        }
        if (type == Boolean.class) {
            return JdbcValues.toBoolean(v);
        }
        if (type == Byte.class) {
            return JdbcValues.toByte(v);
        }
        if (type == Short.class) {
            return JdbcValues.toShort(v);
        }
        if (type == Integer.class) {
            return JdbcValues.toInt(v);
        }
        if (type == Long.class) {
            return JdbcValues.toLong(v);
        }
        if (type == Float.class) {
            return JdbcValues.toFloat(v);
        }
        if (type == Double.class) {
            return JdbcValues.toDouble(v);
        }
        if (type == BigDecimal.class) {
            return JdbcValues.toBigDecimal(v);
        }
        if (type == Date.class) {
            return JdbcValues.toDate(v);
        }
        if (type == Timestamp.class) {
            return JdbcValues.toTimestamp(v);
        }
        // Zoned/local views of a temporal column: derived from the boxed Instant at UTC
        // (the driver's temporal contract is the absolute instant; the column timezone
        // only affects how the SERVER interprets wall-clock literals). Callers wanting a
        // different zone call withZoneSameInstant/atZone themselves.
        if (type == java.time.Instant.class && v instanceof java.time.Instant) {
            return v;
        }
        if (type == java.time.ZonedDateTime.class && v instanceof java.time.Instant instant) {
            return instant.atZone(java.time.ZoneOffset.UTC);
        }
        if (type == java.time.OffsetDateTime.class && v instanceof java.time.Instant instant) {
            return instant.atOffset(java.time.ZoneOffset.UTC);
        }
        if (type == java.time.LocalDateTime.class && v instanceof java.time.Instant instant) {
            return java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
        }
        if (type == java.time.LocalDate.class && v instanceof java.time.LocalDate) {
            return v;
        }
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
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

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private static SQLFeatureNotSupportedException unsupported(String op) {
        return new SQLFeatureNotSupportedException(op + " is not supported by clickhouse-native-client JDBC");
    }

    // =======================================================================
    // Unsupported: scrolling cursor movement
    // =======================================================================

    @Override
    public void beforeFirst() throws SQLException {
        throw unsupported("beforeFirst");
    }

    @Override
    public void afterLast() throws SQLException {
        throw unsupported("afterLast");
    }

    @Override
    public boolean first() throws SQLException {
        throw unsupported("first");
    }

    @Override
    public boolean last() throws SQLException {
        throw unsupported("last");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw unsupported("absolute");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw unsupported("relative");
    }

    @Override
    public boolean previous() throws SQLException {
        throw unsupported("previous");
    }

    // =======================================================================
    // Unsupported: streaming / LOB reads
    // =======================================================================

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw unsupported("getAsciiStream");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw unsupported("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw unsupported("getBinaryStream");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw unsupported("getAsciiStream");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw unsupported("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw unsupported("getBinaryStream");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw unsupported("getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw unsupported("getCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw unsupported("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw unsupported("getNCharacterStream");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw unsupported("getRef");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw unsupported("getRef");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw unsupported("getBlob");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw unsupported("getBlob");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw unsupported("getClob");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw unsupported("getClob");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw unsupported("getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw unsupported("getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw unsupported("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw unsupported("getSQLXML");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        Column col = columnAt(columnIndex);
        Object v = col.value(rowInBlock);
        wasNull = (v == null);
        if (v == null) {
            return null;
        }
        if (!(v instanceof List<?> list)) {
            throw new SQLException("Column " + columnIndex + " (" + col.type()
                    + ") is not an array");
        }
        return new ChArray(list, ChArray.elementType(col.type()));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw unsupported("getURL");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw unsupported("getURL");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw unsupported("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw unsupported("getRowId");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw unsupported("getTime");
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw unsupported("getTime");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw unsupported("getTime");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw unsupported("getTime");
    }

    // =======================================================================
    // Unsupported: row insert/update/delete (read-only result set)
    // =======================================================================

    @Override
    public boolean rowUpdated() throws SQLException {
        throw unsupported("rowUpdated");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw unsupported("rowInserted");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw unsupported("rowDeleted");
    }

    @Override
    public void insertRow() throws SQLException {
        throw unsupported("insertRow");
    }

    @Override
    public void updateRow() throws SQLException {
        throw unsupported("updateRow");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw unsupported("deleteRow");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw unsupported("refreshRow");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw unsupported("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw unsupported("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw unsupported("moveToCurrentRow");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw unsupported("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw unsupported("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw unsupported("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw unsupported("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw unsupported("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw unsupported("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw unsupported("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw unsupported("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw unsupported("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw unsupported("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw unsupported("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw unsupported("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw unsupported("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw unsupported("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw unsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw unsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw unsupported("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw unsupported("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw unsupported("updateObject");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw unsupported("updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw unsupported("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw unsupported("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw unsupported("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw unsupported("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw unsupported("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw unsupported("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw unsupported("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw unsupported("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw unsupported("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw unsupported("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw unsupported("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw unsupported("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw unsupported("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw unsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw unsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw unsupported("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw unsupported("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw unsupported("updateObject");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw unsupported("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw unsupported("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw unsupported("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw unsupported("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw unsupported("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw unsupported("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw unsupported("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw unsupported("updateArray");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw unsupported("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw unsupported("updateRowId");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw unsupported("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw unsupported("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw unsupported("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw unsupported("updateNClob");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw unsupported("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw unsupported("updateSQLXML");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw unsupported("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw unsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw unsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw unsupported("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw unsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw unsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw unsupported("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw unsupported("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw unsupported("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw unsupported("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw unsupported("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw unsupported("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw unsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw unsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw unsupported("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw unsupported("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw unsupported("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw unsupported("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw unsupported("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw unsupported("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw unsupported("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw unsupported("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw unsupported("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw unsupported("updateNClob");
    }
}
