package io.github.danielbunting.clickhouse.jdbc;

import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Calendar;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Server-free contract tests for {@link ChResultSet}, ported from the reference
 * clickhouse-java suites ({@code jdbc-v2 ResultSetImplTest}, {@code jdbc-v1
 * ClickHouseResultSetTest}) and re-targeted at this driver's ACTUAL contract:
 * a forward-only, read-only, statement-detached cursor over native-protocol blocks.
 *
 * <p>Covers the unsupported-operation surface (scrolling, row mutation, updates,
 * stream/LOB getters), cursor-position queries across iteration and for empty
 * results, fetch direction/size semantics, and the JDBC constants.
 */
class ChResultSetContractTest {

    /** Single Int32 column named {@code num} with the given values; cursor un-advanced. */
    private static ChResultSet rows(int... values) {
        Int32Codec codec = new Int32Codec();
        int[] a = codec.allocate(values.length);
        System.arraycopy(values, 0, a, 0, values.length);
        return RsFixtures.open(RsFixtures.col("num", "Int32", codec, a, values.length));
    }

    /** Zero-row result over the same shape (the {@code LIMIT 0} analogue). */
    private static ChResultSet emptyRs() {
        Int32Codec codec = new Int32Codec();
        return RsFixtures.open(RsFixtures.col("num", "Int32", codec, codec.allocate(0), 0));
    }

    // ------------------------------------------------------------------
    // Unsupported operations
    // ------------------------------------------------------------------

    @Test
    void scrollPositioningIsUnsupported() throws SQLException {
        ChResultSet rs = rows(1, 2);
        assertTrue(rs.next());
        Executable[] positioning = {
                rs::beforeFirst,
                rs::afterLast,
                rs::first,
                rs::last,
                () -> rs.absolute(1),
                () -> rs.relative(-1),
                rs::previous,
                rs::moveToInsertRow,
                rs::moveToCurrentRow,
                // isFirst/isLast need look-ahead the streaming cursor cannot do.
                rs::isFirst,
                rs::isLast,
        };
        for (Executable op : positioning) {
            assertThrows(SQLFeatureNotSupportedException.class, op);
        }
    }

    @Test
    void rowMutationIsUnsupported() throws SQLException {
        ChResultSet rs = rows(1);
        assertTrue(rs.next());
        Executable[] mutation = {
                rs::insertRow,
                rs::updateRow,
                rs::deleteRow,
                rs::refreshRow,
                rs::cancelRowUpdates,
                rs::rowUpdated,
                rs::rowInserted,
                rs::rowDeleted,
        };
        for (Executable op : mutation) {
            assertThrows(SQLFeatureNotSupportedException.class, op);
        }
    }

    /**
     * Every {@code updateXxx} overload declared on {@link ResultSet} (except the
     * {@code SQLType} default methods, which the JDK itself rejects) must throw
     * {@link SQLFeatureNotSupportedException}. Driven by reflection so new overloads
     * cannot silently escape coverage; argument values are irrelevant because the
     * read-only guard throws before touching them.
     */
    @Test
    void allUpdateOverloadsThrowFeatureNotSupported() throws SQLException {
        ChResultSet rs = rows(1);
        assertTrue(rs.next());
        int checked = 0;
        for (Method m : ResultSet.class.getMethods()) {
            if (!m.getName().startsWith("update") || m.isDefault()) {
                continue;
            }
            Class<?>[] params = m.getParameterTypes();
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                args[i] = zero(params[i]);
            }
            InvocationTargetException e = assertThrows(InvocationTargetException.class,
                    () -> m.invoke(rs, args), m::toString);
            assertInstanceOf(SQLFeatureNotSupportedException.class, e.getCause(), m.toString());
            checked++;
        }
        assertTrue(checked >= 60, "expected the full updateXxx surface, saw only " + checked);
    }

    /** Default (zero/null) argument for a reflective call; nulls suffice for reference types. */
    private static Object zero(Class<?> t) {
        if (t == boolean.class) {
            return Boolean.FALSE;
        }
        if (t == byte.class) {
            return (byte) 0;
        }
        if (t == short.class) {
            return (short) 0;
        }
        if (t == int.class) {
            return 1;
        }
        if (t == long.class) {
            return 1L;
        }
        if (t == float.class) {
            return 0f;
        }
        if (t == double.class) {
            return 0d;
        }
        return null;
    }

    @Test
    @SuppressWarnings("deprecation")
    void streamAndLobGettersAreUnsupported() throws SQLException {
        ChResultSet rs = rows(1);
        assertTrue(rs.next());
        Executable[] getters = {
                () -> rs.getAsciiStream(1), () -> rs.getAsciiStream("num"),
                () -> rs.getUnicodeStream(1), () -> rs.getUnicodeStream("num"),
                () -> rs.getBinaryStream(1), () -> rs.getBinaryStream("num"),
                () -> rs.getCharacterStream(1), () -> rs.getCharacterStream("num"),
                () -> rs.getNCharacterStream(1), () -> rs.getNCharacterStream("num"),
                () -> rs.getRef(1), () -> rs.getRef("num"),
                () -> rs.getBlob(1), () -> rs.getBlob("num"),
                () -> rs.getClob(1), () -> rs.getClob("num"),
                () -> rs.getNClob(1), () -> rs.getNClob("num"),
                () -> rs.getSQLXML(1), () -> rs.getSQLXML("num"),
                () -> rs.getURL(1), () -> rs.getURL("num"),
                () -> rs.getRowId(1), () -> rs.getRowId("num"),
                () -> rs.getTime(1), () -> rs.getTime("num"),
                () -> rs.getTime(1, Calendar.getInstance()),
                () -> rs.getTime("num", Calendar.getInstance()),
        };
        for (Executable op : getters) {
            assertThrows(SQLFeatureNotSupportedException.class, op);
        }
    }

    /** Getters ChResultSet genuinely supports must keep working, not throw. */
    @Test
    void supportedConvenienceGettersDelegate() throws SQLException {
        ChResultSet rs = rows(42);
        assertTrue(rs.next());
        assertEquals("42", rs.getString(1));
        assertEquals("42", rs.getNString(1), "getNString delegates to getString");
        assertEquals("42", rs.getNString("num"));
        assertEquals(42, ((Number) rs.getObject(1, Map.of())).intValue(),
                "type-map overload ignores the map and reads the plain value");
        assertFalse(rs.wasNull());
    }

    // ------------------------------------------------------------------
    // Cursor position
    // ------------------------------------------------------------------

    @Test
    void cursorPositionAcrossIteration() throws SQLException {
        ChResultSet rs = rows(10, 20);

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(0, rs.getRow());

        assertTrue(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        // getRow is not tracked: support is optional for TYPE_FORWARD_ONLY and this
        // driver always reports 0 (the reference driver reports 1/2 here).
        assertEquals(0, rs.getRow());
        assertEquals(10, rs.getInt(1));

        assertTrue(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(20, rs.getInt(1));

        assertFalse(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertEquals(0, rs.getRow());

        assertFalse(rs.next(), "next() after the end stays after-last");
        assertTrue(rs.isAfterLast());
    }

    @Test
    void cursorPositionOnEmptyResult() throws SQLException {
        ChResultSet rs = emptyRs();

        // Blocks stream lazily, so an empty result still reports before-first
        // until next() actually drains the iterator (reference driver agrees).
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(0, rs.getRow());

        assertFalse(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertEquals(0, rs.getRow());
    }

    // ------------------------------------------------------------------
    // Fetch direction / size
    // ------------------------------------------------------------------

    @Test
    void fetchDirectionIsForwardOnly() throws SQLException {
        ChResultSet rs = rows(1);
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(ResultSet.FETCH_FORWARD); // accepted no-op
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertThrows(SQLFeatureNotSupportedException.class,
                () -> rs.setFetchDirection(ResultSet.FETCH_UNKNOWN));
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    @Test
    void fetchSizeIsAdvisoryNoOp() throws SQLException {
        ChResultSet rs = rows(1);
        assertEquals(0, rs.getFetchSize(), "blocks are server-sized; no client fetch size");
        rs.setFetchSize(10);
        assertEquals(0, rs.getFetchSize(), "setFetchSize hint is ignored");
        // Deliberate contract: the hint is ignored wholesale, negatives included
        // (reference jdbc-v2 instead rejects negative sizes with an SQLException).
        rs.setFetchSize(-1);
        assertEquals(0, rs.getFetchSize());
        // Always detached from the producing statement, so there is no statement
        // fetch size to inherit (jdbc-v1 testFetchSizeOfDetachedResultSet analogue).
        assertNull(rs.getStatement());
    }

    // ------------------------------------------------------------------
    // Constants / misc
    // ------------------------------------------------------------------

    @Test
    void resultSetConstants() throws SQLException {
        ChResultSet rs = rows(1);
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, rs.getHoldability());
        assertNull(rs.getStatement(), "result set is detached from the producing statement");
        assertThrows(SQLFeatureNotSupportedException.class, rs::getCursorName);
        assertNull(rs.getWarnings());
        rs.clearWarnings(); // no-op
        assertNull(rs.getWarnings());
    }
}
