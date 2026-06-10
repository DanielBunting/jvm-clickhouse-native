package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure scalar-coercion helper in {@link ClickHouseConnectionImpl}.
 * No server or transport is involved — these exercise {@code coerceLong} only.
 */
class ClickHouseConnectionImplTest {

    @Test
    void coercesIntegerNumbers() {
        assertEquals(42L, ClickHouseConnectionImpl.coerceLong(42));
        assertEquals(42L, ClickHouseConnectionImpl.coerceLong((short) 42));
        assertEquals(42L, ClickHouseConnectionImpl.coerceLong((byte) 42));
        assertEquals(9_999_999_999L, ClickHouseConnectionImpl.coerceLong(9_999_999_999L));
    }

    @Test
    void coercesFloatingPointByTruncation() {
        assertEquals(3L, ClickHouseConnectionImpl.coerceLong(3.9d));
        assertEquals(2L, ClickHouseConnectionImpl.coerceLong(2.1f));
    }

    @Test
    void coercesBoolean() {
        assertEquals(1L, ClickHouseConnectionImpl.coerceLong(Boolean.TRUE));
        assertEquals(0L, ClickHouseConnectionImpl.coerceLong(Boolean.FALSE));
    }

    @Test
    void coercesNumericString() {
        assertEquals(123L, ClickHouseConnectionImpl.coerceLong("123"));
        assertEquals(-7L, ClickHouseConnectionImpl.coerceLong("  -7 "));
    }

    @Test
    void rejectsNull() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConnectionImpl.coerceLong(null));
    }

    @Test
    void rejectsNonNumericString() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConnectionImpl.coerceLong("abc"));
    }

    @Test
    void rejectsUncoercibleType() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConnectionImpl.coerceLong(new Object()));
    }
}
