package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link ConnectionException}, {@link ProtocolException}, and
 * {@link ConfigurationException} are all subtypes of {@link ClickHouseException}
 * and that their constructors wire the message and cause through correctly.
 */
class ExceptionHierarchyTest {

    // ------------------------------------------------------------------
    // ConnectionException
    // ------------------------------------------------------------------

    @Test
    void connectionExceptionIsClickHouseException() {
        assertInstanceOf(ClickHouseException.class, new ConnectionException("conn"));
    }

    @Test
    void connectionExceptionPreservesMessage() {
        ConnectionException ex = new ConnectionException("refused");
        assertEquals("refused", ex.getMessage());
    }

    @Test
    void connectionExceptionPreservesMessageAndCause() {
        Throwable cause = new RuntimeException("root");
        ConnectionException ex = new ConnectionException("timeout", cause);
        assertEquals("timeout", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    // ------------------------------------------------------------------
    // ProtocolException
    // ------------------------------------------------------------------

    @Test
    void protocolExceptionIsClickHouseException() {
        assertInstanceOf(ClickHouseException.class, new ProtocolException("proto"));
    }

    @Test
    void protocolExceptionPreservesMessage() {
        ProtocolException ex = new ProtocolException("bad packet type 0x99");
        assertEquals("bad packet type 0x99", ex.getMessage());
    }

    @Test
    void protocolExceptionPreservesMessageAndCause() {
        Throwable cause = new RuntimeException("eof");
        ProtocolException ex = new ProtocolException("unexpected eof", cause);
        assertEquals("unexpected eof", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    // ------------------------------------------------------------------
    // ConfigurationException
    // ------------------------------------------------------------------

    @Test
    void configurationExceptionIsClickHouseException() {
        assertInstanceOf(ClickHouseException.class, new ConfigurationException("cfg"));
    }

    @Test
    void configurationExceptionPreservesMessage() {
        ConfigurationException ex = new ConfigurationException("unknown compression: xz");
        assertEquals("unknown compression: xz", ex.getMessage());
    }

    @Test
    void configurationExceptionPreservesMessageAndCause() {
        Throwable cause = new IllegalArgumentException("bad url");
        ConfigurationException ex = new ConfigurationException("malformed URL", cause);
        assertEquals("malformed URL", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    // ------------------------------------------------------------------
    // Catch-as-base-type: all three are catchable as ClickHouseException.
    // ------------------------------------------------------------------

    @Test
    void allThreeAreCatchableAsClickHouseException() {
        assertCatchable(new ConnectionException("c"));
        assertCatchable(new ProtocolException("p"));
        assertCatchable(new ConfigurationException("f"));
    }

    private void assertCatchable(ClickHouseException ex) {
        // If the cast succeeded at the call site, this method body is a no-op;
        // the real assertion is that no ClassCastException was thrown.
        assertInstanceOf(ClickHouseException.class, ex);
    }
}
