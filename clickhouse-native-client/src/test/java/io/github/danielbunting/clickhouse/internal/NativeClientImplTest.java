package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.protocol.Block;
import java.time.Duration;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure helpers of {@link NativeClientImpl}. These do not open a
 * socket or talk to a server (those are covered by the integration tests, task D).
 */
class NativeClientImplTest {

    @Test
    void resolveZoneUsesServerTimezoneWhenValid() {
        assertEquals(ZoneId.of("Europe/London"),
                NativeClientImpl.resolveZone("Europe/London"));
        assertEquals(ZoneId.of("UTC"), NativeClientImpl.resolveZone("UTC"));
    }

    @Test
    void resolveZoneFallsBackToUtcForBlankOrUnknown() {
        assertEquals(ZoneId.of("UTC"), NativeClientImpl.resolveZone(null));
        assertEquals(ZoneId.of("UTC"), NativeClientImpl.resolveZone(""));
        assertEquals(ZoneId.of("UTC"), NativeClientImpl.resolveZone("Not/AZone"));
    }

    @Test
    void toMillisClampsAndTreatsNonPositiveAsNoTimeout() {
        assertEquals(0, NativeClientImpl.toMillis(null));
        assertEquals(0, NativeClientImpl.toMillis(Duration.ZERO));
        assertEquals(0, NativeClientImpl.toMillis(Duration.ofSeconds(-5)));
        assertEquals(5_000, NativeClientImpl.toMillis(Duration.ofSeconds(5)));
        assertEquals(Integer.MAX_VALUE,
                NativeClientImpl.toMillis(Duration.ofDays(365_000)));
    }

    @Test
    void emptyBlockIsEmpty() {
        // The empty block that follows a Query and terminates an insert carries no data.
        Block empty = new Block();
        org.junit.jupiter.api.Assertions.assertTrue(empty.isEmpty());
        assertEquals(0, empty.columnCount());
        assertEquals(0, empty.rowCount());
    }
}
