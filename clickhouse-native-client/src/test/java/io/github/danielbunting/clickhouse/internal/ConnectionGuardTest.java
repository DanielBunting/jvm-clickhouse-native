package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ConcurrentConnectionUseException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ConnectionGuardTest {

    @Test
    void secondAcquireThrowsUntilReleased() {
        ConnectionGuard g = new ConnectionGuard();
        g.acquire();
        assertTrue(g.isHeld());
        assertThrows(ConcurrentConnectionUseException.class, g::acquire);
        g.release();
        assertFalse(g.isHeld());
        assertDoesNotThrow(g::acquire); // re-acquire after release
    }

    @Test
    void releaseIsIdempotentAndSafeWhenNotHeld() {
        ConnectionGuard g = new ConnectionGuard();
        g.release(); // not held — no-op
        g.acquire();
        g.release();
        g.release(); // double release — no-op
        assertFalse(g.isHeld());
    }

    @Test
    void anyThreadMayRelease() throws Exception {
        ConnectionGuard g = new ConnectionGuard();
        g.acquire();
        Thread t = new Thread(g::release);
        t.start();
        t.join();
        assertFalse(g.isHeld());
        assertDoesNotThrow(g::acquire);
    }

    @Test
    void concurrentAcquireFromAnotherThreadThrows() throws Exception {
        ConnectionGuard g = new ConnectionGuard();
        g.acquire(); // held by the test thread
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                g.acquire();
            } catch (Throwable e) {
                err.set(e);
            }
        });
        t.start();
        t.join();
        assertInstanceOf(ConcurrentConnectionUseException.class, err.get());
    }
}
