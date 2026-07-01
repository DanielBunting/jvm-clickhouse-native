package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Integration coverage for cross-thread query cancellation ({@code ClickHouseConnection.cancel()}
 * → {@code NativeClientImpl.cancel()} writing a {@code Cancel} packet). Previously untested end
 * to end: cancellation originates on a different thread from the one blocked reading results,
 * so these tests exercise exactly that race.
 *
 * <p>Observable contract (see {@code NativeClientImpl}): {@code cancel()} stops an in-flight
 * query early, the reading thread drains to {@code END_OF_STREAM} rather than hanging, the
 * connection is left in sync (reusable), and a {@code cancel()} after {@code close()} quietly
 * no-ops instead of throwing.
 */
class CancellationIT extends TypeRoundTripBase {

    /** A scan far too large to complete quickly, so a prompt cancel is guaranteed to be partial. */
    private static final long HUGE = 2_000_000_000L;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void cancelStopsInFlightStreamingQuery_andConnectionStaysUsable() throws Exception {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            AtomicLong rowsRead = new AtomicLong();

            // Reader thread: stream a huge result, counting rows until cancelled or exhausted.
            CompletableFuture<Void> reader = CompletableFuture.runAsync(() -> {
                try (QueryResult r = conn.query("SELECT number FROM numbers(" + HUGE + ")")) {
                    Iterator<Block> blocks = r.blocks();
                    while (blocks.hasNext()) {
                        rowsRead.addAndGet(blocks.next().rowCount());
                    }
                }
            });

            // Wait until the query is actually streaming, then cancel from this thread.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (rowsRead.get() == 0 && System.nanoTime() < deadline && !reader.isDone()) {
                Thread.sleep(20);
            }
            conn.cancel();

            // The reader must return (not hang) despite the 2-billion-row scan.
            reader.get(30, TimeUnit.SECONDS);
            assertTrue(rowsRead.get() < HUGE,
                    "cancel stopped the scan early; read " + rowsRead.get() + " of " + HUGE);

            // Connection drained to END_OF_STREAM after the cancel, so it is reusable in sync.
            assertEquals(1L, conn.executeScalar("SELECT 1"),
                    "connection remains usable after a cancelled query");
        }
    }

    @Test
    void cancelAfterCloseIsQuietNoOp() {
        ClickHouseConnection conn = ClickHouseConnection.open(config());
        conn.close();
        // A cancel racing with (or following) close must not throw — see NativeClientImpl.cancel.
        assertDoesNotThrow(conn::cancel);
    }
}
