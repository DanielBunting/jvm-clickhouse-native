package io.github.danielbunting.clickhouse.internal

import io.github.danielbunting.clickhouse.ConcurrentConnectionUseException
import java.util.concurrent.atomic.AtomicReference

/**
 * Fail-fast mutual-exclusion guard for a single [NativeClient] connection.
 *
 * A connection is one socket and cannot multiplex operations. This guard tracks the
 * thread that currently "owns" the connection's logical operation and rejects a second
 * acquisition with a [ConcurrentConnectionUseException], turning what would
 * otherwise be silent protocol-stream corruption into a clear error.
 *
 * The owning window can outlast a single method call: a lazy `QueryResult`
 * holds the guard until it is fully consumed/closed, and a `BulkInserter` holds it
 * from `init()` to `complete()`/`close()`. Because the releasing thread
 * may differ from the acquiring thread (e.g. an async query whose result is closed
 * elsewhere), this uses an [AtomicReference] rather than a thread-owned lock so any
 * thread may release.
 */
public class ConnectionGuard {

    private val owner = AtomicReference<Thread>()

    /**
     * Marks the connection in use by the current thread.
     *
     * @throws ConcurrentConnectionUseException if it is already in use
     */
    public fun acquire() {
        val current = Thread.currentThread()
        if (!owner.compareAndSet(null, current)) {
            val holder = owner.get()
            val by = if (holder != null) " by thread '" + holder.name + "'" else ""
            throw ConcurrentConnectionUseException(
                "ClickHouseConnection is not thread-safe and is already in use" + by
                    + ". A streaming QueryResult or an open BulkInserter must be closed before the next"
                    + " operation; for concurrent access use a ClickHouseConnectionPool or one connection"
                    + " per thread."
            )
        }
    }

    /**
     * Marks the connection in use by the current thread if it is free, returning
     * whether ownership was taken. The non-throwing variant of [acquire] for callers
     * (e.g. a liveness probe) that must treat "busy" as an answer, not an error.
     *
     * @return `true` if the guard was acquired; `false` if it is already in use
     */
    public fun tryAcquire(): Boolean {
        return owner.compareAndSet(null, Thread.currentThread())
    }

    /** Releases the connection (idempotent; may be called by any thread). */
    public fun release() {
        owner.set(null)
    }

    /** Whether the connection is currently held by some operation. */
    public fun isHeld(): Boolean {
        return owner.get() != null
    }
}
