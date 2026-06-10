package io.github.danielbunting.clickhouse.pool

import io.github.danielbunting.clickhouse.ClickHouseConfig
import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.ClickHouseException
import java.time.Duration
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import java.util.function.Function

/**
 * A fixed-size pool of [ClickHouseConnection]s for safe concurrent use.
 *
 * A single connection is a non-thread-safe socket. This pool hands out independent
 * connections, each used by exactly one borrower at a time, so `N` threads can run
 * `N` operations concurrently. Use it via try-with-resources — a borrowed
 * connection's [close()][ClickHouseConnection.close] returns it to the pool rather
 * than closing the socket:
 *
 * ```java
 * try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.create(config, 8)) {
 *     try (ClickHouseConnection conn = pool.borrow()) {
 *         conn.execute("INSERT INTO t ...");
 *     }
 *     // or, leak-proof:
 *     long n = pool.withConnection(c -> c.executeScalar("SELECT count() FROM t"));
 * }
 * ```
 *
 * **Capacity** is governed by a [Semaphore] of `size` permits, not by a
 * fixed set of socket instances. A borrow acquires a permit (blocking up to the configured
 * timeout) and then either reuses an idle connection or — if none is cached for that slot —
 * opens a fresh one. Connections are still opened eagerly at construction (fail fast if
 * ClickHouse is unreachable), but the permit model makes the pool **self-healing**: when a
 * connection is discarded (see below), its permit is released and the *next* borrow lazily
 * opens a replacement for that slot over a hopefully-healthy network. A transient outage can
 * therefore never permanently shrink the pool.
 *
 * **Hygiene.** On return, a [poisoned][ClickHouseConnection.isPoisoned]
 * connection — its protocol stream desynced, its socket broken, or a bulk INSERT abandoned
 * mid-stream — is closed and never recycled (invalidate-on-return). When `validateOnBorrow`
 * is enabled (default), each reused connection is additionally checked with `SELECT 1`
 * before being handed out, catching connections that merely died while idle (which set no poison
 * flag). Either way the slot self-heals by opening a fresh connection on the next borrow.
 *
 * The pool itself is thread-safe.
 */
public class ClickHouseConnectionPool private constructor(b: Builder) : AutoCloseable {

    private val config: ClickHouseConfig
    private val size: Int
    private val borrowTimeoutMillis: Long
    private val validateOnBorrow: Boolean

    /** One permit per slot; acquiring a permit is the right to hold one connection. */
    private val permits: Semaphore

    /** Reusable healthy connections. Bounded by the permit count, so it never exceeds `size`. */
    private val idle: Queue<ClickHouseConnection>

    private val closed = AtomicBoolean(false)

    init {
        if (b.size <= 0) {
            throw IllegalArgumentException("pool size must be positive: " + b.size)
        }
        this.config = b.config
        this.size = b.size
        this.borrowTimeoutMillis = b.borrowTimeout.toMillis()
        this.validateOnBorrow = b.validateOnBorrow
        this.permits = Semaphore(b.size, true)
        this.idle = ConcurrentLinkedQueue()

        val opened = ArrayList<ClickHouseConnection>(b.size)
        try {
            for (i in 0 until b.size) {
                opened.add(ClickHouseConnection.open(config))
            }
        } catch (e: RuntimeException) {
            for (c in opened) {
                closeQuietly(c)
            }
            throw ClickHouseException("Failed to initialize ClickHouse connection pool", e)
        }
        idle.addAll(opened)
    }

    /**
     * Borrows a connection, blocking up to the configured timeout. The returned
     * connection is exclusively the caller's until its `close()` returns it.
     *
     * @return a pooled connection (close it to return it to the pool)
     * @throws ClickHouseException if the pool is closed or no connection is available within the timeout
     */
    public fun borrow(): ClickHouseConnection {
        if (closed.get()) {
            throw ClickHouseException("Connection pool is closed")
        }
        val acquired: Boolean
        try {
            acquired = permits.tryAcquire(borrowTimeoutMillis, TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw ClickHouseException("Interrupted while borrowing a connection", e)
        }
        if (!acquired) {
            throw ClickHouseException(
                "Connection pool exhausted: no connection available within "
                    + borrowTimeoutMillis + "ms (size=" + size + ")"
            )
        }
        // We now hold one of `size` permits. Reuse a cached idle connection for this slot, or
        // open a fresh one if none is cached (the slot's previous connection was discarded as
        // poisoned/dead — this is where the pool self-heals). Any failure before we hand the
        // connection out must release the permit so the slot is not lost.
        var handedOut = false
        try {
            if (closed.get()) {
                throw ClickHouseException("Connection pool is closed")
            }
            var conn: ClickHouseConnection? = idle.poll()
            if (conn != null && !healthy(conn)) {
                closeQuietly(conn)
                conn = null
            }
            if (conn == null) {
                conn = ClickHouseConnection.open(config)
            }
            val pooled: ClickHouseConnection = PooledConnection(conn, this)
            handedOut = true
            return pooled
        } finally {
            if (!handedOut) {
                permits.release()
            }
        }
    }

    /** Borrows a connection, runs [work], and always returns the connection. */
    public fun <T> withConnection(work: Function<ClickHouseConnection, T>): T {
        borrow().use { conn ->
            return work.apply(conn)
        }
    }

    /**
     * Borrows a connection, runs [work], and always returns the connection. (Named
     * distinctly from [withConnection] to avoid lambda overload ambiguity.)
     */
    public fun useConnection(work: Consumer<ClickHouseConnection>) {
        borrow().use { conn ->
            work.accept(conn)
        }
    }

    /** Total number of connections the pool may hold (its permit count). */
    public fun size(): Int {
        return size
    }

    /** Number of slots currently free to borrow (permits not checked out). */
    public fun available(): Int {
        return permits.availablePermits()
    }

    /** Closes the pool and all its connections. Idempotent; further [borrow] throws. */
    override fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        drainIdle()
    }

    /** Returns a borrowed underlying connection (recycling it if healthy) and frees its permit. */
    internal fun returnConnection(conn: ClickHouseConnection) {
        val recycle = !closed.get() && !conn.isPoisoned()
        try {
            if (recycle) {
                idle.offer(conn)
                if (closed.get()) {
                    // The pool was closed concurrently with this return; make sure the connection
                    // we just published doesn't escape close()'s drain and leak.
                    drainIdle()
                }
            } else {
                // Poisoned (stream desynced / socket broken / abandoned mid-INSERT) or pool closed:
                // never recycle. We do NOT open a replacement here — releasing the permit frees the
                // slot, and the next borrow() lazily opens a fresh connection for it. A synchronous
                // replace here could fail during a transient outage and shrink the pool forever.
                closeQuietly(conn)
            }
        } finally {
            permits.release()
        }
    }

    /** Whether a recycled connection may be handed out: not poisoned, and (if configured) passes `SELECT 1`. */
    private fun healthy(conn: ClickHouseConnection): Boolean {
        if (conn.isPoisoned()) {
            return false
        }
        if (!validateOnBorrow) {
            return true
        }
        return try {
            conn.executeScalar("SELECT 1")
            true
        } catch (invalid: RuntimeException) {
            // Dead, or left dirty/in-use by a prior borrower: caller discards and opens fresh.
            false
        }
    }

    private fun drainIdle() {
        var c: ClickHouseConnection? = idle.poll()
        while (c != null) {
            closeQuietly(c)
            c = idle.poll()
        }
    }

    /** Builder for [ClickHouseConnectionPool]. */
    public class Builder internal constructor(config: ClickHouseConfig?) {
        internal val config: ClickHouseConfig
        internal var size: Int = 8
        internal var borrowTimeout: Duration = Duration.ofSeconds(10)
        internal var validateOnBorrow: Boolean = true

        init {
            if (config == null) {
                throw IllegalArgumentException("config must not be null")
            }
            this.config = config
        }

        public fun size(size: Int): Builder {
            this.size = size
            return this
        }

        public fun borrowTimeout(borrowTimeout: Duration): Builder {
            this.borrowTimeout = borrowTimeout
            return this
        }

        public fun validateOnBorrow(validateOnBorrow: Boolean): Builder {
            this.validateOnBorrow = validateOnBorrow
            return this
        }

        public fun build(): ClickHouseConnectionPool {
            return ClickHouseConnectionPool(this)
        }
    }

    public companion object {

        /** Creates a pool of [size] connections with default settings (10s borrow timeout, validate-on-borrow). */
        @JvmStatic
        public fun create(config: ClickHouseConfig?, size: Int): ClickHouseConnectionPool {
            return builder(config).size(size).build()
        }

        @JvmStatic
        public fun builder(config: ClickHouseConfig?): Builder {
            return Builder(config)
        }

        private fun closeQuietly(conn: ClickHouseConnection) {
            try {
                conn.close()
            } catch (ignored: RuntimeException) {
                // best-effort
            }
        }
    }
}
