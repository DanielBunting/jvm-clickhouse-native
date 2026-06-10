package io.github.danielbunting.clickhouse

/**
 * Allocation-lean bulk insert: accumulates rows into per-column primitive buffers
 * and ships them as native data blocks, bypassing any row-by-row SQL path. This is
 * CH.Native's single biggest performance margin (Spec 2) — optimize the
 * implementation hardest here.
 *
 * ```java
 * try (BulkInserter<User> inserter = conn.createBulkInserter("users", User.class)) {
 *     inserter.init();            // fetch target schema (sample block), build column buffers
 *     inserter.addRange(users);   // accumulate into per-column primitive buffers
 *     inserter.complete();        // flush data block(s); empty block terminates
 * }
 * ```
 *
 * **Contract frozen in W0.2.** Implementation is task W2.4.
 *
 * @param T the row type, mapped to columns by the object mapper (W1.E3)
 */
public interface BulkInserter<T> : AutoCloseable {

    /** Sends the insert query and reads the server's sample block to learn the target schema. */
    public fun init()

    /** Accumulates one row into the column buffers, flushing a block when a batch fills. */
    public fun add(row: T)

    /** Convenience bulk form of [add]. */
    public fun addRange(rows: Iterable<T>)

    /** Flushes any buffered rows and sends the terminating empty block. */
    public fun complete()

    override fun close()
}
