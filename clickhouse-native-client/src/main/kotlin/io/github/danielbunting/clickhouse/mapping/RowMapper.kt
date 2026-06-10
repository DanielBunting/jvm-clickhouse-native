package io.github.danielbunting.clickhouse.mapping

/**
 * Maps between a Java object of type [T] and a positional row of column
 * values. Powers `query(sql, Class<T>)` (read path) and
 * [io.github.danielbunting.clickhouse.BulkInserter] (write path).
 *
 * Column order is fixed at mapper-construction time and aligned with the
 * target/result schema; values in [map]/[bind] are positionally
 * aligned with [columnNames].
 *
 * **Contract frozen in W0.2.** The reflection-based implementation and a
 * `forClass(Class<T>)` factory are task W1.E3.
 *
 * @param T the mapped row type
 */
public interface RowMapper<T> {

    /** Column names this mapper reads/writes, in positional order. */
    public fun columnNames(): Array<String>

    /** Builds a `T` from one row of column values (read path). */
    public fun map(columnValues: Array<Any?>): T

    /** Writes [value]'s fields into [dest] positionally (write path). */
    public fun bind(value: T, dest: Array<Any?>)
}
