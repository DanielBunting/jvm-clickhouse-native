package io.github.danielbunting.clickhouse.types

/**
 * Resolves ClickHouse type strings to codecs, caching results. Typically backed by
 * a [TypeParser] with a memoization layer so repeated columns of the same
 * type don't re-parse.
 *
 * **Contract frozen in W0.2.** Implementation is task W1.B7.
 */
public interface CodecRegistry {

    /** Resolves (parsing + caching) a full CH type string to a codec. */
    public fun resolve(chType: String): ColumnCodec<*>
}
