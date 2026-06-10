package io.github.danielbunting.clickhouse.types

import java.time.ZoneId
import java.util.concurrent.ConcurrentHashMap

/**
 * Default [CodecRegistry]: delegates to a [DefaultTypeParser] and memoizes
 * the resulting codecs keyed by the raw ClickHouse type string, so repeated columns of
 * the same type don't re-parse.
 *
 * Codecs are treated as immutable, stateless and thread-safe for the lifetime of a
 * registry; the cache is a [ConcurrentHashMap] so resolution is safe across
 * connection threads.
 */
public class DefaultCodecRegistry
/**
 * Creates a registry backed by the given parser.
 *
 * @param parser the parser used on cache misses, never `null`
 */
public constructor(parser: TypeParser?) : CodecRegistry {

    private val parser: TypeParser
    private val cache = ConcurrentHashMap<String, ColumnCodec<*>>()

    init {
        if (parser == null) {
            throw IllegalArgumentException("parser must not be null")
        }
        this.parser = parser
    }

    /** Creates a registry backed by a UTC-default [DefaultTypeParser]. */
    public constructor() : this(DefaultTypeParser())

    /** Creates a registry whose parser uses [defaultZone] for date/time types. */
    public constructor(defaultZone: ZoneId?) : this(DefaultTypeParser(defaultZone))

    override fun resolve(chType: String): ColumnCodec<*> {
        return cache.computeIfAbsent(chType, parser::parse)
    }
}
