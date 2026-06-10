package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.QueryParameters

/*
 * Kotlin sugar over [QueryParameters] — the safe, server-side alternative to splicing values
 * into SQL text. The SQL references `{name:Type}` placeholders; values travel separately on the
 * Query packet and the server casts them against the declared `Type`.
 */

/**
 * Builds a [QueryParameters] from `name to value` pairs, converting each value to its
 * ClickHouse textual form via [QueryParameters.toText] (a `null` value binds SQL `NULL`):
 *
 * ```
 * conn.queryFlow(
 *     "SELECT * FROM events WHERE id = {id:UInt32} AND day = {day:Date}",
 *     queryParametersOf("id" to 42, "day" to LocalDate.now()),
 * )
 * ```
 */
fun queryParametersOf(vararg params: Pair<String, Any?>): QueryParameters {
    if (params.isEmpty()) return QueryParameters.EMPTY
    val builder = QueryParameters.builder()
    for ((name, value) in params) {
        builder.bind(name, value)
    }
    return builder.build()
}

/**
 * Builds a [QueryParameters] with the core's fluent [QueryParameters.Builder] as receiver,
 * for when binding is conditional or mixes [bind][QueryParameters.Builder.bind] with
 * verbatim [bindText][QueryParameters.Builder.bindText]:
 *
 * ```
 * val params = queryParameters {
 *     bind("id", 42)
 *     if (until != null) bind("until", until)
 * }
 * ```
 */
fun queryParameters(block: QueryParameters.Builder.() -> Unit): QueryParameters =
    QueryParameters.builder().apply(block).build()
