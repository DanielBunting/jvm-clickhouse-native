package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryAs
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Exercises [queryAs] — the reified mapping Flow. The mutable all-default `data class` here is
 * a Kotlin class, so it routes through primary-constructor binding (immutable shapes and the
 * dispatch rules are covered in [DataClassMappingIT]). Also proves early termination (`take`)
 * closes the underlying result and releases the connection guard: the connection stays usable
 * afterwards.
 */
@Tag("integration")
class QueryAsIT : ClickHouseIntegrationTest() {

    data class Event(var id: Long = 0, var name: String = "", var score: Double = 0.0)

    @Test
    fun mapsRowsToDataClassInstances() = runBlocking {
        val table = uniqueTable("k_queryas")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, name String, score Float64) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command(
                    "INSERT INTO $table (id, name, score) VALUES (1,'alpha',1.5),(2,'beta',2.5)",
                )

                val events = conn.queryAs<Event>("SELECT id, name, score FROM $table ORDER BY id")
                    .toList()
                assertEquals(listOf(Event(1, "alpha", 1.5), Event(2, "beta", 2.5)), events)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun earlyTerminationReleasesTheConnection() = runBlocking {
        val table = uniqueTable("k_queryas_take")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, name String, score Float64) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command("INSERT INTO $table SELECT number, 'n', 0.0 FROM numbers(10000)")

                val first = conn.queryAs<Event>("SELECT id, name, score FROM $table ORDER BY id")
                    .take(1)
                    .toList()
                assertEquals(listOf(0L), first.map { it.id })

                // The cancelled flow must have closed the stream and released the guard:
                // the very same connection serves the next query.
                assertEquals(10_000L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }
}
