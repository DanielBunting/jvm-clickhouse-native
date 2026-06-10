package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryAs
import io.github.danielbunting.clickhouse.kotlin.queryParametersOf
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Exercises primary-constructor binding end-to-end: [queryAs] constructs **immutable** Kotlin
 * data classes — `val`s, no default values, no no-arg constructor — straight from result
 * columns matched to constructor parameters by name. Also proves `@JvmRecord` types still go
 * through the core's record mapper, and that NULL-into-non-nullable fails loudly, not with a
 * silent zero.
 */
@Tag("integration")
class DataClassMappingIT : ClickHouseIntegrationTest() {

    /** Fully idiomatic: immutable, no defaults — unconstructable by the core's POJO mapper. */
    data class Event(val id: Long, val name: String, val score: Double)

    data class TaggedEvent(val id: Long, val tag: String?)

    /** Narrower numeric parameter than the wire type (UInt32 arrives boxed as a wider value). */
    data class Compact(val id: Int, val weight: Float)

    @JvmRecord
    data class RecordEvent(val id: Long, val name: String)

    data class Total(val total: Long)

    @Test
    fun immutableDataClassRoundTrips() = runBlocking {
        val table = uniqueTable("k_ctor_basic")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, name String, score Float64) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command(
                    "INSERT INTO $table (id, name, score) VALUES (1,'alpha',1.5),(2,'beta',-2.5)",
                )

                val events = conn.queryAs<Event>("SELECT id, name, score FROM $table ORDER BY id")
                    .toList()
                assertEquals(listOf(Event(1, "alpha", 1.5), Event(2, "beta", -2.5)), events)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun nullableParameterReceivesSqlNull() = runBlocking {
        val table = uniqueTable("k_ctor_nullable")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, tag Nullable(String)) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command("INSERT INTO $table (id, tag) VALUES (1,'set'),(2,NULL)")

                val events = conn.queryAs<TaggedEvent>("SELECT id, tag FROM $table ORDER BY id")
                    .toList()
                assertEquals(listOf(TaggedEvent(1, "set"), TaggedEvent(2, null)), events)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun sqlNullIntoNonNullableParameterFailsLoudly() = runBlocking {
        val table = uniqueTable("k_ctor_nullfail")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, tag Nullable(String)) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command("INSERT INTO $table (id, tag) VALUES (1,NULL)")

                val e = assertFailsWith<ClickHouseException> {
                    // TaggedEvent's nullable sibling with a non-null `tag`.
                    conn.queryAs<Event>("SELECT id, tag AS name, 0.0 AS score FROM $table").toList()
                }
                assertTrue("non-nullable" in e.message!!, "got: ${e.message}")

                // The failed flow closed its result; the connection must still serve queries.
                assertEquals(1L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun numericWidthsAdaptToParameterTypes() = runBlocking {
        val table = uniqueTable("k_ctor_widths")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id UInt32, weight Float32) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command("INSERT INTO $table (id, weight) VALUES (7, 1.25)")

                val rows = conn.queryAs<Compact>("SELECT id, weight FROM $table").toList()
                assertEquals(listOf(Compact(7, 1.25f)), rows)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun jvmRecordStillUsesTheCoreRecordMapper() = runBlocking {
        val table = uniqueTable("k_ctor_record")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id Int64, name String) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command("INSERT INTO $table (id, name) VALUES (1,'rec')")

                val rows = conn.queryAs<RecordEvent>("SELECT id, name FROM $table").toList()
                assertEquals(listOf(RecordEvent(1, "rec")), rows)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun bindsAliasedExpressionsAndParameterizedQueries() = runBlocking {
        config().connect().use { conn ->
            val totals = conn.queryAs<Total>(
                "SELECT count() AS total FROM numbers(42)",
            ).toList()
            assertEquals(listOf(Total(42)), totals)

            // Constructor binding composes with the rest of the suspend surface.
            assertEquals(
                9L,
                conn.scalar("SELECT {n:UInt64}", queryParametersOf("n" to 9)),
            )
        }
    }
}
