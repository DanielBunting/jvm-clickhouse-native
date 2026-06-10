package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.query
import io.github.danielbunting.clickhouse.kotlin.queryFlow
import io.github.danielbunting.clickhouse.kotlin.queryParameters
import io.github.danielbunting.clickhouse.kotlin.queryParametersOf
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Exercises server-side query parameters through the suspend/Flow surface: values bound with
 * [queryParametersOf]/[queryParameters] travel separately from the SQL and the *server* casts
 * them against the `{name:Type}` placeholder — no client-side string splicing.
 */
@Tag("integration")
class QueryParametersIT : ClickHouseIntegrationTest() {

    @Test
    fun scalarBindsNumericParameter() = runBlocking {
        config().connect().use { conn ->
            val n = conn.scalar(
                "SELECT count() FROM numbers({n:UInt64})",
                queryParametersOf("n" to 41),
            )
            assertEquals(41L, n)
        }
    }

    @Test
    fun commandBindsParametersInDml() = runBlocking {
        val table = uniqueTable("k_params_dml")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id UInt64) ENGINE = MergeTree() ORDER BY id")
            try {
                conn.command(
                    "INSERT INTO $table SELECT number FROM numbers({n:UInt64})",
                    queryParametersOf("n" to 25),
                )
                assertEquals(25L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun queryFlowFiltersByBoundParameters() = runBlocking {
        val table = uniqueTable("k_params_filter")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id UInt32, name String) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            try {
                conn.command(
                    "INSERT INTO $table (id, name) VALUES (1,'alpha'),(2,'beta'),(3,'gamma'),(4,'beta')",
                )

                val betaIds = conn.queryFlow(
                    "SELECT id FROM $table WHERE name = {name:String} ORDER BY id",
                    queryParametersOf("name" to "beta"),
                ).map { it.long("id") }.toList()
                assertEquals(listOf(2L, 4L), betaIds)

                // A string value is NOT spliced into the SQL: a quote travels safely as data.
                val quoted = conn.query(
                    "SELECT count() AS c FROM $table WHERE name = {name:String}",
                    queryParametersOf("name" to "it's"),
                ) { it.long("c") }.toList()
                assertEquals(listOf(0L), quoted)
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun builderDslBindsConditionally() = runBlocking {
        config().connect().use { conn ->
            val params = queryParameters {
                bind("a", 30)
                bind("b", 12)
            }
            assertEquals(42L, conn.scalar("SELECT {a:UInt32} + {b:UInt32}", params))
        }
    }
}
