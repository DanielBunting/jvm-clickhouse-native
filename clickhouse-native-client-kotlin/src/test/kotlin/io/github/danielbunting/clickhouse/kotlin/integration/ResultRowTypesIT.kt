package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.queryFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Round-trips a spread of column types through a real server and reads them back through
 * [ResultRow][io.github.danielbunting.clickhouse.kotlin.ResultRow] — covering the no-boxing
 * primitive getters, the lazy String path, the boxed `get`, and `Nullable(...)` null-map handling.
 */
@Tag("integration")
class ResultRowTypesIT : ClickHouseIntegrationTest() {

    @Test
    fun primitivesStringsAndBoundaryValues() = runBlocking {
        val table = uniqueTable("k_types_numeric")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (u32 UInt32, i64 Int64, f64 Float64, s String) " +
                    "ENGINE = MergeTree() ORDER BY u32",
            )
            conn.command(
                "INSERT INTO $table VALUES " +
                    "(0, -9223372036854775808, -1.5, 'low'), " +
                    "(4294967295, 9223372036854775807, 3.14159265358979, 'high')",
            )
            try {
                val rows = conn.queryFlow("SELECT u32, i64, f64, s FROM $table ORDER BY u32").toList()
                assertEquals(2, rows.size)

                val low = rows[0]
                assertEquals(0L, low.long("u32"))
                assertEquals(Long.MIN_VALUE, low.long("i64"))
                assertEquals(-1.5, low.double("f64"))
                assertEquals("low", low.string("s"))

                val high = rows[1]
                assertEquals(4_294_967_295L, high.long("u32"))   // UInt32 max — unsigned handling
                assertEquals(Long.MAX_VALUE, high.long("i64"))
                assertEquals(3.14159265358979, high.double("f64"))
                assertEquals("high", high.string("s"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun nullableColumnsHonorNullMap() = runBlocking {
        val table = uniqueTable("k_types_nullable")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table " +
                    "(id UInt32, ni Nullable(Int64), ns Nullable(String)) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            conn.command("INSERT INTO $table VALUES (1, 100, 'present'), (2, NULL, NULL)")
            try {
                val rows = conn.queryFlow("SELECT id, ni, ns FROM $table ORDER BY id").toList()
                assertEquals(2, rows.size)

                val present = rows[0]
                assertFalse(present.isNull("ni"))
                assertEquals(100L, present.longOrNull("ni"))
                assertEquals("present", present.string("ns"))
                assertEquals(100L, present["ni"])               // boxed accessor

                val absent = rows[1]
                assertTrue(absent.isNull("ni"))
                assertNull(absent.longOrNull("ni"))
                assertNull(absent.string("ns"))
                assertNull(absent["ni"])                          // boxed accessor returns null
                assertNull(absent["ns"])
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }
}
