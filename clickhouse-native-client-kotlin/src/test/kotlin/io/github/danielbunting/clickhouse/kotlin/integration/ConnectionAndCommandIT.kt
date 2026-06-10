package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.connect
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Exercises the suspend connection/point-op surface against a real server:
 * [connect], [scalar], and [command] (DDL/DML, with and without per-query settings).
 * Mirrors the Java client's `ScalarIT` and `DdlAndCountIT`.
 */
@Tag("integration")
class ConnectionAndCommandIT : ClickHouseIntegrationTest() {

    @Test
    fun scalarLiteralsRoundTrip() = runBlocking {
        config().connect().use { conn ->
            assertEquals(1L, conn.scalar("SELECT 1"))
            assertEquals(42L, conn.scalar("SELECT 42"))
            assertEquals(9_999_999L, conn.scalar("SELECT 9999999"))
            assertEquals(1_000_000_000_000L, conn.scalar("SELECT toUInt64(1000000000000)"))
        }
    }

    @Test
    fun createInsertAndCount() = runBlocking {
        val table = uniqueTable("k_ddl_count")
        config().connect().use { conn ->
            conn.command(
                "CREATE TABLE IF NOT EXISTS $table (id UInt32, name String) " +
                    "ENGINE = MergeTree() ORDER BY id",
            )
            conn.command("INSERT INTO $table (id, name) VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
            try {
                assertEquals(3L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun connectionIsReusableAcrossCommands() = runBlocking {
        val table = uniqueTable("k_reuse")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (val UInt64) ENGINE = MergeTree() ORDER BY val")
            conn.command("INSERT INTO $table (val) VALUES (10),(20)")
            conn.command("INSERT INTO $table (val) VALUES (30),(40),(50)")
            try {
                assertEquals(5L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun commandHonorsPerQuerySettings() = runBlocking {
        val table = uniqueTable("k_settings")
        config().connect().use { conn ->
            conn.command("CREATE TABLE IF NOT EXISTS $table (id UInt32) ENGINE = MergeTree() ORDER BY id")
            // A small max_block_size is harmless server-side; we only assert the call succeeds and
            // the settings overload path is wired through.
            conn.command(
                "INSERT INTO $table (id) SELECT number FROM numbers(100)",
                settings = mapOf("max_block_size" to "10"),
            )
            try {
                assertEquals(100L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }

    @Test
    fun createIfNotExistsIsIdempotent() = runBlocking {
        val table = uniqueTable("k_idempotent")
        val ddl = "CREATE TABLE IF NOT EXISTS $table (id UInt32) ENGINE = MergeTree() ORDER BY id"
        config().connect().use { conn ->
            conn.command(ddl)
            conn.command(ddl) // second time must not throw
            try {
                assertEquals(0L, conn.scalar("SELECT count() FROM $table"))
            } finally {
                conn.command("DROP TABLE IF EXISTS $table")
            }
        }
    }
}
