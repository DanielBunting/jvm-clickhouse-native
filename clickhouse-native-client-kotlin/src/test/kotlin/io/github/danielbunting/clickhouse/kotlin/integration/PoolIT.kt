package io.github.danielbunting.clickhouse.kotlin.integration

import io.github.danielbunting.clickhouse.kotlin.command
import io.github.danielbunting.clickhouse.kotlin.lease
import io.github.danielbunting.clickhouse.kotlin.openPool
import io.github.danielbunting.clickhouse.kotlin.scalar
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Exercises the pool extensions against a real server: [openPool] construction, [lease]
 * borrow/return (including return-on-failure), and genuinely concurrent leases — the pool is
 * the unit of concurrency, one coroutine per leased connection.
 */
@Tag("integration")
class PoolIT : ClickHouseIntegrationTest() {

    @Test
    fun leaseRunsSuspendOpsAndReturnsTheConnection() = runBlocking {
        config().openPool(size = 2).use { pool ->
            val one = pool.lease { conn -> conn.scalar("SELECT 1") }
            assertEquals(1L, one)
            assertEquals(2, pool.available(), "lease must return its connection")
        }
    }

    @Test
    fun leaseReturnsTheConnectionWhenTheBlockThrows() = runBlocking {
        config().openPool(size = 1).use { pool ->
            assertFailsWith<IllegalStateException> {
                pool.lease<Nothing> { throw IllegalStateException("boom") }
            }
            assertEquals(1, pool.available(), "permit must be released after a failed block")
            // The single slot must be borrowable again.
            assertEquals(7L, pool.lease { conn -> conn.scalar("SELECT 7") })
        }
    }

    @Test
    fun concurrentLeasesRunInParallelCoroutines() = runBlocking {
        val table = uniqueTable("k_pool_conc")
        config().openPool(size = 4).use { pool ->
            pool.lease { conn ->
                conn.command(
                    "CREATE TABLE IF NOT EXISTS $table (id UInt64) ENGINE = MergeTree() ORDER BY id",
                )
            }
            try {
                coroutineScope {
                    (1..8).map { i ->
                        async(Dispatchers.Default) {
                            pool.lease { conn ->
                                conn.command("INSERT INTO $table (id) VALUES ($i)")
                            }
                        }
                    }.awaitAll()
                }
                assertEquals(8L, pool.lease { conn -> conn.scalar("SELECT count() FROM $table") })
                assertEquals(
                    (1..8).sum().toLong(),
                    pool.lease { conn -> conn.scalar("SELECT sum(id) FROM $table") },
                )
                assertEquals(4, pool.available(), "all leases must have been returned")
            } finally {
                pool.lease { conn -> conn.command("DROP TABLE IF EXISTS $table") }
            }
        }
    }

    @Test
    fun openPoolHonoursSizeAndExposesIt() = runBlocking {
        config().openPool(size = 3, validateOnBorrow = false).use { pool ->
            assertEquals(3, pool.size())
            assertEquals(3, pool.available())
        }
    }
}
