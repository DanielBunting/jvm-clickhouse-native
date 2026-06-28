package io.github.danielbunting.clickhouse.adbc

import io.github.danielbunting.clickhouse.ClickHouseConfig
import org.apache.arrow.adbc.core.AdbcDatabase
import org.apache.arrow.adbc.core.AdbcDriver
import org.apache.arrow.memory.BufferAllocator
import java.util.function.Function

/**
 * ADBC entry point for ClickHouse over the native TCP protocol.
 *
 * [open] translates the ADBC parameter map into a [ClickHouseConfig] (see [AdbcParams]) and
 * returns a not-yet-connected [ChAdbcDatabase]. The driver holds a parent [BufferAllocator];
 * each opened database gets a child of it, so the whole Arrow allocator tree is rooted here.
 *
 * Register with `AdbcDriverManager` using [FACTORY] under [DRIVER_NAME], or construct directly
 * with a caller-owned allocator.
 */
public class ChAdbcDriver(private val allocator: BufferAllocator) : AdbcDriver {

    override fun open(parameters: MutableMap<String, Any>): AdbcDatabase {
        val config = AdbcParams.toConfig(parameters)
        val databaseAllocator = allocator.newChildAllocator("adbc-database", 0, Long.MAX_VALUE)
        return try {
            ChAdbcDatabase(config, databaseAllocator)
        } catch (t: Throwable) {
            databaseAllocator.close()
            throw t
        }
    }

    public companion object {
        /** Registration key / ADBC driver name reported by `getInfo`. */
        public const val DRIVER_NAME: String = "clickhouse-native-client-adbc"

        public const val DRIVER_VERSION: String = "0.0"

        /** Factory for `AdbcDriverManager.registerDriver(DRIVER_NAME, FACTORY)`. */
        @JvmField
        public val FACTORY: Function<BufferAllocator, AdbcDriver> =
            Function { allocator -> ChAdbcDriver(allocator) }
    }
}
