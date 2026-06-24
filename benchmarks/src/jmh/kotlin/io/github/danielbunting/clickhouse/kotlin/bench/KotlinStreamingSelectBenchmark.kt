package io.github.danielbunting.clickhouse.kotlin.bench

import io.github.danielbunting.clickhouse.ClickHouseConnection
import io.github.danielbunting.clickhouse.bench.BenchRow
import io.github.danielbunting.clickhouse.bench.ClickHouseResource
import io.github.danielbunting.clickhouse.bench.SyntheticData
import io.github.danielbunting.clickhouse.compress.CompressionMethod
import io.github.danielbunting.clickhouse.kotlin.query
import io.github.danielbunting.clickhouse.kotlin.queryBatched
import io.github.danielbunting.clickhouse.protocol.Block
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit

/**
 * Quantifies the cost of the Kotlin coroutine/Flow streaming layer relative to the Java core, on
 * the same 1M-row `bench` dataset the other benchmarks use. Four lanes read the identical SELECT:
 *
 *  - [ours_kotlin_flow]         — the per-row `query(sql) { mapper }` Flow path → one object per
 *                                 row, collected under `runBlocking` (how a caller consumes it).
 *  - [ours_kotlin_flow_batched] — `queryBatched(sql, 100_000) { mapper }` → the same objects, but
 *                                 emitted in 100k-row `List`s. Batching amortises the per-row
 *                                 `flowOn(Dispatchers.IO)` channel handoff (~1M → ~10 crossings).
 *  - [ours_java_mapped]         — the Java row-oriented baseline: `query(sql, Class<T>)` lazy
 *                                 mapped `Stream` → one object per row. The non-Flow apples-to-
 *                                 apples reference for both Kotlin lanes.
 *  - [ours_java_columnar]       — the Java zero-boxing column-major path: read the primitive
 *                                 backing arrays directly. The ceiling the row-oriented APIs trade
 *                                 away.
 *
 * The per-row Flow lane is expected to be much slower (a [ResultRow] per row + a Flow channel
 * `emit` across the `flowOn` boundary *per row* + the constant `runBlocking` cost). The batched
 * lane isolates how much of that is the per-element channel crossing: it should land near
 * [ours_java_mapped]. The point is to *measure* the ergonomics tax and guard against regressions.
 *
 * Run with the `gc` profiler (already enabled in `jmh { profilers.add("gc") }`) to get ms/op and
 * MB/op:
 *
 * ```
 * ./gradlew :benchmarks:jmh -PjmhIncludes=KotlinStreamingSelectBenchmark
 * ```
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
open class KotlinStreamingSelectBenchmark {

    /**
     * Mapped row type. `@JvmRecord` makes it a real Java record so the core's reflection mapper
     * ([ClickHouseConnection.query] with a `Class`) binds it via the canonical constructor — a
     * plain Kotlin `data class` would not work there. The Flow lane maps with an explicit lambda,
     * so it accepts the same type.
     */
    @JvmRecord
    data class MappedRow(val id: Long, val value: Double)

    /** Number of rows to pre-load and SELECT. Parameterised so it can be overridden via JMH. */
    @Param("1000000")
    @JvmField
    var rows: Int = 0

    private lateinit var resource: ClickHouseResource
    private lateinit var nativeConn: ClickHouseConnection

    @Setup(Level.Trial)
    fun setup() {
        resource = ClickHouseResource()
        resource.setUp()

        nativeConn = resource.openNative(CompressionMethod.NONE)
        resource.recreateTable(nativeConn)

        val data = SyntheticData.generate(rows)
        nativeConn.createBulkInserter(SyntheticData.TABLE, BenchRow::class.java).use { ins ->
            ins.init()
            ins.addRange(data)
            ins.complete()
        }
    }

    @TearDown(Level.Trial)
    fun teardown() {
        nativeConn.close()
        resource.tearDown()
    }

    /** Kotlin coroutine/Flow lane: `query { mapper }` → objects, collected under `runBlocking`. */
    @Benchmark
    fun ours_kotlin_flow(bh: Blackhole) {
        runBlocking {
            nativeConn.query(SELECT_SQL) { row -> MappedRow(row.long("id"), row.double("value")) }
                .collect { bh.consume(it) }
        }
    }

    /** Batched Kotlin Flow lane: `queryBatched { mapper }` → 100k-row lists, collected under `runBlocking`. */
    @Benchmark
    fun ours_kotlin_flow_batched(bh: Blackhole) {
        runBlocking {
            nativeConn.queryBatched(SELECT_SQL, BATCH_SIZE) { row -> MappedRow(row.long("id"), row.double("value")) }
                .collect { batch -> bh.consume(batch) }
        }
    }

    /** Java row-oriented baseline: lazy `query(sql, Class<T>)` mapped `Stream` → objects. */
    @Benchmark
    fun ours_java_mapped(bh: Blackhole) {
        nativeConn.query(SELECT_SQL, MappedRow::class.java).use { stream ->
            stream.forEach { bh.consume(it) }
        }
    }

    /** Java column-major baseline: zero-boxing primitive read over blocks (the ceiling). */
    @Benchmark
    fun ours_java_columnar(bh: Blackhole) {
        nativeConn.query(SELECT_SQL).use { result ->
            val iter = result.blocks()
            while (iter.hasNext()) {
                val block: Block = iter.next()
                if (block.isEmpty) continue
                val ids = block.column(0).values() as LongArray     // id    UInt64  -> long[]
                val values = block.column(1).values() as DoubleArray // value Float64 -> double[]
                val n = block.rowCount()
                var idSum = 0L
                var valueSum = 0.0
                for (r in 0 until n) {
                    idSum += ids[r]
                    valueSum += values[r]
                }
                bh.consume(idSum)
                bh.consume(valueSum)
            }
        }
    }

    private companion object {
        /** Selects exactly the two mapped columns, matching the other select benchmarks. */
        const val SELECT_SQL = "SELECT id, value FROM bench"

        /** Rows per emitted batch for [ours_kotlin_flow_batched]. */
        const val BATCH_SIZE = 100_000
    }
}
