package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.ClickHouseException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Offline tests for [PrimaryConstructorMapper] over hand-built blocks: immutable `val` data
 * classes bind by primary-constructor parameter name — no `var`s, no default values, no no-arg
 * constructor — plus the dispatch predicate [isPrimaryConstructorBindable].
 */
class PrimaryConstructorMapperTest {

    data class Row(val id: Long, val name: String?, val score: Double)
    data class Narrow(val id: Int)
    data class CaseInsensitive(val userid: Long)
    data class NonNullName(val name: String)
    @JvmRecord
    data class RecordRow(val id: Long)

    @Test
    fun bindsImmutableValsByParameterName() {
        val block = blockOf(
            longColumn("id", 1, 2),
            stringColumn("name", "alpha", "beta"),
            doubleColumn("score", 1.5, -2.5),
        )
        val mapper = PrimaryConstructorMapper(Row::class.java, listOf("id", "name", "score"))

        assertEquals(Row(1, "alpha", 1.5), mapper.map(block, 0))
        assertEquals(Row(2, "beta", -2.5), mapper.map(block, 1))
    }

    @Test
    fun columnOrderDoesNotNeedToMatchParameterOrder() {
        val block = blockOf(
            doubleColumn("score", 9.0),
            longColumn("id", 7),
            stringColumn("name", "gamma"),
        )
        val mapper = PrimaryConstructorMapper(Row::class.java, listOf("score", "id", "name"))

        assertEquals(Row(7, "gamma", 9.0), mapper.map(block, 0))
    }

    @Test
    fun nullablePararameterReceivesSqlNull() {
        val block = blockOf(
            longColumn("id", 1),
            stringColumn("name", null, nulls = booleanArrayOf(true)),
            doubleColumn("score", 0.0),
        )
        val mapper = PrimaryConstructorMapper(Row::class.java, listOf("id", "name", "score"))

        assertNull(mapper.map(block, 0).name)
    }

    @Test
    fun sqlNullForNonNullableParameterIsAClearError() {
        val block = blockOf(stringColumn("name", null, nulls = booleanArrayOf(true)))
        val mapper = PrimaryConstructorMapper(NonNullName::class.java, listOf("name"))

        val e = assertFailsWith<ClickHouseException> { mapper.map(block, 0) }
        assertTrue("non-nullable" in e.message!! && "name" in e.message!!)
    }

    @Test
    fun numericWidthIsAdaptedToTheParameterType() {
        val block = blockOf(longColumn("id", 41))
        val mapper = PrimaryConstructorMapper(Narrow::class.java, listOf("id"))

        assertEquals(Narrow(41), mapper.map(block, 0))
    }

    @Test
    fun columnNamesMatchCaseInsensitivelyAsAFallback() {
        val block = blockOf(longColumn("UserId", 5))
        val mapper = PrimaryConstructorMapper(CaseInsensitive::class.java, listOf("UserId"))

        assertEquals(CaseInsensitive(5), mapper.map(block, 0))
    }

    @Test
    fun missingColumnForAParameterIsAClearError() {
        val e = assertFailsWith<ClickHouseException> {
            PrimaryConstructorMapper(Row::class.java, listOf("id", "score"))
        }
        assertTrue("name" in e.message!!)
    }

    @Test
    fun dispatchPredicateSelectsKotlinClassesButNotRecordsOrJavaTypes() {
        assertTrue(isPrimaryConstructorBindable(Row::class.java))
        assertFalse(isPrimaryConstructorBindable(RecordRow::class.java), "@JvmRecord stays on the core record path")
        assertFalse(isPrimaryConstructorBindable(String::class.java), "plain JDK class")
    }
}
