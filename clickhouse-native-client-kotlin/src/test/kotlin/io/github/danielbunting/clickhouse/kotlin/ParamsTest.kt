package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.QueryParameters
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ParamsTest {

    @Test
    fun emptyVarargReturnsTheSharedEmptyInstance() {
        assertSame(QueryParameters.EMPTY, queryParametersOf())
        assertTrue(queryParametersOf().isEmpty())
    }

    @Test
    fun valuesConvertToClickHouseTextualForm() {
        val params = queryParametersOf(
            "id" to 42,
            "name" to "alpha",
            "ratio" to 1.5,
            "flag" to true,
            "day" to LocalDate.of(2026, 6, 10),
        )
        assertEquals("42", params.wireValue("id"))
        assertEquals("alpha", params.wireValue("name"))
        assertEquals("1.5", params.wireValue("ratio"))
        assertEquals("true", params.wireValue("flag"))
        assertEquals("2026-06-10", params.wireValue("day"))
    }

    @Test
    fun nullValueBindsTheNullSentinel() {
        val params = queryParametersOf("maybe" to null)
        assertEquals("\\N", params.wireValue("maybe"))
    }

    @Test
    fun insertionOrderIsPreserved() {
        val params = queryParametersOf("z" to 1, "a" to 2, "m" to 3)
        assertEquals(listOf("z", "a", "m"), params.asMap().keys.toList())
    }

    @Test
    fun builderDslSupportsConditionalAndVerbatimBinds() {
        val until: String? = null
        val params = queryParameters {
            bind("id", 7)
            bindText("raw", "verbatim-text")
            if (until != null) bind("until", until)
        }
        assertEquals("7", params.wireValue("id"))
        assertEquals("verbatim-text", params.wireValue("raw"))
        assertEquals(setOf("id", "raw"), params.asMap().keys)
    }
}
