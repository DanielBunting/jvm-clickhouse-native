package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.protocol.Block
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ResultRowTest {

    private fun rowOf(block: Block, rowIndex: Int): ResultRow {
        val nameToIndex = block.columns().withIndex().associate { (i, c) -> c.name() to i }
        return ResultRow(block, rowIndex, nameToIndex)
    }

    @Test
    fun readsPrimitivesAndStringsByName() {
        val block = blockOf(
            longColumn("id", 10, 20),
            doubleColumn("score", 1.5, 2.5),
            stringColumn("name", "alpha", "beta"),
        )

        val r0 = rowOf(block, 0)
        assertEquals(10L, r0.long("id"))
        assertEquals(10, r0.int("id"))
        assertEquals(1.5, r0.double("score"))
        assertEquals("alpha", r0.string("name"))
        assertEquals(3, r0.columnCount)

        val r1 = rowOf(block, 1)
        assertEquals(20L, r1.long("id"))
        assertEquals("beta", r1.string("name"))
        assertEquals(20L, r1["id"])      // boxed accessor
    }

    @Test
    fun readsByPositionalIndex() {
        val block = blockOf(longColumn("id", 7), stringColumn("name", "x"))
        val r = rowOf(block, 0)
        assertEquals(7L, r.long(0))
        assertEquals("x", r.string(1))
    }

    @Test
    fun honorsNullMap() {
        val block = blockOf(
            longColumn("id", 1, 0, nulls = booleanArrayOf(false, true)),
            stringColumn("name", "present", "", nulls = booleanArrayOf(false, true)),
        )

        val present = rowOf(block, 0)
        assertFalse(present.isNull("id"))
        assertEquals(1L, present.longOrNull("id"))
        assertEquals("present", present.string("name"))

        val absent = rowOf(block, 1)
        assertTrue(absent.isNull("id"))
        assertNull(absent.longOrNull("id"))
        assertNull(absent["id"])         // boxed accessor returns null
        assertNull(absent.string("name"))
        assertFailsWith<NullPointerException> { absent.long("id") } // primitive can't represent null
    }

    @Test
    fun unknownColumnThrows() {
        val block = blockOf(longColumn("id", 1))
        assertFailsWith<IllegalArgumentException> { rowOf(block, 0).long("nope") }
    }
}
