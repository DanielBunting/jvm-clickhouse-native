package io.github.danielbunting.clickhouse.kotlin

import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.protocol.Block
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.util.Locale
import kotlin.reflect.KParameter
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaConstructor

/*
 * Primary-constructor row binding for Kotlin classes — the piece the core mapper cannot do.
 *
 * Compiled Java keeps no constructor parameter names, so the core binds rows either through a
 * record's canonical constructor or through a no-arg constructor + field injection. Kotlin
 * classes always carry their primary constructor's parameter names in `@Metadata`, which
 * `kotlin-reflect` exposes — so an immutable `data class Event(val id: Long, val name: String)`
 * can be constructed directly, no `var`s or default values required.
 *
 * The kotlin-reflect work happens ONCE per query (resolve the primary constructor, match its
 * parameters to result columns by name); each row is then built through the plain Java
 * [Constructor] with a positional argument array — the same shape and cost as the core's
 * record path.
 */

/**
 * True when [type] should bind result rows through its Kotlin primary constructor: a Kotlin
 * class (`@Metadata` present) with a primary constructor that is not a record. `@JvmRecord`
 * data classes report `isRecord` and stay on the core's record path, which binds canonical
 * record components without needing kotlin-reflect.
 */
internal fun isPrimaryConstructorBindable(type: Class<*>): Boolean =
    type.getAnnotation(Metadata::class.java) != null &&
        !type.isRecord &&
        type.kotlin.primaryConstructor != null

/**
 * Maps positional block rows to [T] through [T]'s primary constructor.
 *
 * Built once per query from the result's header [columnNames]: every constructor parameter
 * must match a column by exact name (falling back to case-insensitive); a parameter with no
 * matching column is an error, as is a SQL `NULL` arriving for a non-nullable parameter.
 */
internal class PrimaryConstructorMapper<T : Any>(type: Class<T>, columnNames: List<String>) {

    private val constructor: Constructor<T>
    private val parameterNames: Array<String>
    private val parameterColumns: IntArray
    private val parameterTypes: Array<Class<*>>
    private val parameterNullable: BooleanArray

    init {
        val primary = type.kotlin.primaryConstructor
            ?: throw ClickHouseException("No primary constructor on ${type.name}")
        constructor = primary.javaConstructor
            ?: throw ClickHouseException("Primary constructor of ${type.name} has no Java counterpart")
        constructor.isAccessible = true

        val parameters = primary.parameters
        check(parameters.all { it.kind == KParameter.Kind.VALUE }) {
            "Primary constructor of ${type.name} has non-value parameters (inner class?)"
        }

        val byName = HashMap<String, Int>(columnNames.size * 2)
        val byLowerName = HashMap<String, Int>(columnNames.size * 2)
        columnNames.forEachIndexed { i, n ->
            byName.putIfAbsent(n, i)
            byLowerName.putIfAbsent(n.lowercase(Locale.ROOT), i)
        }

        parameterNames = Array(parameters.size) { i ->
            parameters[i].name
                ?: throw ClickHouseException(
                    "Unnamed primary-constructor parameter #$i on ${type.name}",
                )
        }
        parameterColumns = IntArray(parameters.size) { i ->
            byName[parameterNames[i]]
                ?: byLowerName[parameterNames[i].lowercase(Locale.ROOT)]
                ?: throw ClickHouseException(
                    "No result column for constructor parameter '${parameterNames[i]}' of " +
                        "${type.name}; columns: $columnNames",
                )
        }
        parameterNullable = BooleanArray(parameters.size) { i -> parameters[i].type.isMarkedNullable }
        // Exact JVM types, positionally aligned with the primary constructor's value parameters
        // (primitives for non-null Kotlin Long/Int/Double/...).
        parameterTypes = constructor.parameterTypes
    }

    /** Builds one [T] from row [row] of [block] (whose columns follow the header order). */
    fun map(block: Block, row: Int): T {
        val args = arrayOfNulls<Any>(parameterColumns.size)
        for (i in parameterColumns.indices) {
            args[i] = coerce(block.column(parameterColumns[i]).value(row), i)
        }
        try {
            return constructor.newInstance(*args)
        } catch (e: InvocationTargetException) {
            throw ClickHouseException(
                "Primary constructor of ${constructor.declaringClass.name} threw for row $row",
                e.cause ?: e,
            )
        } catch (e: ReflectiveOperationException) {
            throw ClickHouseException(
                "Cannot invoke primary constructor of ${constructor.declaringClass.name}",
                e,
            )
        }
    }

    /** Adapts a boxed column value to parameter [i]'s JVM type (numeric width changes by name). */
    private fun coerce(value: Any?, i: Int): Any? {
        if (value == null) {
            if (parameterNullable[i]) return null
            throw ClickHouseException(
                "SQL NULL for non-nullable constructor parameter '${parameterNames[i]}' of " +
                    constructor.declaringClass.name,
            )
        }
        val target = parameterTypes[i]
        if (target.isInstance(value)) return value
        if (value is Number) {
            when (target) {
                java.lang.Long.TYPE, java.lang.Long::class.java -> return value.toLong()
                Integer.TYPE, java.lang.Integer::class.java -> return value.toInt()
                java.lang.Short.TYPE, java.lang.Short::class.java -> return value.toShort()
                java.lang.Byte.TYPE, java.lang.Byte::class.java -> return value.toByte()
                java.lang.Double.TYPE, java.lang.Double::class.java -> return value.toDouble()
                java.lang.Float.TYPE, java.lang.Float::class.java -> return value.toFloat()
            }
        }
        if (value is Boolean && (target == java.lang.Boolean.TYPE || target == java.lang.Boolean::class.java)) {
            return value
        }
        throw ClickHouseException(
            "Column value of type ${value.javaClass.name} is not assignable to constructor " +
                "parameter '${parameterNames[i]}' of type ${target.name} on " +
                constructor.declaringClass.name,
        )
    }
}
