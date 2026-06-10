package io.github.danielbunting.clickhouse.mapping

import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.lang.invoke.LambdaMetafactory
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.util.function.Function
import java.util.function.ToDoubleFunction
import java.util.function.ToLongFunction

/**
 * A specialized, per-column binder that moves one component of a row object
 * straight into a column's backing array, avoiding the `Object[]` scratch
 * and per-field boxing where the component is a primitive numeric type and the
 * column codec exposes a non-boxing [ColumnCodec.setLong]/[ColumnCodec.setDouble] path.
 *
 * Three flavours exist:
 *  - `LONG` — a [ToLongFunction] reads a primitive `long`
 *    (widened from `long/int/short/byte`) and calls
 *    `codec.setLong(arr, row, v)`; zero boxing.
 *  - `DOUBLE` — a [ToDoubleFunction] reads a primitive `double`
 *    (widened from `double/float`) and calls
 *    `codec.setDouble(arr, row, v)`; zero boxing.
 *  - `OBJECT` — a [Function] returns the boxed value and calls the
 *    boxed [ColumnCodec.set] (or sets the column null-map for `null`).
 *    Used for reference types, temporal types (`Instant`/`LocalDate`),
 *    nullable boxed fields, and any component that could not be safely
 *    specialized.
 *
 * The `LambdaMetafactory`-backed primitive accessors are the risky part;
 * if any step of the specialization fails the binder falls back to a reflective
 * `OBJECT` accessor for that single column. Correctness over cleverness.
 */
public class ColumnBinder private constructor(
    private val kind: Kind,
    private val longGetter: ToLongFunction<Any>?,
    private val doubleGetter: ToDoubleFunction<Any>?,
    private val objectGetter: Function<Any, Any?>?,
    codec: ColumnCodec<*>,
) {

    /** The specialization flavour of a binder. */
    public enum class Kind { LONG, DOUBLE, OBJECT }

    @Suppress("UNCHECKED_CAST")
    private val codec: ColumnCodec<Any?> = codec as ColumnCodec<Any?>

    public fun kind(): Kind {
        return kind
    }

    /**
     * Binds this column's value for `row` from [rowObj] directly into
     * [valueArray] at index [rowIndex]. For an `OBJECT` binder a
     * `null` value sets `nulls[rowIndex]` (when `nulls != null`)
     * instead of writing the array. Returns `true` if the cell was written as
     * a non-null value, `false` if it was recorded as SQL `NULL`.
     */
    public fun bind(rowObj: Any, valueArray: Any?, rowIndex: Int, nulls: BooleanArray?): Boolean {
        when (kind) {
            Kind.LONG -> {
                codec.setLong(valueArray, rowIndex, longGetter!!.applyAsLong(rowObj))
                return true
            }
            Kind.DOUBLE -> {
                codec.setDouble(valueArray, rowIndex, doubleGetter!!.applyAsDouble(rowObj))
                return true
            }
            else -> {
                val v = objectGetter!!.apply(rowObj)
                if (v == null && nulls != null) {
                    nulls[rowIndex] = true
                    return false
                }
                codec.set(valueArray, rowIndex, v)
                return true
            }
        }
    }

    // ------------------------------------------------------------------
    // Factory
    // ------------------------------------------------------------------

    internal companion object {

        /**
         * Builds a binder for one column. [member] is either a [Method]
         * (record component accessor) or a [Field] (POJO field). The component's
         * declared type and the column codec decide whether a primitive fast path is
         * usable; anything else (or any failure) yields a reflective `OBJECT`
         * binder.
         *
         * @param ownerType the row class (the functional-interface input type)
         * @param member    the record-component accessor [Method] or POJO [Field]
         * @param memberType the declared Java type of the component
         * @param nullable  whether the column is `Nullable(...)`
         * @param codec     the resolved column codec
         */
        @JvmStatic
        internal fun build(ownerType: Class<*>, member: Any, memberType: Class<*>,
                           nullable: Boolean, codec: ColumnCodec<*>): ColumnBinder {
            // Nullable columns and boxed/reference component types must keep the Object
            // path so a null travels through the null-map rather than setLong(0).
            val primitiveComponent = memberType.isPrimitive
            if (primitiveComponent && !nullable) {
                try {
                    if (isLongCompatible(memberType) && codecSupportsLong(codec)) {
                        val g = makeLongGetter(ownerType, member, memberType)
                        if (g != null) {
                            return ColumnBinder(Kind.LONG, g, null, null, codec)
                        }
                    } else if (isDoubleCompatible(memberType) && codecSupportsDouble(codec)) {
                        val g = makeDoubleGetter(ownerType, member, memberType)
                        if (g != null) {
                            return ColumnBinder(Kind.DOUBLE, null, g, null, codec)
                        }
                    }
                } catch (t: Throwable) {
                    // Fall through to the reflective Object path.
                }
            }
            return ColumnBinder(Kind.OBJECT, null, null, makeObjectGetter(member), codec)
        }

        private fun isLongCompatible(t: Class<*>): Boolean {
            return t == Long::class.javaPrimitiveType || t == Int::class.javaPrimitiveType
                    || t == Short::class.javaPrimitiveType || t == Byte::class.javaPrimitiveType
                    || t == Char::class.javaPrimitiveType
        }

        private fun isDoubleCompatible(t: Class<*>): Boolean {
            return t == Double::class.javaPrimitiveType || t == Float::class.javaPrimitiveType
        }

        /** Integer-backed codecs expose a real `setLong`; identified by their javaType. */
        private fun codecSupportsLong(codec: ColumnCodec<*>): Boolean {
            val jt = codec.javaType()
            return jt == Long::class.javaObjectType || jt == Int::class.javaObjectType
                    || jt == Short::class.javaObjectType || jt == Byte::class.javaObjectType
        }

        /** Float-backed codecs expose a real `setDouble`; identified by their javaType. */
        private fun codecSupportsDouble(codec: ColumnCodec<*>): Boolean {
            val jt = codec.javaType()
            return jt == Double::class.javaObjectType || jt == Float::class.javaObjectType
        }

        // ------------------------------------------------------------------
        // LambdaMetafactory specialization
        // ------------------------------------------------------------------

        /**
         * Produces a [ToLongFunction] that reads the (primitive integral)
         * component and widens it to `long`, via `LambdaMetafactory` when
         * the underlying handle's return type is exactly `long`; otherwise wraps a
         * direct [MethodHandle] that the JIT widens. Returns `null` if no
         * safe path exists.
         */
        @Throws(Throwable::class)
        private fun makeLongGetter(ownerType: Class<*>, member: Any,
                                   memberType: Class<*>): ToLongFunction<Any>? {
            val lookup = MethodHandles.lookup()
            val raw = unreflect(lookup, member) ?: return null
            // Widen the primitive return to long so the resulting handle is (Object)long.
            val asLong = raw.asType(MethodType.methodType(Long::class.javaPrimitiveType, Any::class.java))
            if (memberType == Long::class.javaPrimitiveType) {
                // Exact long return — bind a ToLongFunction directly through LambdaMetafactory.
                val lmf = tryLambdaLong(lookup, raw, ownerType)
                if (lmf != null) {
                    return lmf
                }
            }
            // Fallback: wrap the (widened) MethodHandle. Still no boxing of the primitive.
            val h = asLong
            return ToLongFunction { obj: Any ->
                try {
                    h.invokeExact(obj) as Long
                } catch (t: Throwable) {
                    throw ClickHouseException("Typed long binder failed", t)
                }
            }
        }

        @Throws(Throwable::class)
        private fun makeDoubleGetter(ownerType: Class<*>, member: Any,
                                     memberType: Class<*>): ToDoubleFunction<Any>? {
            val lookup = MethodHandles.lookup()
            val raw = unreflect(lookup, member) ?: return null
            val asDouble = raw.asType(MethodType.methodType(Double::class.javaPrimitiveType, Any::class.java))
            if (memberType == Double::class.javaPrimitiveType) {
                val lmf = tryLambdaDouble(lookup, raw, ownerType)
                if (lmf != null) {
                    return lmf
                }
            }
            val h = asDouble
            return ToDoubleFunction { obj: Any ->
                try {
                    h.invokeExact(obj) as Double
                } catch (t: Throwable) {
                    throw ClickHouseException("Typed double binder failed", t)
                }
            }
        }

        /** Unreflects a record-accessor [Method] or POJO [Field] to a getter handle. */
        private fun unreflect(lookup: MethodHandles.Lookup, member: Any): MethodHandle? {
            try {
                if (member is Method) {
                    member.isAccessible = true
                    return lookup.unreflect(member)
                }
                if (member is Field) {
                    member.isAccessible = true
                    return lookup.unreflectGetter(member)
                }
            } catch (e: IllegalAccessException) {
                return null
            }
            return null
        }

        /**
         * Attempts to spin a [ToLongFunction] via [LambdaMetafactory] for a
         * handle whose erased shape is `(Owner)long`. Returns `null` on any
         * failure (the caller then uses the MethodHandle wrapper fallback).
         */
        @Suppress("UNCHECKED_CAST")
        private fun tryLambdaLong(lookup: MethodHandles.Lookup,
                                  target: MethodHandle, ownerType: Class<*>): ToLongFunction<Any>? {
            return try {
                val site = LambdaMetafactory.metafactory(
                        lookup,
                        "applyAsLong",
                        MethodType.methodType(ToLongFunction::class.java),
                        MethodType.methodType(Long::class.javaPrimitiveType, Any::class.java),
                        target,
                        MethodType.methodType(Long::class.javaPrimitiveType, ownerType))
                site.target.invoke() as ToLongFunction<Any>
            } catch (t: Throwable) {
                null
            }
        }

        @Suppress("UNCHECKED_CAST")
        private fun tryLambdaDouble(lookup: MethodHandles.Lookup,
                                    target: MethodHandle, ownerType: Class<*>): ToDoubleFunction<Any>? {
            return try {
                val site = LambdaMetafactory.metafactory(
                        lookup,
                        "applyAsDouble",
                        MethodType.methodType(ToDoubleFunction::class.java),
                        MethodType.methodType(Double::class.javaPrimitiveType, Any::class.java),
                        target,
                        MethodType.methodType(Double::class.javaPrimitiveType, ownerType))
                site.target.invoke() as ToDoubleFunction<Any>
            } catch (t: Throwable) {
                null
            }
        }

        /** Reflective boxed getter over a record accessor [Method] or POJO [Field]. */
        private fun makeObjectGetter(member: Any): Function<Any, Any?> {
            if (member is Method) {
                member.isAccessible = true
                return Function { obj: Any ->
                    try {
                        member.invoke(obj)
                    } catch (e: ReflectiveOperationException) {
                        throw ClickHouseException("Cannot read component " + member.name, e)
                    }
                }
            }
            val f = member as Field
            f.isAccessible = true
            return Function { obj: Any ->
                try {
                    f.get(obj)
                } catch (e: IllegalAccessException) {
                    throw ClickHouseException("Cannot read field " + f.name, e)
                }
            }
        }
    }
}
