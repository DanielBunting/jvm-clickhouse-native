package io.github.danielbunting.clickhouse.mapping

import io.github.danielbunting.clickhouse.ClickHouseException
import java.lang.reflect.Constructor
import java.lang.reflect.Field
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.RecordComponent
import java.math.BigDecimal
import java.math.BigInteger
import java.net.InetAddress
import java.net.UnknownHostException
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.Period
import java.util.Locale
import java.util.UUID

/**
 * Reflection-based [RowMapper] that maps ClickHouse column values to
 * a Java class via field (POJO) or record-component introspection.
 *
 * ## Supported Java types
 *  - [Long] / `long[]`
 *  - [Integer][java.lang.Integer] / `int[]`
 *  - [String] / `Object[]`
 *  - [java.time.Instant] (stored as [Long] epoch-seconds on the wire)
 *  - [java.time.LocalDate] (stored as [Integer][java.lang.Integer] days-since-epoch on the wire)
 *  - [java.util.UUID]
 *  - [java.math.BigDecimal]
 *  - [java.util.List]
 *
 * ## Name matching
 * Column names are matched to field / record-component names exactly first,
 * then case-insensitively as a fallback.
 *
 * Instantiation uses the canonical record constructor for records; for POJOs
 * a no-arg constructor is required and fields are set via reflection.
 *
 * @param T the mapped row type
 */
public class ReflectionRowMapper<T> internal constructor(
    targetClass: Class<T>,
    columnNames: Array<String>,
) : RowMapper<T> {

    /** The ordered column names this mapper handles. */
    private val columnNames: Array<String>

    /** For each column index: the field (POJO) or component accessor for records. */
    private val accessors: Array<FieldAccessor>

    private val targetClass: Class<T>
    private val isRecord: Boolean

    /** Canonical constructor used for records. */
    private val recordConstructor: Constructor<T>?

    /**
     * No-arg constructor used for POJOs on the read path; `null` when the class has none
     * (legal for write-only use — [bind] reads fields and never constructs a `T`).
     */
    private val noArgConstructor: Constructor<T>?

    /**
     * Records only: for each canonical-constructor parameter (in record-component
     * declaration order) the index into the positional `columnValues` row that
     * feeds it, or `-1` when no column matches. Precomputed once so the per-row
     * [mapRecord] path allocates no `HashMap` and does no reflection.
     */
    private val recordComponentToColumn: IntArray?

    /** Records only: the target type of each record component, in declaration order. */
    private val recordComponentTypes: Array<Class<*>>?

    /**
     * Records only: reused staging buffer for the canonical-constructor arguments.
     * Kotlin's spread at the `newInstance(*args)` call site copies the array, so the
     * fresh-array-per-row of the Java original is provided by that copy; staging into
     * a reused field keeps the per-row allocation count identical (one array per row).
     * Safe because a mapper instance is created per query/inserter and rows are mapped
     * sequentially ([trySplit][java.util.Spliterator.trySplit] returns null upstream).
     */
    private val recordArgs: Array<Any?>?

    /*
     * Constructs a mapper for the given class and ordered column names.
     *
     * Field / component lookup is performed eagerly at construction time so
     * that any missing-column errors are reported upfront.
     *
     * targetClass: the class to map to/from
     * columnNames: column names in positional order
     * Throws ClickHouseException if the class cannot be reflected or a column
     * has no matching field.
     */
    init {
        this.targetClass = targetClass
        this.columnNames = columnNames.clone()
        this.isRecord = targetClass.isRecord

        val accessors = arrayOfNulls<FieldAccessor>(columnNames.size)

        if (isRecord) {
            // Build index: componentName -> RecordComponent
            val components = targetClass.recordComponents
            val byName = HashMap<String, RecordComponent>()
            val byNameLower = HashMap<String, RecordComponent>()
            for (rc in components) {
                byName[rc.name] = rc
                byNameLower[rc.name.lowercase(Locale.getDefault())] = rc
            }

            for (i in columnNames.indices) {
                val col = columnNames[i]
                var rc = byName[col]
                if (rc == null) {
                    rc = byNameLower[col.lowercase(Locale.getDefault())]
                }
                if (rc == null) {
                    throw ClickHouseException(
                        "No record component found for column '" + col + "' in " + targetClass.name)
                }
                accessors[i] = RecordComponentAccessor(rc)
            }

            // Resolve canonical constructor: parameter types in component order
            val compTypes = arrayOfNulls<Class<*>>(components.size)
            for (i in components.indices) {
                compTypes[i] = components[i].type
            }
            val ctor: Constructor<T>
            try {
                ctor = targetClass.getDeclaredConstructor(*compTypes)
                ctor.isAccessible = true
            } catch (e: NoSuchMethodException) {
                throw ClickHouseException(
                    "Cannot find canonical constructor for record " + targetClass.name, e)
            }
            this.recordConstructor = ctor
            this.noArgConstructor = null

            // Precompute the component->column binding once. Matching mirrors the
            // (case-insensitive) lookup the old per-row path did: build a lowercased
            // column-name index, then resolve each component against it.
            val colIndexLower = HashMap<String, Int>()
            for (i in columnNames.indices) {
                colIndexLower[columnNames[i].lowercase(Locale.getDefault())] = i
            }
            val toColumn = IntArray(components.size)
            val compTypesExact = arrayOfNulls<Class<*>>(components.size)
            for (c in components.indices) {
                compTypesExact[c] = components[c].type
                val idx = colIndexLower[components[c].name.lowercase(Locale.getDefault())]
                toColumn[c] = idx ?: -1
            }
            this.recordComponentToColumn = toColumn
            @Suppress("UNCHECKED_CAST")
            this.recordComponentTypes = compTypesExact as Array<Class<*>>
            this.recordArgs = arrayOfNulls(toColumn.size)
        } else {
            // POJO: build index of all declared fields (walk hierarchy)
            val byName = HashMap<String, Field>()
            val byNameLower = HashMap<String, Field>()
            var cls: Class<*>? = targetClass
            while (cls != null && cls != Any::class.java) {
                for (f in cls.declaredFields) {
                    f.isAccessible = true
                    byName.putIfAbsent(f.name, f)
                    byNameLower.putIfAbsent(f.name.lowercase(Locale.getDefault()), f)
                }
                cls = cls.superclass
            }

            for (i in columnNames.indices) {
                val col = columnNames[i]
                var f = byName[col]
                if (f == null) {
                    f = byNameLower[col.lowercase(Locale.getDefault())]
                }
                if (f == null) {
                    throw ClickHouseException(
                        "No field found for column '" + col + "' in " + targetClass.name)
                }
                accessors[i] = FieldAccessorImpl(f)
            }

            // Resolved leniently: only the READ path (mapPojo) constructs instances. A
            // write-only mapper (BulkInserter binds fields, never builds a T) must work
            // for classes without a no-arg constructor, e.g. immutable Kotlin data classes.
            this.noArgConstructor = try {
                val ctor = targetClass.getDeclaredConstructor()
                ctor.isAccessible = true
                ctor
            } catch (e: NoSuchMethodException) {
                null
            }
            this.recordConstructor = null
            this.recordComponentToColumn = null
            this.recordComponentTypes = null
            this.recordArgs = null
        }

        @Suppress("UNCHECKED_CAST")
        this.accessors = accessors as Array<FieldAccessor>
    }

    override fun columnNames(): Array<String> {
        return columnNames.clone()
    }

    /**
     * Constructs a `T` from a positional row of column values (read path).
     *
     * Column values are the raw objects extracted from the column array by the
     * block layer. Type coercions from wire primitives (e.g. `Long` ->
     * [Instant]) are applied here.
     *
     * @param columnValues positional column values; may contain `null` entries
     *                     for nullable columns
     * @return a fully-populated instance of `T`
     */
    override fun map(columnValues: Array<Any?>): T {
        return if (isRecord) {
            mapRecord(columnValues)
        } else {
            mapPojo(columnValues)
        }
    }

    /**
     * Reads field values from [value] into [dest] positionally
     * (write path, used by [io.github.danielbunting.clickhouse.BulkInserter]).
     *
     * @param value the source object
     * @param dest  destination array; element at index `i` corresponds to
     *              `columnNames()[i]`
     */
    override fun bind(value: T, dest: Array<Any?>) {
        for (i in accessors.indices) {
            val raw = accessors[i].get(value)
            dest[i] = toWire(raw, accessors[i].javaType())
        }
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private fun mapRecord(columnValues: Array<Any?>): T {
        // Build the constructor arguments in record-component order using the
        // precomputed component->column binding (no per-row HashMap / reflection).
        val toColumn = recordComponentToColumn!!
        val types = recordComponentTypes!!
        // Staged into the reused buffer; the spread at newInstance(*args) copies it,
        // which is the per-row fresh array (see the recordArgs field doc).
        val args = recordArgs!!
        for (c in toColumn.indices) {
            val idx = toColumn[c]
            if (idx >= 0) {
                args[c] = fromWire(columnValues[idx], types[c])
            } else {
                // A component with no matching column stays null (allowed for
                // optional / nullable fields in the result set).
                args[c] = null
            }
        }

        try {
            return recordConstructor!!.newInstance(*args)
        } catch (e: InstantiationException) {
            throw ClickHouseException(
                "Failed to instantiate record " + targetClass.name, e)
        } catch (e: IllegalAccessException) {
            throw ClickHouseException(
                "Failed to instantiate record " + targetClass.name, e)
        } catch (e: InvocationTargetException) {
            throw ClickHouseException(
                "Failed to instantiate record " + targetClass.name, e)
        }
    }

    private fun mapPojo(columnValues: Array<Any?>): T {
        val ctor = noArgConstructor
            ?: throw ClickHouseException(
                "POJO " + targetClass.name + " requires a no-arg constructor to map query"
                    + " results (writing via BulkInserter does not)")
        val instance: T
        try {
            instance = ctor.newInstance()
        } catch (e: InstantiationException) {
            throw ClickHouseException(
                "Failed to instantiate POJO " + targetClass.name, e)
        } catch (e: IllegalAccessException) {
            throw ClickHouseException(
                "Failed to instantiate POJO " + targetClass.name, e)
        } catch (e: InvocationTargetException) {
            throw ClickHouseException(
                "Failed to instantiate POJO " + targetClass.name, e)
        }
        for (i in accessors.indices) {
            val coerced = fromWire(columnValues[i], accessors[i].javaType())
            accessors[i].set(instance, coerced)
        }
        return instance
    }

    // ------------------------------------------------------------------
    // Accessor abstraction
    // ------------------------------------------------------------------

    /** Thin uniform accessor over either a [Field] or a [RecordComponent]. */
    private interface FieldAccessor {
        fun get(instance: Any?): Any?
        fun set(instance: Any?, value: Any?)
        fun javaType(): Class<*>
    }

    private class FieldAccessorImpl(private val field: Field) : FieldAccessor {

        override fun get(instance: Any?): Any? {
            try {
                return field.get(instance)
            } catch (e: IllegalAccessException) {
                throw ClickHouseException(
                    "Cannot read field " + field.name, e)
            }
        }

        override fun set(instance: Any?, value: Any?) {
            try {
                field.set(instance, value)
            } catch (e: IllegalAccessException) {
                throw ClickHouseException(
                    "Cannot set field " + field.name, e)
            }
        }

        override fun javaType(): Class<*> {
            return field.type
        }
    }

    private class RecordComponentAccessor(component: RecordComponent) : FieldAccessor {
        private val component: RecordComponent

        init {
            component.accessor.isAccessible = true
            this.component = component
        }

        override fun get(instance: Any?): Any? {
            try {
                return component.accessor.invoke(instance)
            } catch (e: IllegalAccessException) {
                throw ClickHouseException(
                    "Cannot read record component " + component.name, e)
            } catch (e: InvocationTargetException) {
                throw ClickHouseException(
                    "Cannot read record component " + component.name, e)
            }
        }

        override fun set(instance: Any?, value: Any?) {
            // Record components are immutable; set is a no-op (construction is
            // handled via the canonical constructor in mapRecord).
            throw ClickHouseException(
                "Cannot set component '" + component.name + "' on an immutable record")
        }

        override fun javaType(): Class<*> {
            return component.type
        }
    }

    internal companion object {

        /**
         * Converts a raw wire value (typically the boxed primitive that the column
         * array element was extracted as) into the Java type expected by the field.
         *
         * Supported coercions:
         *  - `Number` -> [Long] / [Integer][java.lang.Integer] (widening/narrowing)
         *  - `Long` (epoch-seconds) -> [Instant]
         *  - `Integer` (days-since-epoch) -> [LocalDate]
         *  - Identity for [String], [UUID], [BigDecimal], [List]
         */
        @JvmStatic
        internal fun fromWire(raw: Any?, targetType: Class<*>): Any? {
            if (raw == null) {
                return null
            }
            if (targetType.isInstance(raw)) {
                return raw
            }
            if (targetType == Long::class.javaObjectType || targetType == Long::class.javaPrimitiveType) {
                return (raw as Number).toLong()
            }
            if (targetType == Int::class.javaObjectType || targetType == Int::class.javaPrimitiveType) {
                return (raw as Number).toInt()
            }
            if (targetType == Boolean::class.javaObjectType || targetType == Boolean::class.javaPrimitiveType) {
                if (raw is Boolean) {
                    return raw
                }
                if (raw is Number) {
                    return raw.toLong() != 0L
                }
                if (raw is String) {
                    // trim { it <= ' ' } replicates java.lang.String.trim() exactly.
                    return java.lang.Boolean.parseBoolean(raw.trim { it <= ' ' })
                }
                return raw
            }
            if (targetType == Instant::class.java) {
                // Wire stores DateTime as epoch-seconds (Long or long[])
                return Instant.ofEpochSecond((raw as Number).toLong())
            }
            if (targetType == Duration::class.java) {
                // Time/Time64 codecs already return a Duration (caught by the isInstance
                // check above); this branch coerces a raw Number (seconds) to a Duration.
                if (raw is Duration) {
                    return raw
                }
                return Duration.ofSeconds((raw as Number).toLong())
            }
            if (targetType == Float::class.javaObjectType || targetType == Float::class.javaPrimitiveType) {
                return (raw as Number).toFloat()
            }
            if (targetType == LocalDate::class.java) {
                // Wire stores Date as days-since-epoch (Integer / int)
                return LocalDate.ofEpochDay((raw as Number).toLong())
            }
            if (targetType == Duration::class.java) {
                // Fixed-length Interval* codecs already return a Duration (caught by the
                // isInstance check above); a bare Number is treated as a second count.
                if (raw is Duration) {
                    return raw
                }
                return Duration.ofSeconds((raw as Number).toLong())
            }
            if (targetType == Period::class.java) {
                // Calendar Interval* codecs already return a Period (caught above); a bare
                // Number is treated as a month count.
                if (raw is Period) {
                    return raw
                }
                return Period.ofMonths((raw as Number).toInt())
            }
            if (targetType == String::class.java) {
                return raw.toString()
            }
            if (targetType == UUID::class.java) {
                if (raw is String) {
                    return UUID.fromString(raw)
                }
                return raw
            }
            if (InetAddress::class.java.isAssignableFrom(targetType)) {
                // IPv4/IPv6 codecs already return an InetAddress (caught by the isInstance
                // check above); this branch coerces a String literal to an InetAddress.
                if (raw is String) {
                    try {
                        return InetAddress.getByName(raw)
                    } catch (e: UnknownHostException) {
                        throw ClickHouseException("Cannot parse InetAddress: " + raw, e)
                    }
                }
                return raw
            }
            if (targetType == BigDecimal::class.java) {
                if (raw is BigDecimal) {
                    return raw
                }
                return BigDecimal(raw.toString())
            }
            if (targetType == BigInteger::class.java) {
                if (raw is BigInteger) {
                    return raw
                }
                return BigInteger(raw.toString())
            }
            if (targetType == Map::class.java || Map::class.java.isAssignableFrom(targetType)) {
                // Map(K, V) decodes to a LinkedHashMap; pass it through unchanged.
                return raw
            }
            if (targetType == List::class.java || List::class.java.isAssignableFrom(targetType)) {
                if (raw is List<*>) {
                    return raw
                }
                return raw
            }
            // Fall through: return raw and let any class-cast surface naturally.
            return raw
        }

        /**
         * Converts a Java field value to the wire representation stored in the
         * column array. Inverse of [fromWire].
         *
         * For [Instant] and [LocalDate] the numeric epoch value is
         * extracted; everything else is returned as-is.
         */
        @JvmStatic
        internal fun toWire(value: Any?, fieldType: Class<*>): Any? {
            // Pass the raw Java value straight through. The column codecs accept the boxed
            // Java types directly — ColumnCodec.set takes Instant (DateTime/DateTime64),
            // LocalDate (Date), UUID, BigDecimal, etc. and performs the Java->wire conversion
            // itself. Converting here (e.g. Instant->epoch Long) would double-convert and make
            // ColumnCodec.set throw a ClassCastException on the bulk-insert path.
            return value
        }
    }
}
