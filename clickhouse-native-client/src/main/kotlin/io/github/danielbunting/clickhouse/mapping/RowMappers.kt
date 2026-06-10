package io.github.danielbunting.clickhouse.mapping

import io.github.danielbunting.clickhouse.ClickHouseException
import io.github.danielbunting.clickhouse.types.ColumnCodec
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.lang.reflect.RecordComponent
import java.util.Locale

/**
 * Factory for [RowMapper] instances.
 *
 * The primary entry point is [forClass], which builds a
 * [ReflectionRowMapper] by introspecting the target class. For Java
 * records the column names are derived from record components in declaration
 * order; for POJOs they are derived from declared fields in declaration order
 * (superclass fields follow subclass fields).
 *
 * The resulting mapper's [RowMapper.columnNames] matches the
 * declaration order of the class's components / fields.
 */
public object RowMappers {

    /**
     * Builds a [ReflectionRowMapper] for [clazz] whose column
     * names are inferred from the class structure:
     *
     *  - If [clazz] is a Java record, column names come from record
     *    components in their declaration order.
     *  - Otherwise column names come from declared instance fields, with
     *    subclass fields listed before superclass fields.
     *
     * @param T     the row type
     * @param clazz the class to build a mapper for
     * @return a new [ReflectionRowMapper]
     * @throws ClickHouseException if introspection fails
     */
    @JvmStatic
    public fun <T> forClass(clazz: Class<T>): RowMapper<T> {
        val names = inferColumnNames(clazz)
        return ReflectionRowMapper(clazz, names)
    }

    /**
     * Builds a [ReflectionRowMapper] for [clazz] with an explicit,
     * ordered set of column names. Use this when the column names in the result
     * set differ from or are a subset of the class's field names.
     *
     * @param T           the row type
     * @param clazz       the class to build a mapper for
     * @param columnNames the column names in positional order
     * @return a new [ReflectionRowMapper]
     */
    @JvmStatic
    public fun <T> forClass(clazz: Class<T>, vararg columnNames: String): RowMapper<T> {
        @Suppress("UNCHECKED_CAST")
        return ReflectionRowMapper(clazz, columnNames as Array<String>)
    }

    /**
     * Builds per-column [typed binders][ColumnBinder] for the write path,
     * aligned positionally with [columnNames]. Each binder either moves a
     * primitive numeric component straight into the column's backing array (no
     * boxing) via [ColumnCodec.setLong]/[ColumnCodec.setDouble], or
     * falls back to the boxed [ColumnCodec.set] path for reference, temporal,
     * nullable, or non-specializable components.
     *
     * Works for both records (canonical accessors) and POJOs (declared fields),
     * mirroring the name-matching of [ReflectionRowMapper].
     *
     * @param clazz       the row class
     * @param columnNames the target column names in positional order
     * @param codecs      the resolved codec per column (positional with [columnNames])
     * @param nullable    per-column flag: `true` if the column is `Nullable(...)`
     * @return a binder per column, never `null`
     * @throws ClickHouseException if a column has no matching component/field
     */
    @JvmStatic
    public fun columnBinders(clazz: Class<*>, columnNames: Array<String>,
                             codecs: Array<ColumnCodec<*>>, nullable: BooleanArray): Array<ColumnBinder> {
        val binders = arrayOfNulls<ColumnBinder>(columnNames.size)
        if (clazz.isRecord) {
            val components = clazz.recordComponents
            val byName = HashMap<String, RecordComponent>()
            val byNameLower = HashMap<String, RecordComponent>()
            for (rc in components) {
                byName[rc.name] = rc
                byNameLower[rc.name.lowercase(Locale.getDefault())] = rc
            }
            for (i in columnNames.indices) {
                var rc = byName[columnNames[i]]
                if (rc == null) {
                    rc = byNameLower[columnNames[i].lowercase(Locale.getDefault())]
                }
                if (rc == null) {
                    throw ClickHouseException("No record component for column '"
                            + columnNames[i] + "' in " + clazz.name)
                }
                binders[i] = ColumnBinder.build(clazz, rc.accessor, rc.type,
                        nullable[i], codecs[i])
            }
        } else {
            val byName = HashMap<String, Field>()
            val byNameLower = HashMap<String, Field>()
            var cls: Class<*>? = clazz
            while (cls != null && cls != Any::class.java) {
                for (f in cls.declaredFields) {
                    byName.putIfAbsent(f.name, f)
                    byNameLower.putIfAbsent(f.name.lowercase(Locale.getDefault()), f)
                }
                cls = cls.superclass
            }
            for (i in columnNames.indices) {
                var f = byName[columnNames[i]]
                if (f == null) {
                    f = byNameLower[columnNames[i].lowercase(Locale.getDefault())]
                }
                if (f == null) {
                    throw ClickHouseException("No field for column '"
                            + columnNames[i] + "' in " + clazz.name)
                }
                binders[i] = ColumnBinder.build(clazz, f, f.type, nullable[i], codecs[i])
            }
        }
        @Suppress("UNCHECKED_CAST")
        return binders as Array<ColumnBinder>
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /**
     * Infers the ordered list of column names from the class structure.
     *
     * For records, returns record-component names in order. For POJOs,
     * collects declared instance fields starting from the most-derived class
     * and walking up to (but not including) [Object]. Synthetic,
     * static, and transient fields are excluded.
     */
    private fun inferColumnNames(clazz: Class<*>): Array<String> {
        if (clazz.isRecord) {
            val components = clazz.recordComponents
            val names = arrayOfNulls<String>(components.size)
            for (i in components.indices) {
                names[i] = components[i].name
            }
            @Suppress("UNCHECKED_CAST")
            return names as Array<String>
        }

        val names = ArrayList<String>()
        var cls: Class<*>? = clazz
        while (cls != null && cls != Any::class.java) {
            for (f in cls.declaredFields) {
                val mod = f.modifiers
                // Skip static, synthetic, and transient fields.
                if (Modifier.isStatic(mod)
                        || f.isSynthetic
                        || Modifier.isTransient(mod)) {
                    continue
                }
                names.add(f.name)
            }
            cls = cls.superclass
        }
        return names.toTypedArray()
    }
}
