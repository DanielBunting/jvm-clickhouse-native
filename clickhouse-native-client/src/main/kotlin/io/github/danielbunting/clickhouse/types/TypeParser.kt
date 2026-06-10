package io.github.danielbunting.clickhouse.types

/**
 * Parses a ClickHouse type string into a composed [ColumnCodec] tree, e.g.
 * `"Nullable(Array(UInt32))"` → Nullable(Array(UInt32)) codec.
 *
 * **Contract frozen in W0.2.** Implementation is task W1.B7 (write it
 * test-first against literal type strings).
 */
public interface TypeParser {

    /** Builds a codec for a full ClickHouse type string. Throws on unsupported types. */
    public fun parse(chType: String?): ColumnCodec<*>
}
