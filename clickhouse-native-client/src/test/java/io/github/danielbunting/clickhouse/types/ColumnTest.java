package io.github.danielbunting.clickhouse.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.danielbunting.clickhouse.types.codec.StringColumn;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Column#stringAt(int)} — the lazy-String convenience accessor.
 */
class ColumnTest {

    private static StringColumn stringBacking(StringColumnCodec codec, String... values) {
        StringColumn col = codec.allocate(values.length);
        for (int i = 0; i < values.length; i++) {
            codec.set(col, i, values[i]);
        }
        return col;
    }

    @Test
    void stringAtReturnsDecodedValueForStringColumn() {
        StringColumnCodec codec = new StringColumnCodec();
        Column col = new Column("name", "String");
        col.codec(codec);
        col.rowCount(3);
        col.values(stringBacking(codec, "alpha", "", "😀 世界"));

        assertEquals("alpha", col.stringAt(0));
        assertEquals("", col.stringAt(1));
        assertEquals("😀 世界", col.stringAt(2));
    }

    @Test
    void stringAtReturnsNullForNullCellInNullableColumn() {
        StringColumnCodec codec = new StringColumnCodec();
        Column col = new Column("name", "Nullable(String)");
        col.codec(codec);
        col.rowCount(3);
        // Backing carries placeholder values at null positions; the null-map is authoritative.
        col.values(stringBacking(codec, "present", "ignored", "also"));
        col.nulls(new boolean[]{false, true, false});

        assertEquals("present", col.stringAt(0));
        assertNull(col.stringAt(1), "null cell must return null, not the placeholder backing value");
        assertEquals("also", col.stringAt(2));
    }

    @Test
    void stringAtFallsBackToToStringForNonStringColumn() {
        UInt32Codec codec = new UInt32Codec();
        Column col = new Column("id", "UInt32");
        col.codec(codec);
        col.rowCount(2);
        long[] backing = codec.allocate(2);
        codec.set(backing, 0, 7L);
        codec.set(backing, 1, 42L);
        col.values(backing);

        assertEquals("7", col.stringAt(0));
        assertEquals("42", col.stringAt(1));
    }

    @Test
    void stringAtNullForNullNumericCell() {
        UInt32Codec codec = new UInt32Codec();
        Column col = new Column("id", "Nullable(UInt32)");
        col.codec(codec);
        col.rowCount(2);
        long[] backing = codec.allocate(2);
        codec.set(backing, 0, 9L);
        col.values(backing);
        col.nulls(new boolean[]{false, true});

        assertEquals("9", col.stringAt(0));
        assertNull(col.stringAt(1));
    }
}
