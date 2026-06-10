package io.github.danielbunting.clickhouse.protocol;

import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.CodecRegistry;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry;
import io.github.danielbunting.clickhouse.types.codec.StringColumn;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Block-layer null-map tests for {@code Nullable(T)} columns.
 *
 * <p>{@code BlockCodecTest} already round-trips mixed-null, unset, and plain columns; this
 * suite adds the pieces those don't cover:
 * <ul>
 *   <li>a <b>golden ordering</b> assertion that the null-map is written at its exact wire
 *       position — immediately after the (empty) state prefix and <em>before</em> the value
 *       array — which a symmetric round-trip cannot catch; and</li>
 *   <li>an <b>all-null</b> column (every row flagged null).</li>
 * </ul>
 */
class BlockNullMapTest {

    private final CodecRegistry registry = new DefaultCodecRegistry();

    @Test
    void nullMapPrecedesValuesOnTheWire() throws IOException {
        // Nullable(UInt32), rows [10, NULL, 30].
        Block original = new Block();
        original.rowCount(3);
        Column v = new Column("v", "Nullable(UInt32)");
        v.rowCount(3);
        v.codec(new UInt32Codec());
        v.nulls(new boolean[]{false, true, false});
        v.values(new long[]{10L, 0L, 30L});
        original.addColumn(v);

        byte[] wire = Bytes.capture(w -> BlockCodec.write(w, original));

        // Walk the wire by hand to pin field ordering. (Revision 0: no per-column custom
        // byte; UInt32 has an empty state prefix, so the null-map follows the type string.)
        BinaryReader r = Bytes.reader(wire);
        // Block info: (1, is_overflows), (2, bucket_num), terminator 0.
        assertEquals(1L, r.readVarUInt());
        r.readUInt8();
        assertEquals(2L, r.readVarUInt());
        r.readInt32();
        assertEquals(0L, r.readVarUInt());
        // Header.
        assertEquals(1L, r.readVarUInt(), "num_columns");
        assertEquals(3L, r.readVarUInt(), "num_rows");
        assertEquals("v", r.readString());
        assertEquals("Nullable(UInt32)", r.readString());
        // Null-map: exactly num_rows bytes, here [0, 1, 0], BEFORE any value byte.
        assertEquals(0, r.readUInt8());
        assertEquals(1, r.readUInt8(), "row 1 is null");
        assertEquals(0, r.readUInt8());
        // Only now do the UInt32 values follow.
        assertEquals(10L, r.readUInt32());
        r.readUInt32(); // null row's value slot (unspecified content)
        assertEquals(30L, r.readUInt32());
    }

    @Test
    void allNullColumnRoundTrips() throws IOException {
        Block original = new Block();
        original.rowCount(3);
        Column v = new Column("v", "Nullable(String)");
        v.rowCount(3);
        StringColumnCodec codec = new StringColumnCodec();
        v.codec(codec);
        v.nulls(new boolean[]{true, true, true});
        StringColumn vals = codec.allocate(3);
        codec.set(vals, 0, "");
        codec.set(vals, 1, "");
        codec.set(vals, 2, "");
        v.values(vals);
        original.addColumn(v);

        byte[] wire = Bytes.capture(w -> BlockCodec.write(w, original));
        Block decoded = BlockCodec.read(Bytes.reader(wire), registry);

        Column dv = decoded.column(0);
        assertTrue(dv.isNullable());
        assertArrayEquals(new boolean[]{true, true, true}, dv.nulls());
        assertEquals(3, dv.rowCount());
    }
}
