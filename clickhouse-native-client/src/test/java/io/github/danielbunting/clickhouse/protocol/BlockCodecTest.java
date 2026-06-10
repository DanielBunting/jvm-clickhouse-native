package io.github.danielbunting.clickhouse.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.CodecRegistry;
import io.github.danielbunting.clickhouse.types.Column;
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry;
import io.github.danielbunting.clickhouse.types.codec.StringColumn;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

/**
 * Round-trip unit tests for {@link BlockCodec}: a block written to an in-memory buffer
 * and read back must reproduce the original columns, types, values and null-maps.
 */
class BlockCodecTest {

    private final CodecRegistry registry = new DefaultCodecRegistry();

    /** Builds a {@link StringColumn} backing populated via the codec's set path. */
    private static StringColumn stringColumn(StringColumnCodec codec, String... values) {
        StringColumn col = codec.allocate(values.length);
        for (int i = 0; i < values.length; i++) {
            codec.set(col, i, values[i]);
        }
        return col;
    }

    /** Materialises a decoded String column into a String[] via {@link Column#stringAt(int)}. */
    private static String[] decodedStrings(Column col) {
        String[] out = new String[col.rowCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = col.stringAt(i);
        }
        return out;
    }

    /** Round-trips a two-column block (UInt32 + String) with no nullable columns. */
    @Test
    void roundTripsPlainColumns() throws IOException {
        Block original = new Block();
        original.rowCount(3);

        Column id = new Column("id", "UInt32");
        id.rowCount(3);
        id.codec(new UInt32Codec());
        id.values(new long[] {1L, 2L, 4_294_967_295L});
        original.addColumn(id);

        Column name = new Column("name", "String");
        name.rowCount(3);
        StringColumnCodec nameCodec = new StringColumnCodec();
        name.codec(nameCodec);
        name.values(stringColumn(nameCodec, "alpha", "", "gamma"));
        original.addColumn(name);

        byte[] wire = Bytes.capture(w -> {
            try {
                BlockCodec.write(w, original);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Block decoded = BlockCodec.read(Bytes.reader(wire), registry);

        assertEquals(3, decoded.rowCount());
        assertEquals(2, decoded.columnCount());

        Column dId = decoded.column(0);
        assertEquals("id", dId.name());
        assertEquals("UInt32", dId.type());
        assertFalse(dId.isNullable());
        assertArrayEquals(new long[] {1L, 2L, 4_294_967_295L}, (long[]) dId.values());

        Column dName = decoded.column(1);
        assertEquals("name", dName.name());
        assertEquals("String", dName.type());
        assertArrayEquals(new String[] {"alpha", "", "gamma"}, decodedStrings(dName));
    }

    /** Round-trips a block containing a Nullable(UInt32) column with mixed nulls. */
    @Test
    void roundTripsNullableColumn() throws IOException {
        Block original = new Block();
        original.rowCount(4);

        Column v = new Column("v", "Nullable(UInt32)");
        v.rowCount(4);
        v.codec(new UInt32Codec());
        v.nulls(new boolean[] {false, true, false, true});
        // Null rows still occupy a slot in the value array (value is ignored on read).
        v.values(new long[] {10L, 0L, 30L, 0L});
        original.addColumn(v);

        byte[] wire = Bytes.capture(w -> {
            try {
                BlockCodec.write(w, original);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Block decoded = BlockCodec.read(Bytes.reader(wire), registry);

        assertEquals(1, decoded.columnCount());
        Column dv = decoded.column(0);
        assertEquals("v", dv.name());
        assertEquals("Nullable(UInt32)", dv.type());
        assertTrue(dv.isNullable());
        assertNotNull(dv.nulls());
        assertArrayEquals(new boolean[] {false, true, false, true}, dv.nulls());

        long[] values = (long[]) dv.values();
        assertEquals(10L, values[0]);
        assertEquals(30L, values[2]);
    }

    /** An empty block (zero columns, zero rows) survives a round-trip. */
    @Test
    void roundTripsEmptyBlock() throws IOException {
        Block original = new Block();
        original.rowCount(0);

        byte[] wire = Bytes.capture(w -> {
            try {
                BlockCodec.write(w, original);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Block decoded = BlockCodec.read(Bytes.reader(wire), registry);
        assertEquals(0, decoded.columnCount());
        assertEquals(0, decoded.rowCount());
        assertTrue(decoded.isEmpty());
    }

    /**
     * A Nullable column whose {@link Column#nulls()} is left {@code null} is written as
     * an all-zero null-map and reads back as a non-null map of the correct length.
     */
    @Test
    void writesAllZeroNullMapWhenNullsUnset() throws IOException {
        Block original = new Block();
        original.rowCount(2);

        Column v = new Column("v", "Nullable(String)");
        v.rowCount(2);
        StringColumnCodec vCodec = new StringColumnCodec();
        v.codec(vCodec);
        v.values(stringColumn(vCodec, "x", "y"));
        original.addColumn(v);

        byte[] wire = Bytes.capture(w -> {
            try {
                BlockCodec.write(w, original);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Block decoded = BlockCodec.read(Bytes.reader(wire), registry);
        Column dv = decoded.column(0);
        assertArrayEquals(new boolean[] {false, false}, dv.nulls());
        assertArrayEquals(new String[] {"x", "y"}, decodedStrings(dv));
    }

    /** ClickHouse {@code END_OF_GRANULE_FLAG = 1ULL << 62}. */
    private static final long END_OF_GRANULE_FLAG = 1L << 62;

    /** Protocol revision at/above which the per-column custom-serialization flag is present. */
    private static final int REVISION_WITH_CUSTOM = 54_454;

    /**
     * A column flagged {@code has_custom = 1} with serialization kind {@code SPARSE} is
     * dispatched to the sparse-expand path and decoded into a dense column. This exercises
     * the {@link BlockCodec} dispatch end-to-end (block info, header, has_custom byte, kind
     * byte, SparseOffsets, SparseElements) — the path a real sparse column on the wire would
     * take. Hand-crafted because {@link BlockCodec#write} only ever emits default
     * serialization (and CH 25.6 does not transmit sparse in query results).
     */
    @Test
    void readsSparseSerializedColumn() throws IOException {
        int rows = 6;
        int[] positions = {1, 4};
        long[] nonDefault = {111L, 222L};

        byte[] wire = Bytes.capture(w -> {
            try {
                // Block info (is_overflows=0, bucket_num=-1, terminator).
                w.writeVarUInt(1);
                w.writeUInt8(0);
                w.writeVarUInt(2);
                w.writeInt32(-1);
                w.writeVarUInt(0);
                // Header.
                w.writeVarUInt(1);     // num_columns
                w.writeVarUInt(rows);  // num_rows
                // Column.
                w.writeString("v");
                w.writeString("UInt64");
                w.writeUInt8(1);       // has_custom = 1
                w.writeUInt8(1);       // serialization kind = SPARSE
                // SparseOffsets.
                long start = 0;
                for (int off : positions) {
                    w.writeVarUInt(off - start);
                    start = off + 1;
                }
                w.writeVarUInt((rows - start) | END_OF_GRANULE_FLAG);
                // SparseElements (dense UInt64).
                w.writeFrom(nonDefault, nonDefault.length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Block decoded = BlockCodec.read(Bytes.reader(wire), registry, REVISION_WITH_CUSTOM);

        assertEquals(1, decoded.columnCount());
        Column v = decoded.column(0);
        assertEquals("UInt64", v.type());
        assertArrayEquals(new long[] {0, 111, 0, 0, 222, 0}, (long[]) v.values(),
                "sparse column must expand with non-defaults at 1 and 4");
    }

    /**
     * A column flagged {@code has_custom = 1} with an unknown serialization kind is rejected
     * with a {@link io.github.danielbunting.clickhouse.ProtocolException} (the fallback for any
     * non-Default/non-Sparse custom kind), rather than silently mis-reading the data.
     */
    @Test
    void rejectsUnknownCustomSerializationKind() {
        byte[] wire = Bytes.capture(w -> {
            try {
                w.writeVarUInt(1);
                w.writeUInt8(0);
                w.writeVarUInt(2);
                w.writeInt32(-1);
                w.writeVarUInt(0);
                w.writeVarUInt(1);  // num_columns
                w.writeVarUInt(1);  // num_rows
                w.writeString("v");
                w.writeString("UInt64");
                w.writeUInt8(1);    // has_custom = 1
                w.writeUInt8(4);    // unknown/unsupported kind (e.g. REPLICATED)
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        org.junit.jupiter.api.Assertions.assertThrows(
                io.github.danielbunting.clickhouse.ProtocolException.class,
                () -> BlockCodec.read(Bytes.reader(wire), registry, REVISION_WITH_CUSTOM));
    }

    /**
     * {@code has_custom = 1} with serialization kind {@code DEFAULT} reads the column as a
     * plain dense column (the flag being set does not imply sparse).
     */
    @Test
    void readsCustomFlagWithDefaultKindAsDense() throws IOException {
        byte[] wire = Bytes.capture(w -> {
            try {
                w.writeVarUInt(1);
                w.writeUInt8(0);
                w.writeVarUInt(2);
                w.writeInt32(-1);
                w.writeVarUInt(0);
                w.writeVarUInt(1);  // num_columns
                w.writeVarUInt(2);  // num_rows
                w.writeString("v");
                w.writeString("UInt64");
                w.writeUInt8(1);    // has_custom = 1
                w.writeUInt8(0);    // kind = DEFAULT
                w.writeFrom(new long[] {42L, 43L}, 2); // dense data
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        Block decoded = BlockCodec.read(Bytes.reader(wire), registry, REVISION_WITH_CUSTOM);
        assertArrayEquals(new long[] {42L, 43L}, (long[]) decoded.column(0).values());
    }

    /** Confirms a non-nullable column leaves {@link Column#nulls()} as {@code null}. */
    @Test
    void plainColumnHasNoNullMap() throws IOException {
        Block original = new Block();
        original.rowCount(1);
        Column id = new Column("id", "UInt32");
        id.rowCount(1);
        id.codec(new UInt32Codec());
        id.values(new long[] {7L});
        original.addColumn(id);

        byte[] wire = Bytes.capture(w -> {
            try {
                BlockCodec.write(w, original);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Block decoded = BlockCodec.read(Bytes.reader(wire), registry);
        assertNull(decoded.column(0).nulls());
    }
}
