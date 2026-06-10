package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.CodecRegistry;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Offset/null-map correctness at composite boundaries for {@code Array(T)} and
 * {@code Map(K, V)}.
 *
 * <p>Array and Map both serialize a leading section of cumulative {@code UInt64}
 * end-offsets followed by a flattened value/entry section (see
 * {@link ArrayColumnCodec} and {@link MapColumnCodec}). The bugs this suite targets
 * hide in degenerate shapes (empty rows interleaved with non-empty), in the
 * <em>two-layer</em> null-map composition of {@code Array(Nullable(T))} (the array
 * structure carries no null-map, but the inner {@link NullableColumnCodec} prepends a
 * per-element null-map to the flattened values), and at block boundaries (offsets are
 * <b>block-local</b>: every {@link io.github.danielbunting.clickhouse.protocol.BlockCodec#read}
 * decodes one block with a fresh {@code start = 0} baseline).
 *
 * <p>These are codec-level round-trips: build a backing array, write it through the
 * codec, then (a) assert the raw offset/null-map bytes on the wire directly and
 * (b) decode and assert structural equality. No ClickHouse server is required.
 */
class CompositeBoundaryCodecTest {

    private static final CodecRegistry REGISTRY = new DefaultCodecRegistry();

    /** Captures the bytes a codec writes for the given backing array. */
    private static byte[] write(ColumnCodec<Object[]> codec, Object[] src, int rows) {
        return Bytes.capture(w -> codec.write(w, src, rows));
    }

    // =====================================================================
    // CASE 1 — Empty arrays interleaved with non-empty ones.
    //   Rows: [10,11] , [] , [12] , []         (Array(UInt32))
    //   Expected cumulative end-offsets: 2, 2, 3, 3   (monotonic, equal across
    //   the empty rows; last offset == total flattened length == 3).
    // =====================================================================
    @Test
    void emptyArraysInterleavedWithNonEmpty_offsetsAreMonotonic() throws IOException {
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());

        Object[] src = codec.allocate(4);
        src[0] = List.of(10L, 11L);
        src[1] = List.of();
        src[2] = List.of(12L);
        src[3] = List.of();

        byte[] wire = write(asObj(codec), src, 4);

        // Assert the offset section directly: cumulative end-offsets 2, 2, 3, 3.
        BinaryReader in = Bytes.reader(wire);
        assertEquals(2L, in.readUInt64());
        assertEquals(2L, in.readUInt64(), "empty row 1 must repeat the previous offset");
        assertEquals(3L, in.readUInt64());
        assertEquals(3L, in.readUInt64(), "trailing empty row must repeat the previous offset");
        // Then the 3 flattened UInt32 values: 10, 11, 12.
        assertEquals(10L, in.readUInt32());
        assertEquals(11L, in.readUInt32());
        assertEquals(12L, in.readUInt32());

        // Round-trip: empty rows decode to empty (not null) lists.
        Object[] dest = codec.allocate(4);
        codec.read(Bytes.reader(wire), 4, dest);
        assertEquals(List.of(10L, 11L), dest[0]);
        assertEquals(List.of(), dest[1]);
        assertEquals(List.of(12L), dest[2]);
        assertEquals(List.of(), dest[3]);
    }

    // =====================================================================
    // CASE 2 — Array(Nullable(Int32)): two independent layers must not cross.
    //   Rows:
    //     row 0: [1, null, 3]
    //     row 1: []                 (empty array, NOT a null array)
    //     row 2: [null]             (single null element)
    //   Wire shape:
    //     offsets (UInt64): 3, 3, 4          (array structure; no null-map here)
    //     inner null-map (1 byte / flattened element): 0,1,0, 1   (4 elements)
    //     inner Int32 values: 1, <default>, 3, <default>
    //   The array layer carries NO null-map (it is not Nullable(Array(...))); only
    //   the inner Nullable does. Decoded non-null Int32 values box to Integer.
    // =====================================================================
    @Test
    void arrayOfNullable_innerNullMapDoesNotCrossArrayStructure() throws IOException {
        ArrayColumnCodec codec = new ArrayColumnCodec(new NullableColumnCodec(new Int32Codec()));

        Object[] src = codec.allocate(3);
        src[0] = Arrays.asList(1, null, 3);
        src[1] = List.of();
        src[2] = Arrays.asList((Integer) null);

        byte[] wire = write(asObj(codec), src, 3);

        BinaryReader in = Bytes.reader(wire);
        // Offset section: 3 array rows -> cumulative end-offsets 3, 3, 4.
        assertEquals(3L, in.readUInt64());
        assertEquals(3L, in.readUInt64(), "empty array row repeats previous offset");
        assertEquals(4L, in.readUInt64());
        // Inner null-map: one byte per flattened element (4 total): 0,1,0,1.
        byte[] nullMap = in.readBytes(4);
        assertArrayEquals(new byte[] {0, 1, 0, 1}, nullMap,
                "inner Nullable null-map must align to flattened elements, not array rows");

        // Round-trip: empty array stays empty; nulls land at the right inner slots.
        Object[] dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);
        assertEquals(Arrays.asList(1, null, 3), dest[0]);
        assertEquals(List.of(), dest[1]);
        assertEquals(Arrays.asList((Object) null), dest[2]);
    }

    // =====================================================================
    // CASE 3 — Array(Nothing): empty / NULL-only arrays surface with the bottom
    //   element type. SELECT [] yields Array(Nothing). The parser resolves the
    //   inner "Nothing" to NothingCodec; the flattened value section is always
    //   empty, so every row decodes to an empty List and the offsets are all 0.
    // =====================================================================
    @Test
    void arrayOfNothing_resolvesAndDecodesToEmptyLists() throws IOException {
        ColumnCodec<?> codec = REGISTRY.resolve("Array(Nothing)");
        assertInstanceOf(ArrayColumnCodec.class, codec);
        assertInstanceOf(NothingCodec.class, ((ArrayColumnCodec) codec).inner());

        @SuppressWarnings("unchecked")
        ColumnCodec<Object[]> arr = (ColumnCodec<Object[]>) codec;

        // Two rows, both empty: offsets 0, 0; no value bytes follow.
        Object[] src = arr.allocate(2);
        src[0] = List.of();
        src[1] = List.of();

        byte[] wire = write(arr, src, 2);
        BinaryReader in = Bytes.reader(wire);
        assertEquals(0L, in.readUInt64());
        assertEquals(0L, in.readUInt64());

        Object[] dest = arr.allocate(2);
        arr.read(Bytes.reader(wire), 2, dest);
        assertEquals(List.of(), dest[0]);
        assertEquals(List.of(), dest[1]);

        // NothingCodec.get returns null for any (never-present) element.
        assertNull(new NothingCodec().get(new Object[1], 0));
    }

    // =====================================================================
    // CASE 4 — Multi-block offset reset (block-local offsets).
    //   STUB. Offsets are block-local: a fresh BlockCodec.read per block restarts
    //   the cumulative baseline at 0. Build TWO independent wire payloads for the
    //   SAME Array(UInt32) codec and decode each separately to prove block 2's
    //   offsets do not continue from block 1's running total.
    //
    //   Block A rows: [1], [2,3]        -> offsets 1, 3 ; values 1,2,3
    //   Block B rows: [4,5], [6]        -> offsets 2, 3 ; values 4,5,6   (NOT 5,6)
    //
    //   Recommendation: keep this OFFLINE. Two separate codec.read() calls model the
    //   per-block decode exactly and assert the offset reset deterministically.
    //   The integration variant (force small server blocks via the
    //   `max_block_size` setting on a `numbers(N)` scan, see MultiBlockReadIT) is
    //   complementary but slower and only checks aggregate count/sum, not offset bytes.
    // =====================================================================
    @Test
    void multiBlockOffsetsResetPerBlock() throws IOException {
        // Two independent payloads for the SAME codec model two blocks decoded by two
        // BlockCodec.read calls. Block B's offsets restart at 2,3 — they do NOT continue
        // from block A's running total (which ended at 3).
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());

        Object[] a = codec.allocate(2);
        a[0] = List.of(1L);
        a[1] = List.of(2L, 3L);
        byte[] wireA = write(asObj(codec), a, 2);
        BinaryReader inA = Bytes.reader(wireA);
        assertEquals(1L, inA.readUInt64());
        assertEquals(3L, inA.readUInt64());

        Object[] b = codec.allocate(2);
        b[0] = List.of(4L, 5L);
        b[1] = List.of(6L);
        byte[] wireB = write(asObj(codec), b, 2);
        BinaryReader inB = Bytes.reader(wireB);
        assertEquals(2L, inB.readUInt64(), "block B offsets restart at 0 baseline");
        assertEquals(3L, inB.readUInt64());

        Object[] destB = codec.allocate(2);
        codec.read(Bytes.reader(wireB), 2, destB);
        assertEquals(List.of(4L, 5L), destB[0]);
        assertEquals(List.of(6L), destB[1]);
    }

    // =====================================================================
    // CASE 5a — Empty Map interleaved with non-empty.
    //   STUB. Map(String, UInt32) rows: {"a":1,"b":2}, {}, {"c":3}.
    //   Expected offsets: 2, 2, 3 ; flattened Tuple(String,UInt32) entries column
    //   holds keys a,b,c and values 1,2,3. Empty map row repeats the prior offset.
    // =====================================================================
    @Test
    void emptyMapInterleavedWithNonEmpty() throws IOException {
        // Map(String, UInt32) mirrors Array's offset logic; an empty map row repeats the
        // prior offset. UInt32 values box to Long.
        MapColumnCodec codec = new MapColumnCodec(new StringColumnCodec(), new UInt32Codec());
        MapColumn src = codec.allocate(3);
        LinkedHashMap<String, Long> m0 = new LinkedHashMap<>();
        m0.put("a", 1L);
        m0.put("b", 2L);
        codec.set(src, 0, m0);
        codec.set(src, 1, new LinkedHashMap<>());
        LinkedHashMap<String, Long> m2 = new LinkedHashMap<>();
        m2.put("c", 3L);
        codec.set(src, 2, m2);

        byte[] wire = Bytes.capture(w -> codec.write(w, src, 3));
        BinaryReader in = Bytes.reader(wire);
        assertEquals(2L, in.readUInt64());
        assertEquals(2L, in.readUInt64(), "empty map row repeats previous offset");
        assertEquals(3L, in.readUInt64());

        MapColumn dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);
        assertEquals(m0, codec.get(dest, 0));
        assertEquals(Map.of(), codec.get(dest, 1));
        assertEquals(m2, codec.get(dest, 2));
    }

    // =====================================================================
    // CASE 5b — Map with "duplicate-key-ish" entries.
    //   STUB. ClickHouse Map does NOT dedup on the wire; the codec set() path takes
    //   a java.util.Map, so true duplicate keys cannot be expressed at the set layer.
    //   Cover the closest meaningful case: distinct-but-colliding keys that round-trip
    //   and preserve count/order. (A genuine duplicate-key wire stream is an
    //   integration concern — assert decode tolerates it there, NOT here.)
    // =====================================================================
    @Test
    void mapDuplicateKeyLikeEntriesRoundTrip() throws IOException {
        // Distinct-but-colliding keys: round-trip count, order, and offset (the codec set()
        // path takes a java.util.Map, so genuine duplicate keys cannot be expressed here).
        MapColumnCodec codec = new MapColumnCodec(new StringColumnCodec(), new UInt32Codec());
        MapColumn src = codec.allocate(1);
        LinkedHashMap<String, Long> m = new LinkedHashMap<>();
        m.put("k", 1L);
        m.put("k2", 2L);
        codec.set(src, 0, m);

        byte[] wire = Bytes.capture(w -> codec.write(w, src, 1));
        assertEquals(2L, Bytes.reader(wire).readUInt64(), "one row, two entries");

        MapColumn dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        @SuppressWarnings("unchecked")
        Map<Object, Object> back = (Map<Object, Object>) codec.get(dest, 0);
        assertEquals(2, back.size());
        assertEquals(m, back);
    }

    // =====================================================================
    // CASE 5c — Nested Map(String, Array(UInt32)) and Map(String, Map(...)).
    //   STUB. Verifies the inner value codec composes: the flattened entries column
    //   is Tuple(String, Array(UInt32)), so the value sub-column itself carries an
    //   offset section. Round-trip {"a":[1,2], "b":[]}.
    // =====================================================================
    @Test
    void nestedMapOfArrayRoundTrip() throws IOException {
        // The value sub-column is itself Array(UInt32) — it carries its own offset section
        // nested inside the map's entries Tuple. The "b" -> [] empty inner array exercises a
        // zero-length array inside a map value.
        ColumnCodec<?> raw = REGISTRY.resolve("Map(String, Array(UInt32))");
        assertInstanceOf(MapColumnCodec.class, raw);
        MapColumnCodec codec = (MapColumnCodec) raw;

        MapColumn src = codec.allocate(1);
        LinkedHashMap<String, List<Long>> m = new LinkedHashMap<>();
        m.put("a", List.of(1L, 2L));
        m.put("b", List.of());
        codec.set(src, 0, m);

        byte[] wire = Bytes.capture(w -> codec.write(w, src, 1));
        MapColumn dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        assertEquals(m, codec.get(dest, 0));
    }

    // ---------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private static ColumnCodec<Object[]> asObj(ColumnCodec<?> c) {
        return (ColumnCodec<Object[]>) c;
    }
}
