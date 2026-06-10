package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.DefaultTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * OFFLINE codec-level tests pinning the {@code LowCardinality(T)} dictionary index-key
 * width transitions at their EXACT thresholds, without a running ClickHouse server.
 *
 * <p>These complement {@code LowCardinalityScaleIT}, which inserts "a lot" of values and so
 * never lands deterministically on a width boundary. Here we build a {@code LowCardinality}
 * column with a precisely controlled number of distinct dictionary values, serialize it with
 * the codec's own {@code write}, then (a) assert round-trip equality through {@code read}, and
 * (b) decode the serialized header by hand and assert the on-wire key-width selector byte.
 *
 * <h2>Wire format recap (see {@link LowCardinalityColumnCodec})</h2>
 * The column's state prefix carries a single {@code UInt64} KeysSerializationVersion (== 1).
 * Each non-empty block then emits:
 * <pre>
 *   index_type : UInt64  — low byte = key width (0=UInt8, 1=UInt16, 2=UInt32, 3=UInt64),
 *                          high bits = flags (0x200 HasAdditionalKeys, 0x400 NeedUpdateDictionary)
 *   dict_size  : UInt64
 *   dictionary : dict_size inner values
 *   num_keys   : UInt64  (== rowCount)
 *   keys       : num_keys unsigned ints of the index_type width
 * </pre>
 *
 * <h2>Confirmed thresholds (from {@code chooseKeyWidth} + the reserved-slot logic in write())</h2>
 * The width is chosen from {@code dictSize}, where {@code maxIndex = dictSize - 1}:
 * UInt8 iff {@code maxIndex <= 0xFF}, UInt16 iff {@code <= 0xFFFF}, else UInt32 (UInt64 above 2^32).
 * For {@code LowCardinality(String)} the write path reserves ONE leading default slot, so
 * {@code dictSize = 1 + distinctCount} and {@code maxIndex = distinctCount}. Therefore, in terms
 * of the number of DISTINCT non-null values inserted:
 * <ul>
 *   <li>distinct &le; 255  -> UInt8  (selector 0)</li>
 *   <li>distinct == 256    -> UInt16 (selector 1)  &lt;-- UInt8 -> UInt16 boundary</li>
 *   <li>distinct &le; 65535 -> UInt16 (selector 1)</li>
 *   <li>distinct == 65536  -> UInt32 (selector 2)  &lt;-- UInt16 -> UInt32 boundary</li>
 * </ul>
 * For {@code LowCardinality(Nullable(String))} TWO slots are reserved (NULL placeholder + nested
 * default), so {@code dictSize = 2 + distinctCount} and the boundary shifts down by one distinct
 * value (distinct == 255 already needs UInt16). The nullable cases below assert NULL handling and
 * round-trip rather than re-pinning the shifted boundary.
 */
class LowCardinalityKeyWidthTest {

    /** index_type low-byte selectors (mirror of the codec's private constants). */
    private static final long KEY_WIDTH_UINT8 = 0L;
    private static final long KEY_WIDTH_UINT16 = 1L;
    private static final long KEY_WIDTH_UINT32 = 2L;

    private static final DefaultTypeParser PARSER = new DefaultTypeParser();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Parses a CH type to its codec; for {@code LowCardinality(...)} this is the LC codec. */
    @SuppressWarnings("unchecked")
    private static ColumnCodec<Object[]> lcCodec(String chType) {
        return (ColumnCodec<Object[]>) PARSER.parse(chType);
    }

    /**
     * Builds a {@code LowCardinality(String)} backing column whose rows reference exactly
     * {@code distinct} distinct dictionary values. One row per distinct value (so rowCount ==
     * distinct), which is the minimal way to force a given dictionary size.
     */
    private static Object[] distinctStringColumn(int distinct) {
        Object[] col = new Object[distinct];
        for (int i = 0; i < distinct; i++) {
            col[i] = "v" + i;
        }
        return col;
    }

    /** Serializes a LowCardinality column (state prefix once, then the single block). */
    private static byte[] serialize(ColumnCodec<Object[]> codec, Object[] col) {
        return Bytes.capture(out -> {
            codec.writeStatePrefix(out);
            codec.write(out, col, col.length);
        });
    }

    /**
     * Reads back a LowCardinality column from {@code wire} (consuming the state prefix first)
     * and returns the materialised per-row values.
     */
    private static Object[] deserialize(ColumnCodec<Object[]> codec, byte[] wire, int rowCount)
            throws IOException {
        BinaryReader in = Bytes.reader(wire);
        codec.readStatePrefix(in);
        Object[] dest = codec.allocate(rowCount);
        codec.read(in, rowCount, dest);
        return dest;
    }

    /**
     * Reads only the {@code index_type} UInt64 from a serialized LowCardinality column
     * (skipping the leading state-prefix UInt64) and returns its low-byte width selector.
     */
    private static long readKeyWidthSelector(byte[] wire) throws IOException {
        BinaryReader in = Bytes.reader(wire);
        in.readUInt64();                 // state-prefix KeysSerializationVersion
        long indexType = in.readUInt64(); // block index_type
        return indexType & 0xFFL;
    }

    /** Asserts the column round-trips to itself and the wire header advertises {@code expectedWidth}. */
    private static void assertRoundTripAndWidth(String chType, Object[] col, long expectedWidth)
            throws IOException {
        ColumnCodec<Object[]> codec = lcCodec(chType);
        byte[] wire = serialize(codec, col);

        assertEquals(expectedWidth, readKeyWidthSelector(wire),
                "on-wire key-width selector for " + chType + " with " + col.length + " distinct values");

        Object[] decoded = deserialize(codec, wire, col.length);
        assertEquals(col.length, decoded.length, "row count preserved");
        for (int i = 0; i < col.length; i++) {
            assertEquals(col[i], decoded[i], "round-trip value at row " + i);
        }
    }

    // =========================================================================
    // UInt8 -> UInt16 boundary (LowCardinality(String): boundary at distinct == 256)
    // =========================================================================

    @Test
    void width_255distinct_isUInt8() throws IOException {
        // distinct=255 -> dictSize=256 -> maxIndex=255 (0xFF) -> UInt8.
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(255), KEY_WIDTH_UINT8);
    }

    @Test
    void width_256distinct_isUInt16() throws IOException {
        // distinct=256 -> dictSize=257 -> maxIndex=256 (0x100) -> UInt16. THE boundary crossing.
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(256), KEY_WIDTH_UINT16);
    }

    @Test
    void width_257distinct_isUInt16() throws IOException {
        // distinct=257 -> dictSize=258 -> maxIndex=257 -> UInt16 (just past the boundary).
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(257), KEY_WIDTH_UINT16);
    }

    // =========================================================================
    // UInt16 -> UInt32 boundary (LowCardinality(String): boundary at distinct == 65536)
    // =========================================================================
    //
    // NOTE: these build a ~65k-element Object[] and exercise the full String dictionary
    // encode/decode in-memory. Still offline (no server) and deterministic; runtime is modest.

    @Test
    void width_65535distinct_isUInt16() throws IOException {
        // distinct=65535 -> dictSize=65536 -> maxIndex=65535 (0xFFFF) -> UInt16.
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(65535), KEY_WIDTH_UINT16);
    }

    @Test
    void width_65536distinct_isUInt32() throws IOException {
        // distinct=65536 -> dictSize=65537 -> maxIndex=65536 (0x10000) -> UInt32. THE boundary crossing.
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(65536), KEY_WIDTH_UINT32);
    }

    @Test
    void width_65537distinct_isUInt32() throws IOException {
        // distinct=65537 -> dictSize=65538 -> maxIndex=65537 -> UInt32 (just past the boundary).
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(65537), KEY_WIDTH_UINT32);
    }

    // =========================================================================
    // Degenerate columns
    // =========================================================================

    @Test
    void emptyColumn_writesNoPayloadBeyondStatePrefix() throws IOException {
        // rowCount == 0: write() emits no block payload (mirrors read()). Only the state prefix
        // (one UInt64 version) should be on the wire (8 bytes), and read() must produce nothing.
        ColumnCodec<Object[]> codec = lcCodec("LowCardinality(String)");
        Object[] col = new Object[0];
        byte[] wire = serialize(codec, col);
        assertEquals(8, wire.length, "empty column emits only the 8-byte state-prefix version");

        Object[] decoded = deserialize(codec, wire, 0);
        assertEquals(0, decoded.length, "empty column round-trips to zero rows");
    }

    @Test
    void singleValueColumn_isUInt8_andRoundTrips() throws IOException {
        // One distinct value -> dictSize=2 -> maxIndex=1 -> UInt8.
        assertRoundTripAndWidth("LowCardinality(String)", distinctStringColumn(1), KEY_WIDTH_UINT8);
    }

    // =========================================================================
    // LowCardinality(Nullable(String)) — slot 0 reserved for NULL
    // =========================================================================

    @Test
    void nullable_nullsInterspersed_roundTripWithSlotZeroReserved() throws IOException {
        // A small column with NULLs scattered among repeated non-null values. NULL maps to
        // dictionary slot 0; the codec must decode those exact rows back to null and the rest
        // to their string. Distinct non-null values: "a","b","c" (3) -> small dictionary, UInt8.
        ColumnCodec<Object[]> codec = lcCodec("LowCardinality(Nullable(String))");
        Object[] col = {"a", null, "b", "a", null, "c", null, "b"};

        byte[] wire = serialize(codec, col);
        assertEquals(KEY_WIDTH_UINT8, readKeyWidthSelector(wire),
                "small nullable dictionary still uses UInt8 keys");

        Object[] decoded = deserialize(codec, wire, col.length);
        assertEquals(col.length, decoded.length);
        for (int i = 0; i < col.length; i++) {
            if (col[i] == null) {
                assertNull(decoded[i], "row " + i + " must decode back to NULL (slot 0)");
            } else {
                assertEquals(col[i], decoded[i], "non-null value at row " + i);
            }
        }
    }

    @Test
    void nullable_allNulls_roundTrip() throws IOException {
        // Every row NULL: all keys point at slot 0. Round-trips to all-null.
        ColumnCodec<Object[]> codec = lcCodec("LowCardinality(Nullable(String))");
        Object[] col = new Object[]{null, null, null, null};
        byte[] wire = serialize(codec, col);
        Object[] decoded = deserialize(codec, wire, col.length);
        for (int i = 0; i < col.length; i++) {
            assertNull(decoded[i], "row " + i + " must be NULL");
        }
    }

    // -------------------------------------------------------------------------
    // (Optional, not yet implemented) Nullable boundary pinning.
    //
    // Because Nullable reserves TWO leading slots, the UInt8->UInt16 boundary shifts to
    // distinct == 255 (dictSize 257). A stub for explicitly pinning that shifted boundary:
    // -------------------------------------------------------------------------

    @Test
    void nullable_widthBoundaryShiftedByReservedNullSlot() throws IOException {
        // distinct=254 non-null + NULL present -> dictSize = 2 + 254 = 256 -> maxIndex=255 -> UInt8.
        // distinct=255 non-null + NULL present -> dictSize = 2 + 255 = 257 -> maxIndex=256 -> UInt16.
        List<Object> below = new ArrayList<>();
        below.add(null);
        for (int i = 0; i < 254; i++) {
            below.add("v" + i);
        }
        assertRoundTripAndWidth("LowCardinality(Nullable(String))",
                below.toArray(), KEY_WIDTH_UINT8);

        List<Object> at = new ArrayList<>();
        at.add(null);
        for (int i = 0; i < 255; i++) {
            at.add("v" + i);
        }
        assertRoundTripAndWidth("LowCardinality(Nullable(String))",
                at.toArray(), KEY_WIDTH_UINT16);
    }
}
