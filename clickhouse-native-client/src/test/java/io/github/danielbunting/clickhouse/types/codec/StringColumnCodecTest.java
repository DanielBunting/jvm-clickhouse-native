package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Round-trip, wire-format and lazy-materialisation tests for {@link StringColumnCodec}.
 *
 * <p>No running ClickHouse server is required; all tests operate on in-memory
 * byte vectors produced and consumed by the {@link Bytes} test utility.
 */
class StringColumnCodecTest {

    private final StringColumnCodec codec = new StringColumnCodec();

    /** Builds a write-ready {@link StringColumn} from a String[] via the codec's set path. */
    private StringColumn columnOf(String... values) {
        StringColumn col = codec.allocate(values.length);
        for (int i = 0; i < values.length; i++) {
            codec.set(col, i, values[i]);
        }
        return col;
    }

    /** Encodes a String[] to the native wire format. */
    private byte[] encode(String... values) throws IOException {
        StringColumn col = columnOf(values);
        return Bytes.capture(w -> {
            try {
                codec.write(w, col, values.length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** Decodes {@code count} values from {@code wire} into a String[] via getString. */
    private String[] decode(byte[] wire, int count) throws IOException {
        StringColumn dest = codec.allocate(count);
        codec.read(Bytes.reader(wire), count, dest);
        String[] out = new String[count];
        for (int i = 0; i < count; i++) {
            out[i] = codec.getString(dest, i);
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Test
    void typeNameIsString() {
        assertEquals("String", codec.typeName());
    }

    @Test
    void javaTypeIsStringClass() {
        assertEquals(String.class, codec.javaType());
    }

    @Test
    void allocateReturnsStringColumnOfCorrectSize() {
        StringColumn col = codec.allocate(5);
        assertInstanceOf(StringColumn.class, col);
        assertEquals(5, col.rowCount());
    }

    @Test
    void allocateZeroRows() {
        assertEquals(0, codec.allocate(0).rowCount());
    }

    // -------------------------------------------------------------------------
    // get / set bridge
    // -------------------------------------------------------------------------

    @Test
    void getAndSetRoundTrip() {
        StringColumn col = codec.allocate(2);
        codec.set(col, 0, "hello");
        codec.set(col, 1, "world");
        assertEquals("hello", codec.get(col, 0));
        assertEquals("world", codec.get(col, 1));
        assertEquals("hello", codec.getString(col, 0));
    }

    // -------------------------------------------------------------------------
    // Wire-format: empty column
    // -------------------------------------------------------------------------

    @Test
    void emptyColumnProducesNoBytes() throws IOException {
        assertArrayEquals(new byte[0], encode());
    }

    // -------------------------------------------------------------------------
    // Round-trip: empty strings
    // -------------------------------------------------------------------------

    @Test
    void roundTripEmptyString() throws IOException {
        byte[] wire = encode("", "", "");
        assertArrayEquals(new byte[]{0x00, 0x00, 0x00}, wire);
        assertArrayEquals(new String[]{"", "", ""}, decode(wire, 3));
    }

    @Test
    void singleEmptyStringRoundTrip() throws IOException {
        byte[] wire = encode("");
        assertEquals(1, wire.length);
        assertEquals(0x00, wire[0] & 0xFF);
        assertArrayEquals(new String[]{""}, decode(wire, 1));
    }

    // -------------------------------------------------------------------------
    // Round-trip: ASCII
    // -------------------------------------------------------------------------

    @Test
    void roundTripAsciiStrings() throws IOException {
        String[] src = {"hello", "world", "foo", "bar"};
        assertArrayEquals(src, decode(encode(src), src.length));
    }

    @Test
    void asciiStringWireLayout() throws IOException {
        // "hi" = 0x68 0x69, preceded by VarUInt(2) = 0x02
        assertArrayEquals(new byte[]{0x02, 0x68, 0x69}, encode("hi"));
    }

    // -------------------------------------------------------------------------
    // Round-trip: multibyte UTF-8 + emoji
    // -------------------------------------------------------------------------

    @Test
    void roundTripMultibyteUtf8() throws IOException {
        String[] src = {"こんにちは", "καλημέρα", "Hello, 世界!", "😀🎉"};
        assertArrayEquals(src, decode(encode(src), src.length));
    }

    @Test
    void roundTripEmoji() throws IOException {
        // Emoji are 4-byte UTF-8 (surrogate pairs in Java); verify exact decode.
        String[] src = {"😀", "👨‍👩‍👧‍👦", "🇬🇧"};
        assertArrayEquals(src, decode(encode(src), src.length));
    }

    @Test
    void utf8LengthPrefixIsByteCountNotCharCount() throws IOException {
        // "€" = U+20AC, 3 bytes in UTF-8: 0xE2 0x82 0xAC
        byte[] wire = encode("€");
        assertArrayEquals(new byte[]{0x03, (byte) 0xE2, (byte) 0x82, (byte) 0xAC}, wire);
        assertArrayEquals(new String[]{"€"}, decode(wire, 1));
    }

    // -------------------------------------------------------------------------
    // Round-trip: long strings (multi-byte VarUInt + buffer growth)
    // -------------------------------------------------------------------------

    @Test
    void roundTripLongStringRequiresMultiByteVarUInt() throws IOException {
        String longStr = "A".repeat(200);
        byte[] wire = encode(longStr);
        assertEquals((byte) 0xC8, wire[0]);
        assertEquals((byte) 0x01, wire[1]);
        assertEquals(202, wire.length);
        assertArrayEquals(new String[]{longStr}, decode(wire, 1));
    }

    @Test
    void roundTripVeryLongString() throws IOException {
        String longStr = "x".repeat(50_000);
        assertArrayEquals(new String[]{longStr}, decode(encode(longStr), 1));
    }

    @Test
    void roundTripManyRowsExercisesBufferGrowth() throws IOException {
        String[] src = new String[1000];
        for (int i = 0; i < src.length; i++) {
            src[i] = "row-" + i + "-" + "z".repeat(i % 40);
        }
        assertArrayEquals(src, decode(encode(src), src.length));
    }

    // -------------------------------------------------------------------------
    // Round-trip: mixed column
    // -------------------------------------------------------------------------

    @Test
    void roundTripMixedColumn() throws IOException {
        String[] src = {"", "plain ASCII", "UTF-8: ñoño", "", "last"};
        assertArrayEquals(src, decode(encode(src), src.length));
    }

    // -------------------------------------------------------------------------
    // Lazy decode: getString matches the old eager decode byte-for-byte
    // -------------------------------------------------------------------------

    @Test
    void getStringMatchesEagerDecode() throws IOException {
        String[] src = {"", "ascii", "ünïcödé", "😀 mixed 世界", "tail"};
        byte[] wire = encode(src);

        StringColumn dest = codec.allocate(src.length);
        codec.read(Bytes.reader(wire), src.length, dest);

        // Re-derive the eager result independently: parse the wire by hand.
        BinaryReader r = Bytes.reader(wire);
        for (int i = 0; i < src.length; i++) {
            int len = (int) r.readVarUInt();
            byte[] b = r.readBytes(len);
            String eager = new String(b, StandardCharsets.UTF_8);
            assertEquals(eager, codec.getString(dest, i), "row " + i);
        }
    }

    @Test
    void getStringCachesSameInstance() throws IOException {
        byte[] wire = encode("cacheable");
        StringColumn dest = codec.allocate(1);
        codec.read(Bytes.reader(wire), 1, dest);
        String first = codec.getString(dest, 0);
        String second = codec.getString(dest, 0);
        assertSame(first, second, "lazy decode should cache and return the same String instance");
    }

    // -------------------------------------------------------------------------
    // CRITICAL: lazy decode survives a subsequent block read (no aliasing)
    // -------------------------------------------------------------------------

    @Test
    void lazyDecodeSurvivesSubsequentBlockRead() throws IOException {
        // Block A and block B are concatenated on one stream, as the real protocol
        // delivers consecutive blocks through a single reader.
        String[] a = {"alpha", "beta", "gamma"};
        String[] b = {"DELTA-overwrite", "EPSILON-overwrite", "ZETA-overwrite"};

        byte[] wireA = encode(a);
        byte[] wireB = encode(b);
        byte[] both = new byte[wireA.length + wireB.length];
        System.arraycopy(wireA, 0, both, 0, wireA.length);
        System.arraycopy(wireB, 0, both, wireA.length, wireB.length);

        BinaryReader reader = Bytes.reader(both);

        // Read block A's column but DO NOT decode it yet (lazy).
        StringColumn colA = codec.allocate(a.length);
        codec.read(reader, a.length, colA);

        // Now read block B into a different holder from the SAME reader.
        StringColumn colB = codec.allocate(b.length);
        codec.read(reader, b.length, colB);

        // Decode A AFTER B was read. If A aliased a shared reader buffer that B
        // overwrote, these assertions would see B's bytes (corruption).
        for (int i = 0; i < a.length; i++) {
            assertEquals(a[i], codec.getString(colA, i), "block A row " + i + " corrupted by block B read");
        }
        for (int i = 0; i < b.length; i++) {
            assertEquals(b[i], codec.getString(colB, i), "block B row " + i);
        }
    }

    // -------------------------------------------------------------------------
    // toStringArray materialisation
    // -------------------------------------------------------------------------

    @Test
    void toStringArrayMaterialisesAllRows() throws IOException {
        String[] src = {"one", "two", "three"};
        byte[] wire = encode(src);
        StringColumn dest = codec.allocate(src.length);
        codec.read(Bytes.reader(wire), src.length, dest);
        assertArrayEquals(src, dest.toStringArray());
    }

    // -------------------------------------------------------------------------
    // set null is retained as null (block layer handles null-maps)
    // -------------------------------------------------------------------------

    @Test
    void setNullReturnsNull() {
        StringColumn col = codec.allocate(2);
        codec.set(col, 0, null);
        codec.set(col, 1, "x");
        assertNull(codec.get(col, 0));
        assertEquals("x", codec.get(col, 1));
    }
}
