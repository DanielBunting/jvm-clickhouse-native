package io.github.danielbunting.clickhouse.types.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for {@link ArrayColumnCodec} using a real {@link UInt32Codec}
 * inner codec and the {@code testutil.Bytes} helpers.
 */
class ArrayColumnCodecTest {

    @Test
    void roundTripArrayOfUInt32() throws IOException {
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());

        // Three rows: [1, 2, 3], [], [42].
        Object[] src = codec.allocate(3);
        src[0] = List.of(1L, 2L, 3L);
        src[1] = List.of();
        src[2] = List.of(42L);

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        BinaryReader in = Bytes.reader(wire);
        // Offsets are cumulative end-offsets: 3, 3, 4 (UInt64 each).
        assertEquals(3L, in.readUInt64());
        assertEquals(3L, in.readUInt64());
        assertEquals(4L, in.readUInt64());
        // Then four flattened UInt32 values: 1, 2, 3, 42.
        assertEquals(1L, in.readUInt32());
        assertEquals(2L, in.readUInt32());
        assertEquals(3L, in.readUInt32());
        assertEquals(42L, in.readUInt32());

        Object[] dest = codec.allocate(3);
        codec.read(Bytes.reader(wire), 3, dest);
        assertEquals(List.of(1L, 2L, 3L), dest[0]);
        assertEquals(List.of(), dest[1]);
        assertEquals(List.of(42L), dest[2]);
    }

    @Test
    void roundTripNestedArrayOfArrayOfUInt32() throws IOException {
        ArrayColumnCodec codec = new ArrayColumnCodec(new ArrayColumnCodec(new UInt32Codec()));

        // Two rows of Array(Array(UInt32)):
        //   row 0: [[1, 2], [3]]
        //   row 1: [[], [4, 5, 6]]
        Object[] src = codec.allocate(2);
        src[0] = List.of(List.of(1L, 2L), List.of(3L));
        src[1] = List.of(List.of(), List.of(4L, 5L, 6L));

        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, src, 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Object[] dest = codec.allocate(2);
        codec.read(Bytes.reader(wire), 2, dest);

        assertEquals(List.of(List.of(1L, 2L), List.of(3L)), dest[0]);
        assertEquals(List.of(List.of(), List.of(4L, 5L, 6L)), dest[1]);
    }

    @Test
    void emptyColumnWritesAndReadsNothingExtra() throws IOException {
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        byte[] wire = Bytes.capture(w -> {
            try {
                codec.write(w, codec.allocate(0), 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertArrayEquals(new byte[0], wire);

        Object[] dest = codec.allocate(0);
        codec.read(Bytes.reader(wire), 0, dest);
        assertEquals(0, dest.length);
    }

    @Test
    void metadataReflectsInnerCodec() {
        ColumnCodec<?> inner = new UInt32Codec();
        ArrayColumnCodec codec = new ArrayColumnCodec(inner);
        assertEquals("Array(" + inner.typeName() + ")", codec.typeName());
        assertEquals(List.class, codec.javaType());
        assertEquals(inner, codec.inner());
    }

    @Test
    void getAndSetBridgeRowLists() {
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] arr = codec.allocate(1);
        codec.set(arr, 0, List.of(7L, 8L));
        assertEquals(List.of(7L, 8L), codec.get(arr, 0));
    }

    @Test
    void rejectsNonListElementOnWrite() {
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] src = codec.allocate(1);
        src[0] = "not a list";
        assertThrows(IllegalArgumentException.class, () ->
                Bytes.capture(w -> {
                    try {
                        codec.write(w, src, 1);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }
}
