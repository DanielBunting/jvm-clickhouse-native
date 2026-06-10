package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import org.junit.jupiter.api.Test;

import java.io.EOFException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Robustness tests for {@link ArrayColumnCodec} / {@link MapColumnCodec} against hostile
 * wire offsets. Both codecs read a leading section of cumulative {@code UInt64} end-offsets
 * that drive how many inner elements they allocate and slice. An out-of-range or
 * non-monotonic offset must be rejected with a {@link ProtocolException} rather than crash
 * the decode with an unchecked {@code ArithmeticException}/{@code NegativeArraySizeException}
 * or force an unbounded allocation; a truncated value section must surface as an
 * {@link EOFException}.
 */
class CompositeOverflowTest {

    @Test
    void arrayOffsetAboveIntMax_isRejected() {
        // A single end-offset of 3 billion would allocate a 3-billion-element inner column.
        byte[] wire = Bytes.capture(w -> w.writeUInt64(3_000_000_000L));
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] dest = codec.allocate(1);
        assertThrows(ProtocolException.class, () -> codec.read(Bytes.reader(wire), 1, dest));
    }

    @Test
    void arrayOffsetWithHighBitSet_isRejected() {
        // 0xFFFF...FF reads as a negative long (a UInt64 above 2^63); rejected as non-monotonic
        // against the implicit leading 0.
        byte[] wire = Bytes.capture(w -> w.writeUInt64(-1L));
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] dest = codec.allocate(1);
        assertThrows(ProtocolException.class, () -> codec.read(Bytes.reader(wire), 1, dest));
    }

    @Test
    void arrayNonMonotonicOffsets_areRejected() {
        // Cumulative offsets must be non-decreasing; [5, 2] is corrupt.
        byte[] wire = Bytes.capture(w -> {
            w.writeUInt64(5L);
            w.writeUInt64(2L);
        });
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] dest = codec.allocate(2);
        assertThrows(ProtocolException.class, () -> codec.read(Bytes.reader(wire), 2, dest));
    }

    @Test
    void arrayTruncatedValues_throwEof() {
        // Offset says 3 inner values follow, but no value bytes are present.
        byte[] wire = Bytes.capture(w -> w.writeUInt64(3L));
        ArrayColumnCodec codec = new ArrayColumnCodec(new UInt32Codec());
        Object[] dest = codec.allocate(1);
        assertThrows(EOFException.class, () -> codec.read(Bytes.reader(wire), 1, dest));
    }

    @Test
    void mapOffsetAboveIntMax_isRejected() {
        byte[] wire = Bytes.capture(w -> w.writeUInt64(3_000_000_000L));
        MapColumnCodec codec = new MapColumnCodec(new StringColumnCodec(), new UInt32Codec());
        MapColumn dest = codec.allocate(1);
        assertThrows(ProtocolException.class, () -> codec.read(Bytes.reader(wire), 1, dest));
    }

    @Test
    void mapNonMonotonicOffsets_areRejected() {
        byte[] wire = Bytes.capture(w -> {
            w.writeUInt64(4L);
            w.writeUInt64(1L);
        });
        MapColumnCodec codec = new MapColumnCodec(new StringColumnCodec(), new UInt32Codec());
        MapColumn dest = codec.allocate(2);
        assertThrows(ProtocolException.class, () -> codec.read(Bytes.reader(wire), 2, dest));
    }
}
