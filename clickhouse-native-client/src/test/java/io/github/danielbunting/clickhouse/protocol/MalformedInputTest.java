package io.github.danielbunting.clickhouse.protocol;

import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.CodecRegistry;
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Robustness tests for decoding malformed / truncated / hostile input. A buggy or
 * adversarial server must not be able to make the client crash with an unchecked
 * exception, hang, or attempt an unbounded allocation: every bad input should surface
 * as a typed, checked failure ({@link ProtocolException} for a protocol violation,
 * {@link EOFException} for a truncated stream, or another {@link IOException}).
 *
 * <p>All input is crafted in-memory via {@link Bytes}; no server is required.
 */
class MalformedInputTest {

    private final CodecRegistry registry = new DefaultCodecRegistry();

    // =====================================================================
    // BinaryReader-level: bounded varint, string length, truncation.
    // =====================================================================

    @Test
    void overlongVarUInt_isRejected() {
        // 11 continuation bytes exceed the 10-byte ceiling for a 64-bit LEB128 value.
        byte[] wire = new byte[11];
        java.util.Arrays.fill(wire, (byte) 0x80);
        assertThrows(IOException.class, () -> Bytes.reader(wire).readVarUInt());
    }

    @Test
    void stringLengthAboveIntMax_isRejected() {
        // A String is a VarUInt byte-length + payload; a length above Integer.MAX_VALUE
        // must be rejected before any attempt to allocate/read the payload.
        byte[] wire = Bytes.capture(w -> w.writeVarUInt(1L << 35)); // ~34 GiB length prefix
        assertThrows(IOException.class, () -> Bytes.reader(wire).readString());
    }

    @Test
    void readBytesPastEndOfStream_throwsEof() {
        // Only 3 bytes available but 10 requested -> clean EOFException, not a short array.
        assertThrows(EOFException.class, () -> Bytes.reader(new byte[]{1, 2, 3}).readBytes(10));
    }

    // =====================================================================
    // BlockCodec-level: hostile row/column counts and truncated columns.
    // =====================================================================

    /** Writes a valid leading block-info (is_overflows=0, bucket_num=-1, terminator). */
    private static void writeBlockInfo(BinaryWriter w) throws IOException {
        w.writeVarUInt(1);
        w.writeUInt8(0);
        w.writeVarUInt(2);
        w.writeInt32(-1);
        w.writeVarUInt(0);
    }

    @Test
    void hugeRowCount_isRejectedAsProtocolException() {
        // num_rows far above Integer.MAX_VALUE would drive an unbounded allocation.
        byte[] wire = Bytes.capture(w -> {
            writeBlockInfo(w);
            w.writeVarUInt(1);          // num_columns
            w.writeVarUInt(1L << 40);   // num_rows (~1 trillion)
        });
        assertThrows(ProtocolException.class, () -> BlockCodec.read(Bytes.reader(wire), registry));
    }

    @Test
    void hugeColumnCount_isRejectedAsProtocolException() {
        byte[] wire = Bytes.capture(w -> {
            writeBlockInfo(w);
            w.writeVarUInt(1L << 40);   // num_columns
            w.writeVarUInt(1);          // num_rows
        });
        assertThrows(ProtocolException.class, () -> BlockCodec.read(Bytes.reader(wire), registry));
    }

    @Test
    void rowCountThatWouldWrapNegative_isRejectedNotCrashed() {
        // 3_000_000_000 is within VarUInt/long range but wraps to a negative int when cast.
        // Before the guard this produced a NegativeArraySizeException on allocate; now it is
        // a clean ProtocolException.
        byte[] wire = Bytes.capture(w -> {
            writeBlockInfo(w);
            w.writeVarUInt(1);            // num_columns
            w.writeVarUInt(3_000_000_000L); // num_rows -> (int) wraps negative
        });
        assertThrows(ProtocolException.class, () -> BlockCodec.read(Bytes.reader(wire), registry));
    }

    @Test
    void truncatedColumnValues_throwEof() {
        // Header promises 3 UInt32 rows but the value bytes are missing entirely.
        byte[] wire = Bytes.capture(w -> {
            writeBlockInfo(w);
            w.writeVarUInt(1);   // num_columns
            w.writeVarUInt(3);   // num_rows
            w.writeString("v");
            w.writeString("UInt32");
            // (revision 0 => no custom-serialization byte; UInt32 has an empty state prefix)
            // ...and then nothing: the 12 value bytes never arrive.
        });
        assertThrows(EOFException.class, () -> BlockCodec.read(Bytes.reader(wire), registry));
    }
}
