package io.github.danielbunting.clickhouse.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ProtocolException;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.codec.StringColumn;
import io.github.danielbunting.clickhouse.types.codec.StringColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt64Codec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Authoritative byte-level unit tests for {@link SparseDecoder}: the sparse-expand
 * routine is fed hand-crafted {@code SparseOffsets} + {@code SparseElements} bytes built
 * exactly as ClickHouse's {@code SerializationSparse::serializeOffsets} would emit them,
 * and must reconstruct the dense column.
 *
 * <p>These are pure unit tests (no server) because ClickHouse 25.6 does not transmit
 * sparse columns in Native query results — the read pipeline materialises sparse on-disk
 * columns to {@code Default} before sending them — so an integration test cannot exercise
 * this path. The byte layout below is therefore the authority.
 */
class SparseDecoderTest {

    /** ClickHouse {@code END_OF_GRANULE_FLAG = 1ULL << 62}. */
    private static final long END_OF_GRANULE_FLAG = 1L << 62;

    /**
     * Builds a sparse offsets stream for {@code rowCount} rows with non-default values at
     * the given ascending {@code positions}, mirroring ClickHouse's serializeOffsets:
     * for each non-default at {@code off} write {@code off - start} then {@code start =
     * off + 1}; finally write {@code (rowCount > start ? rowCount - start : 0) |
     * END_OF_GRANULE_FLAG}.
     */
    private static void writeOffsets(BinaryWriter w, int rowCount, int... positions)
            throws IOException {
        long start = 0;
        for (int off : positions) {
            w.writeVarUInt(off - start);
            start = off + 1;
        }
        long trailing = rowCount > start ? rowCount - start : 0;
        w.writeVarUInt(trailing | END_OF_GRANULE_FLAG);
    }

    /** UInt64 sparse column: non-default values scattered among default (0) rows. */
    @Test
    void expandsUInt64SparseColumn() {
        int rowCount = 10;
        int[] positions = {2, 5, 9};
        long[] nonDefault = {100L, 200L, 300L};

        byte[] wire = Bytes.capture(w -> {
            try {
                writeOffsets(w, rowCount, positions);
                w.writeFrom(nonDefault, nonDefault.length); // SparseElements: dense UInt64
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        long[] dense;
        try {
            dense = SparseDecoder.decode(Bytes.reader(wire), rowCount, new UInt64Codec());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertArrayEquals(
                new long[] {0, 0, 100, 0, 0, 200, 0, 0, 0, 300}, dense,
                "non-default values must land at positions 2,5,9; all else default 0");
    }

    /** A non-default value at row 0 (leading group size 0) round-trips. */
    @Test
    void expandsLeadingNonDefault() {
        int rowCount = 4;
        byte[] wire = Bytes.capture(w -> {
            try {
                writeOffsets(w, rowCount, 0, 1); // values at 0 and 1, then 2 trailing defaults
                w.writeFrom(new long[] {7L, 8L}, 2);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        long[] dense;
        try {
            dense = SparseDecoder.decode(Bytes.reader(wire), rowCount, new UInt64Codec());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertArrayEquals(new long[] {7, 8, 0, 0}, dense);
    }

    /** An all-default column is just a terminator carrying the full row count as trailing. */
    @Test
    void expandsAllDefaultColumn() {
        int rowCount = 5;
        byte[] wire = Bytes.capture(w -> {
            try {
                writeOffsets(w, rowCount); // no non-default positions
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        long[] dense;
        try {
            dense = SparseDecoder.decode(Bytes.reader(wire), rowCount, new UInt64Codec());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertArrayEquals(new long[] {0, 0, 0, 0, 0}, dense);
    }

    /**
     * String (reference-backed) sparse column: default slots must be filled with the
     * empty string, not left {@code null}, and the non-default values placed correctly.
     */
    @Test
    void expandsStringSparseColumn() {
        int rowCount = 6;
        int[] positions = {1, 4};
        String[] nonDefault = {"hello", "world"};

        byte[] wire = Bytes.capture(w -> {
            try {
                writeOffsets(w, rowCount, positions);
                // SparseElements: dense String values (VarUInt len + UTF-8 bytes).
                for (String s : nonDefault) {
                    byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    w.writeVarUInt(b.length);
                    w.writeBytes(b, 0, b.length);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        StringColumnCodec codec = new StringColumnCodec();
        StringColumn col;
        try {
            col = SparseDecoder.decode(Bytes.reader(wire), rowCount, codec);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<String> values = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            values.add(codec.getString(col, i));
        }
        assertEquals(List.of("", "hello", "", "", "world", ""), values,
                "default slots must be empty String, non-defaults at 1 and 4");
    }

    /** A non-default position beyond the row count is rejected (defensive). */
    @Test
    void rejectsPositionBeyondRowCount() {
        int rowCount = 2;
        byte[] wire = Bytes.capture(w -> {
            try {
                // group size 5 places a non-default at row 5, which exceeds rowCount=2.
                w.writeVarUInt(5);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        assertThrows(ProtocolException.class,
                () -> SparseDecoder.decode(Bytes.reader(wire), rowCount, new UInt64Codec()));
    }

    /** A terminator that does not land exactly on the row count is rejected. */
    @Test
    void rejectsTerminatorRowMismatch() {
        int rowCount = 5;
        byte[] wire = Bytes.capture(w -> {
            try {
                // value at row 0, then terminator claiming 1 trailing default -> ends at
                // row 2, not 5.
                w.writeVarUInt(0);
                w.writeVarUInt(1L | END_OF_GRANULE_FLAG);
                w.writeFrom(new long[] {9L}, 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        assertThrows(ProtocolException.class,
                () -> SparseDecoder.decode(Bytes.reader(wire), rowCount, new UInt64Codec()));
    }
}
