package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.DefaultTypeParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link JsonColumnCodec}: the plain (no-declaration) constructor, the
 * stashed-vs-inline prefix read paths, the typed-path write rejection, and the
 * {@code JSON(...)} typed-path declaration classification (parameters, {@code SKIP} /
 * {@code SKIP REGEXP} clauses, backticked names) as observed through the public API.
 *
 * <p>Wire round-trips against a live server are covered by {@code JsonTypesIT}; these
 * tests pin the codec's own encode/decode symmetry and its declaration parsing without
 * a server.
 */
class JsonColumnCodecTest {

    private final DefaultTypeParser parser = new DefaultTypeParser();

    /** Writes the given rows through {@code codec} and returns the raw wire bytes. */
    private static byte[] encode(JsonColumnCodec codec, Object... rows) {
        Object[] src = codec.allocate(rows.length);
        for (int i = 0; i < rows.length; i++) {
            codec.set(src, i, rows[i]);
        }
        return Bytes.capture(w -> codec.write(w, src, rows.length));
    }

    /**
     * The single-argument (no typed paths) constructor round-trips a flat object through
     * the {@code readStatePrefix -> read} sequence used for a top-level column: the
     * prefix is stashed by {@code readStatePrefix} and consumed by {@code read}.
     */
    @Test
    void plainConstructorRoundTripsViaStashedPrefix() throws IOException {
        JsonColumnCodec codec = new JsonColumnCodec(parser);

        byte[] wire = encode(codec, "{\"b\":\"x\",\"a\":1,\"f\":1.5,\"t\":true}", null, "{\"a\":2}");

        Object[] dst = codec.allocate(3);
        BinaryReader in = Bytes.reader(wire);
        codec.readStatePrefix(in);
        codec.read(in, 3, dst);

        assertEquals("{\"a\":1,\"b\":\"x\",\"f\":1.5,\"t\":true}", codec.get(dst, 0),
                "decoded object must carry all paths in sorted-key order");
        assertEquals("{}", codec.get(dst, 1), "a null source row decodes to the empty object");
        assertEquals("{\"a\":2}", codec.get(dst, 2), "absent paths are omitted, not null-valued");
    }

    /**
     * {@code read} without a prior {@code readStatePrefix} (the layout of a JSON value
     * nested inside another JSON column's prefix) consumes the prefix INLINE before the
     * body — the {@code stashed ?: readPrefix(in)} fallback.
     */
    @Test
    void readWithoutStatePrefixConsumesInlinePrefix() throws IOException {
        JsonColumnCodec codec = new JsonColumnCodec(parser);

        byte[] wire = encode(codec, "{\"a\":1}", "{\"a\":3}");

        Object[] dst = codec.allocate(2);
        codec.read(Bytes.reader(wire), 2, dst);

        assertEquals("{\"a\":1}", codec.get(dst, 0));
        assertEquals("{\"a\":3}", codec.get(dst, 1));
    }

    /**
     * Writing into a {@code JSON(...)} column that declares TYPED paths is rejected up
     * front (the writer emits every path as Dynamic, which the server would misread).
     * The exception message lists exactly the typed-path names in canonical sorted
     * order, which pins the declaration classification: {@code max_dynamic_* = N}
     * parameters (spaced or not), {@code SKIP path} and {@code SKIP REGEXP '...'}
     * clauses are all ignored; a backticked name is unquoted; an Enum type's nested
     * {@code '='} does not make the entry a parameter.
     */
    @Test
    void writeIntoTypedPathJsonRejectedListingTypedPaths() {
        JsonColumnCodec codec = new JsonColumnCodec(parser,
                "JSON(max_dynamic_paths = 8, max_dynamic_types=16, `k.1` Int64,"
                        + " status Enum8('ok' = 1, 'err' = 2), SKIP a.b,"
                        + " SKIP REGEXP 'tmp.*', c String)");

        Object[] src = codec.allocate(1);
        codec.set(src, 0, "{\"c\":\"v\"}");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Bytes.capture(w -> codec.write(w, src, 1)));
        assertTrue(ex.getMessage().contains("typed: c, k.1, status"),
                "message must list exactly the sorted typed paths, was: " + ex.getMessage());
    }

    /**
     * Degenerate declarations — no parentheses (a plain {@code JSON} column type),
     * unbalanced parens, empty args, and a bare token that is neither a parameter nor a
     * {@code name Type} pair — all classify as "no typed paths": the codec behaves as a
     * plain JSON codec and round-trips instead of throwing.
     */
    @Test
    void degenerateDeclarationsYieldNoTypedPaths() throws IOException {
        for (String declaration : new String[] {"JSON", "JSON(", "JSON()", "JSON(solo)"}) {
            JsonColumnCodec codec = new JsonColumnCodec(parser, declaration);

            byte[] wire = encode(codec, "{\"a\":1}");
            Object[] dst = codec.allocate(1);
            BinaryReader in = Bytes.reader(wire);
            codec.readStatePrefix(in);
            codec.read(in, 1, dst);

            assertEquals("{\"a\":1}", codec.get(dst, 0),
                    "declaration " + declaration + " must act as a plain JSON column");
        }
    }
}
