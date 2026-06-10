package io.github.danielbunting.clickhouse.testutil;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.protocol.BinaryWriter;
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryReader;
import io.github.danielbunting.clickhouse.protocol.DefaultBinaryWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Test utilities for constructing in-memory {@link BinaryReader} and
 * {@link BinaryWriter} instances backed by byte arrays.
 *
 * <p>Used by unit tests to perform byte-vector / round-trip assertions
 * without a running ClickHouse server.
 */
public final class Bytes {

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private Bytes() {}

    /**
     * Wraps the given byte array in a {@link DefaultBinaryReader} backed by a
     * {@link ByteArrayInputStream}.
     *
     * @param data the bytes to read
     * @return a fresh {@link BinaryReader} positioned at the start of {@code data}
     */
    public static BinaryReader reader(byte[] data) {
        return new DefaultBinaryReader(new ByteArrayInputStream(data));
    }

    /**
     * An action that writes to a {@link BinaryWriter} and may throw
     * {@link IOException} — unlike {@link java.util.function.Consumer}, this lets
     * test lambdas call the checked-exception write methods directly.
     */
    @FunctionalInterface
    public interface WriterAction {
        void write(BinaryWriter out) throws IOException;
    }

    /**
     * Invokes {@code writer} with a {@link DefaultBinaryWriter} backed by a
     * {@link ByteArrayOutputStream}, flushes, and returns the captured bytes.
     *
     * @param writer an action that writes values to the provided {@link BinaryWriter}
     * @return the bytes written by {@code writer}
     */
    public static byte[] capture(WriterAction writer) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DefaultBinaryWriter bw = new DefaultBinaryWriter(baos);
        try {
            writer.write(bw);
            bw.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return baos.toByteArray();
    }

    /**
     * Converts a byte array to a lowercase hex string, e.g. {@code {0x0a, 0xff}}
     * becomes {@code "0aff"}. Useful in assertion failure messages.
     *
     * @param bytes the bytes to format; must not be {@code null}
     * @return a lowercase hex string of length {@code 2 * bytes.length}
     */
    public static String hex(byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException("bytes");
        }
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            result[i * 2]     = HEX_CHARS[b >>> 4];
            result[i * 2 + 1] = HEX_CHARS[b & 0x0F];
        }
        return new String(result);
    }
}
