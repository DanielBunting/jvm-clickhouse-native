package io.github.danielbunting.clickhouse.types;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.github.danielbunting.clickhouse.types.codec.UInt32Codec;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link DefaultCodecRegistry} memoization. */
class DefaultCodecRegistryTest {

    @Test
    void resolvesViaParser() {
        DefaultCodecRegistry registry = new DefaultCodecRegistry();
        assertInstanceOf(UInt32Codec.class, registry.resolve("UInt32"));
    }

    @Test
    void memoizesSameTypeString() {
        DefaultCodecRegistry registry = new DefaultCodecRegistry();
        ColumnCodec<?> first = registry.resolve("Array(UInt32)");
        ColumnCodec<?> second = registry.resolve("Array(UInt32)");
        // Same cache key -> same instance returned.
        assertSame(first, second);
    }

    @Test
    void distinctTypesAreCachedSeparately() {
        DefaultCodecRegistry registry = new DefaultCodecRegistry();
        ColumnCodec<?> a = registry.resolve("UInt32");
        ColumnCodec<?> b = registry.resolve("Int32");
        assertInstanceOf(UInt32Codec.class, a);
        assertSame(a, registry.resolve("UInt32"));
        assertSame(b, registry.resolve("Int32"));
    }
}
