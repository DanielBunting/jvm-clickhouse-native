package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Per-test Arrow {@link BufferAllocator}: injected as a test-method parameter and, at
 * teardown, asserted to hold zero outstanding bytes before being closed.
 *
 * <p>Off-heap Arrow buffers do not show up in JVM heap dumps, so a leak here is otherwise
 * invisible; this extension is the single highest-value guardrail for the bridge. Any test
 * that allocates Arrow memory must declare a {@code BufferAllocator} parameter and free
 * everything it allocates (close roots/readers) before returning.
 */
public final class ArrowAllocatorExtension
        implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final ExtensionContext.Namespace NS =
            ExtensionContext.Namespace.create(ArrowAllocatorExtension.class);
    private static final String KEY = "allocator";

    @Override
    public void beforeEach(ExtensionContext context) {
        context.getStore(NS).put(KEY, new RootAllocator(Long.MAX_VALUE));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        BufferAllocator allocator = context.getStore(NS).get(KEY, BufferAllocator.class);
        if (allocator == null) {
            return;
        }
        try {
            long outstanding = allocator.getAllocatedMemory();
            assertEquals(
                    0L,
                    outstanding,
                    () -> "Arrow buffers leaked: " + outstanding + " bytes still allocated\n"
                            + allocator.toVerboseString());
        } finally {
            allocator.close();
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType() == BufferAllocator.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return extensionContext.getStore(NS).get(KEY, BufferAllocator.class);
    }
}
