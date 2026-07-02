package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Connection-failure mapping: an unreachable endpoint or a rejected handshake must surface as an
 * {@link AdbcException} from {@code connect()} (the boundary wraps the native failure), not leak a
 * raw client exception. Pairs with the success path in {@link AdbcSmokeIT}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcConnectionErrorsIT extends AdbcIntegrationTest {

    @Test
    @DisplayName("connecting to a closed port raises AdbcException(IO)")
    void unreachableEndpointRaisesIo(BufferAllocator allocator) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, CLICKHOUSE.getHost());
        // Port 1 is reserved and never has a ClickHouse listener: connection refused.
        params.put(AdbcParams.PARAM_PORT, 1);

        try (AdbcDatabase database = new ChAdbcDriver(allocator).open(params)) {
            AdbcException ex = assertThrows(AdbcException.class, database::connect);
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
        }
    }

    @Test
    @DisplayName("an unknown user is rejected at the handshake with an AdbcException")
    void badCredentialsRaiseAdbcException(BufferAllocator allocator) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, CLICKHOUSE.getHost());
        params.put(AdbcParams.PARAM_PORT, CLICKHOUSE.getMappedPort(NATIVE_PORT));
        AdbcDriver.PARAM_USERNAME.set(params, "no_such_user_" + System.nanoTime());
        AdbcDriver.PARAM_PASSWORD.set(params, "wrong");

        try (AdbcDatabase database = new ChAdbcDriver(allocator).open(params)) {
            assertThrows(AdbcException.class, database::connect);
        }
    }
}
