package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Server-free unit coverage for {@link ChAdbcDriver}/{@link ChAdbcDatabase}: driver.open is lazy
 * (no connection until {@code connect()}), parameter failures surface as INVALID_ARGUMENT, the
 * allocator tree is rooted at the driver, and database close is idempotent. The ADBC analogue of
 * the JDBC module's {@code ClickHouseDriverTest} acceptance kernel (URL-shape coverage itself
 * lives in {@link AdbcParamsTest}).
 */
@ExtendWith(ArrowAllocatorExtension.class)
class ChAdbcDriverDatabaseTest {

    private static Map<String, Object> validParams() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "db.example.com");
        params.put(AdbcParams.PARAM_PORT, 9000);
        return params;
    }

    @Test
    @DisplayName("open() parses parameters and returns a database without connecting")
    void openIsLazy(BufferAllocator allocator) throws Exception {
        // "db.example.com" is unroutable from a unit test — open() must succeed anyway
        // because no native connection is made until connect().
        AdbcDriver driver = new ChAdbcDriver(allocator);
        try (AdbcDatabase database = driver.open(validParams())) {
            assertInstanceOf(ChAdbcDatabase.class, database);
        }
    }

    @Test
    @DisplayName("open() with invalid parameters raises INVALID_ARGUMENT and leaks nothing")
    void openWithInvalidParamsThrows(BufferAllocator allocator) {
        AdbcDriver driver = new ChAdbcDriver(allocator);
        AdbcException ex = assertThrows(AdbcException.class, () -> driver.open(new HashMap<>()));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
        // The leak-check extension proves at teardown that the aborted open released its
        // child allocator.
    }

    @Test
    @DisplayName("each open() roots a child allocator under the driver; close() releases it")
    void openBuildsAndCloseReleasesChildAllocator(BufferAllocator allocator) throws Exception {
        AdbcDriver driver = new ChAdbcDriver(allocator);
        AdbcDatabase database = driver.open(validParams());
        assertEquals(1, allocator.getChildAllocators().size(),
                "the database must allocate from a child of the driver allocator");
        database.close();
        assertTrue(allocator.getChildAllocators().isEmpty(),
                "closing the database must release its child allocator");
    }

    @Test
    @DisplayName("closing a database twice is safe")
    void databaseDoubleCloseIsSafe(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(validParams());
        database.close();
        database.close();
    }

    @Test
    @DisplayName("FACTORY builds a working driver for AdbcDriverManager registration")
    void factoryBuildsDriver(BufferAllocator allocator) throws Exception {
        AdbcDriver driver = ChAdbcDriver.FACTORY.apply(allocator);
        assertNotNull(driver);
        try (AdbcDatabase database = driver.open(validParams())) {
            assertInstanceOf(ChAdbcDatabase.class, database);
        }
    }

    @Test
    @DisplayName("the driver name constant is the documented registration key")
    void driverNameConstant() {
        assertEquals("clickhouse-native-client-adbc", ChAdbcDriver.DRIVER_NAME);
    }

    @Test
    @DisplayName("open() accepts a URI parameter map without connecting")
    void openAcceptsUriParams(BufferAllocator allocator) throws Exception {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://db.example.com:9000/analytics");
        try (AdbcDatabase database = new ChAdbcDriver(allocator).open(params)) {
            assertInstanceOf(ChAdbcDatabase.class, database);
        }
    }

    @Test
    @DisplayName("open() with a malformed URI raises INVALID_ARGUMENT echoing the URI")
    void openWithMalformedUriThrows(BufferAllocator allocator) {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "http://not-chnative:9000");
        AdbcException ex = assertThrows(AdbcException.class,
                () -> new ChAdbcDriver(allocator).open(params));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
        assertTrue(ex.getMessage().contains("http://not-chnative:9000"));
    }
}
