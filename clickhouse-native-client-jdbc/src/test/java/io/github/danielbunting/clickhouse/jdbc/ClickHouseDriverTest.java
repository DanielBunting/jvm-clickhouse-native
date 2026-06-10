package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClickHouseDriver} URL acceptance and {@link DriverManager} registration.
 *
 * <p>These tests do not open any network connection and therefore are not tagged
 * {@code integration}.
 */
class ClickHouseDriverTest {

    @Test
    @DisplayName("acceptsURL accepts jdbc:chnative:// URLs")
    void acceptsValidUrls() throws Exception {
        ClickHouseDriver driver = new ClickHouseDriver();
        assertTrue(driver.acceptsURL("jdbc:chnative://localhost:9000/default"));
        assertTrue(driver.acceptsURL("jdbc:chnative://host:9000/db?compression=lz4"));
    }

    @Test
    @DisplayName("acceptsURL rejects non-chnative and malformed URLs")
    void rejectsInvalidUrls() throws Exception {
        ClickHouseDriver driver = new ClickHouseDriver();
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/db"));
        assertFalse(driver.acceptsURL("chnative://localhost:9000/default"));
        assertFalse(driver.acceptsURL("jdbc:mysql://host:3306/db"));
        assertFalse(driver.acceptsURL(""));
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    @DisplayName("driver is not JDBC-compliant and reports sensible versions")
    void metadata() {
        ClickHouseDriver driver = new ClickHouseDriver();
        assertFalse(driver.jdbcCompliant());
        assertTrue(driver.getMajorVersion() >= 1);
        assertTrue(driver.getMinorVersion() >= 0);
    }

    @Test
    @DisplayName("DriverManager finds a driver for a jdbc:chnative URL")
    void registeredWithDriverManager() throws Exception {
        // Touch the class so its static initializer runs (registers with DriverManager).
        // ServiceLoader registration also covers this when on the classpath.
        Class.forName(ClickHouseDriver.class.getName());

        Driver found = DriverManager.getDriver("jdbc:chnative://localhost:9000/default");
        assertNotNull(found, "DriverManager should resolve a driver for jdbc:chnative URLs");
        assertSame(ClickHouseDriver.class, found.getClass());
    }
}
