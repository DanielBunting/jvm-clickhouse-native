package io.github.danielbunting.clickhouse.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.LoadBalancingPolicy;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
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
    @DisplayName("reference-client scheme abbreviations are deliberately not accepted")
    void rejectsReferenceClientSchemes() throws Exception {
        // The reference client accepts jdbc:ch:/jdbc:clickhouse: plus protocol-prefixed
        // abbreviations (v1 ClickHouseJdbcUrlParserTest#testParseAbbrevation). This
        // driver is single-scheme by design: only jdbc:chnative:// is accepted, so the
        // two drivers can coexist on one classpath without URL ambiguity.
        ClickHouseDriver driver = new ClickHouseDriver();
        assertFalse(driver.acceptsURL("jdbc:ch://localhost:8123"));
        assertFalse(driver.acceptsURL("jdbc:clickhouse://localhost:8123"));
        assertFalse(driver.acceptsURL("jdbc:ch:http://localhost:8123"));
        assertFalse(driver.acceptsURL("jdbc:chnative:localhost:9000")); // missing "//"
    }

    @Test
    @DisplayName("connect accepts a comma-separated multi-endpoint URL (HA surface)")
    void multiEndpointUrlIsAcceptedAndParsed() throws Exception {
        // Reference: v1 ClickHouseDataSourceTest#testHighAvailabilityConfig. The JDBC
        // layer contributes only URL acceptance + delegation to ClickHouseConfig.fromUrl;
        // actual failover/rotation behaviour is covered by the native layer's
        // EndpointSelectorTest/FailoverConnectorTest.
        ClickHouseDriver driver = new ClickHouseDriver();
        String url = "jdbc:chnative://h1:9000,h2:9001,h3/db?loadBalancingPolicy=round_robin";
        assertTrue(driver.acceptsURL(url));

        // Same parse the driver performs in connect(): strip "jdbc:" and hand to the core.
        ClickHouseConfig config = ClickHouseConfig.fromUrl(url.substring("jdbc:".length()));
        assertEquals(3, config.endpoints().size());
        assertEquals("h1", config.endpoints().get(0).host());
        assertEquals(9000, config.endpoints().get(0).port());
        assertEquals("h2", config.endpoints().get(1).host());
        assertEquals(9001, config.endpoints().get(1).port());
        assertEquals("h3", config.endpoints().get(2).host());
        assertEquals(9000, config.endpoints().get(2).port(), "omitted port defaults to 9000");
        assertEquals(LoadBalancingPolicy.ROUND_ROBIN, config.loadBalancingPolicy());
        assertEquals("db", config.database());
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

    @Test
    @DisplayName("Properties user/password override credentials embedded in the URL")
    void propertiesCredentialsOverrideUrlCredentials() {
        // The driver strips "jdbc:" and delegates to ClickHouseConfig.fromUrl(url, info);
        // its documented merge rule is that user/username and password keys in the
        // Properties, if present, win over credentials embedded in the URL.
        Properties info = new Properties();
        info.setProperty("user", "prop_user");
        info.setProperty("password", "prop_pass");
        ClickHouseConfig config = ClickHouseConfig.fromUrl(
                "chnative://url_user:url_pass@localhost:9000/url_db", info);
        assertEquals("prop_user", config.username());
        assertEquals("prop_pass", config.password());
    }

    @Test
    @DisplayName("'username' Properties key is accepted as an alias for 'user'")
    void usernamePropertyAliasOverridesUrlCredentials() {
        Properties info = new Properties();
        info.setProperty("username", "prop_user");
        ClickHouseConfig config = ClickHouseConfig.fromUrl(
                "chnative://url_user:url_pass@localhost:9000/url_db", info);
        assertEquals("prop_user", config.username());
        // Password was not overridden, so the URL-embedded value survives.
        assertEquals("url_pass", config.password());
    }

    @Test
    @DisplayName("non-credential Properties keys do not override URL values")
    void nonCredentialPropertiesDoNotOverrideUrlValues() {
        // Only user/username and password participate in the merge; a "database"
        // property is ignored by the config parser, so the URL path wins.
        Properties info = new Properties();
        info.setProperty("database", "prop_db");
        ClickHouseConfig config = ClickHouseConfig.fromUrl(
                "chnative://localhost:9000/url_db?compression=lz4", info);
        assertEquals("url_db", config.database());
    }

    @Test
    @DisplayName("URL credentials survive when no Properties overrides are supplied")
    void urlCredentialsSurviveEmptyProperties() {
        ClickHouseConfig config = ClickHouseConfig.fromUrl(
                "chnative://url_user:url_pass@localhost:9000/url_db", new Properties());
        assertEquals("url_user", config.username());
        assertEquals("url_pass", config.password());
    }

    @Test
    @DisplayName("connect wraps an unknown URL parameter in an SQLException naming the key")
    void connectWrapsUnknownUrlParameterInSqlException() {
        // Reference: jdbc-v2 DriverTest#testUnknownSettings (URL path). The config parse
        // happens before any socket is opened, so this stays a pure unit test. The core
        // ClickHouseException must be wrapped in an SQLException that names the bad key.
        ClickHouseDriver driver = new ClickHouseDriver();
        SQLException e = assertThrows(SQLException.class, () -> driver.connect(
                "jdbc:chnative://localhost:9000/db?unknownParam=1", new Properties()));
        assertTrue(e.getMessage().contains("unknownParam"),
                "message should name the offending key: " + e.getMessage());
        assertNotNull(e.getCause(), "the core parse failure must be chained as the cause");
    }

    @Test
    @DisplayName("unknown Properties keys are silently ignored (deviation from jdbc-v2)")
    void unknownPropertiesKeysAreIgnored() {
        // DEVIATION from jdbc-v2 (DriverTest#testUnknownSettings rejects unknown
        // Properties keys): this driver's Properties merge only consults
        // user/username/password, so any other key is ignored rather than rejected.
        Properties info = new Properties();
        info.setProperty("unknown_setting1", "1");
        ClickHouseConfig config = ClickHouseConfig.fromUrl("chnative://localhost:9000/db", info);
        assertEquals("db", config.database());
        assertEquals("default", config.username());
    }

    @Test
    @DisplayName("Driver.getParentLogger throws SQLFeatureNotSupportedException")
    void getParentLoggerUnsupported() {
        // Reference: jdbc-v2 DriverTest#testGetParentLogger. The DataSource variant is
        // covered by ChDataSourceTest#parentLoggerUnsupported.
        ClickHouseDriver driver = new ClickHouseDriver();
        assertThrows(SQLFeatureNotSupportedException.class, driver::getParentLogger);
    }

    @Test
    @DisplayName("getPropertyInfo returns an empty array by documented contract")
    void getPropertyInfoReturnsEmptyArray() {
        ClickHouseDriver driver = new ClickHouseDriver();
        DriverPropertyInfo[] propertyInfo =
                driver.getPropertyInfo("jdbc:chnative://localhost:9000/default", new Properties());
        assertNotNull(propertyInfo);
        assertEquals(0, propertyInfo.length);

        // The arguments are documented as ignored, so nulls are tolerated too.
        assertEquals(0, driver.getPropertyInfo(null, null).length);
    }
}
