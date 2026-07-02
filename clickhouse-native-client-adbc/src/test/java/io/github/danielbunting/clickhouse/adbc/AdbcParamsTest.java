package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Offline unit coverage for {@link AdbcParams#toConfig}, the connection-parameter parsing surface.
 * Pure parsing — no server — so these run as plain unit tests (no {@code @Tag("integration")}).
 * The happy paths are exercised by the integration suite; this pins the validation/error arms that
 * a real connection never reaches.
 */
class AdbcParamsTest {

    // ---- discrete host/port/database ------------------------------------------------------

    @Test
    @DisplayName("discrete host/port/database build a config reporting those values")
    void discreteParamsBuildConfig() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "db.example.com");
        params.put(AdbcParams.PARAM_PORT, 9123);
        params.put(AdbcParams.PARAM_DATABASE, "analytics");

        ClickHouseConfig config = AdbcParams.toConfig(params);
        assertEquals("db.example.com", config.host());
        assertEquals(9123, config.port());
        assertEquals("analytics", config.database());
    }

    @Test
    @DisplayName("a numeric string port is accepted (driver-manager passes string options)")
    void portAsNumericStringIsAccepted() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "localhost");
        params.put(AdbcParams.PARAM_PORT, "9001");

        assertEquals(9001, AdbcParams.toConfig(params).port());
    }

    @Test
    @DisplayName("username/password options flow onto the discrete-host config")
    void discreteParamsCarryCredentials() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "localhost");
        AdbcDriver.PARAM_USERNAME.set(params, "reader");
        AdbcDriver.PARAM_PASSWORD.set(params, "s3cret");

        ClickHouseConfig config = AdbcParams.toConfig(params);
        assertEquals("reader", config.username());
        assertEquals("s3cret", config.password());
    }

    // ---- discrete-path validation ---------------------------------------------------------

    @Test
    @DisplayName("neither URI nor host raises INVALID_ARGUMENT naming both keys")
    void missingConnectionTargetIsInvalidArgument() {
        AdbcException ex = assertThrows(AdbcException.class, () -> AdbcParams.toConfig(new HashMap<>()));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains(AdbcParams.PARAM_HOST), "message should name the host key");
        assertTrue(ex.getMessage().contains(AdbcDriver.PARAM_URI.getKey()), "message should name the URI key");
    }

    @Test
    @DisplayName("a non-numeric string port raises INVALID_ARGUMENT naming the port key")
    void nonNumericStringPortIsInvalidArgument() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "localhost");
        params.put(AdbcParams.PARAM_PORT, "not-a-number");

        AdbcException ex = assertThrows(AdbcException.class, () -> AdbcParams.toConfig(params));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
        assertTrue(ex.getMessage().contains(AdbcParams.PARAM_PORT), "message should name the port key");
    }

    @Test
    @DisplayName("a port of an unexpected type raises INVALID_ARGUMENT")
    void wrongTypePortIsInvalidArgument() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "localhost");
        params.put(AdbcParams.PARAM_PORT, Boolean.TRUE);

        AdbcException ex = assertThrows(AdbcException.class, () -> AdbcParams.toConfig(params));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
    }

    // ---- URI form -------------------------------------------------------------------------

    @Test
    @DisplayName("a chnative:// URI resolves host, port and database")
    void uriResolvesHostPortDatabase() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://myhost:9001/mydb");

        ClickHouseConfig config = AdbcParams.toConfig(params);
        assertEquals("myhost", config.host());
        assertEquals(9001, config.port());
        assertEquals("mydb", config.database());
    }

    @Test
    @DisplayName("username/password options override the URI credentials")
    void uriCredentialsOverriddenByOptions() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://myhost:9001");
        AdbcDriver.PARAM_USERNAME.set(params, "override-user");
        AdbcDriver.PARAM_PASSWORD.set(params, "override-pass");

        ClickHouseConfig config = AdbcParams.toConfig(params);
        assertEquals("override-user", config.username());
        assertEquals("override-pass", config.password());
    }

    @Test
    @DisplayName("a non-chnative scheme raises INVALID_ARGUMENT and preserves the cause")
    void invalidUriSchemeIsInvalidArgument() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "jdbc://myhost:9001/mydb");

        AdbcException ex = assertThrows(AdbcException.class, () -> AdbcParams.toConfig(params));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
        assertNotNull(ex.getCause(), "the underlying parse failure should be retained as the cause");
        assertTrue(ex.getMessage().contains("jdbc://myhost:9001/mydb"), "message should echo the bad URI");
    }

    @Test
    @DisplayName("a URI with no host raises INVALID_ARGUMENT")
    void uriMissingHostIsInvalidArgument() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative:///mydb");

        AdbcException ex = assertThrows(AdbcException.class, () -> AdbcParams.toConfig(params));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
    }

    @Test
    @DisplayName("URI user-info credentials are parsed when no credential options are given")
    void uriCredentialsParsedFromUserInfo() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://alice:s3cret@myhost:9001/mydb");

        ClickHouseConfig config = AdbcParams.toConfig(params);
        assertEquals("alice", config.username());
        assertEquals("s3cret", config.password());
    }

    @Test
    @DisplayName("a password option alone overrides only the password; the URI username stays")
    void passwordOptionAloneOverridesOnlyPassword() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://alice:s3cret@myhost:9001");
        AdbcDriver.PARAM_PASSWORD.set(params, "rotated");

        ClickHouseConfig config = AdbcParams.toConfig(params);
        assertEquals("alice", config.username());
        assertEquals("rotated", config.password());
    }

    @Test
    @DisplayName("URI query options (e.g. connectTimeout) flow onto the config")
    void uriQueryOptionsApplied() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://myhost:9001/mydb?connectTimeout=7");

        assertEquals(java.time.Duration.ofSeconds(7), AdbcParams.toConfig(params).connectTimeout());
    }

    @Test
    @DisplayName("a URI without an explicit port uses the native default")
    void uriDefaultPortApplied() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://myhost/mydb");

        assertEquals(9000, AdbcParams.toConfig(params).port());
    }

    @Test
    @DisplayName("an unknown URI query parameter raises INVALID_ARGUMENT (fail fast, not silently ignore)")
    void unknownUriQueryParamIsInvalidArgument() {
        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, "chnative://myhost:9001/mydb?bogusParam=1");

        AdbcException ex = assertThrows(AdbcException.class, () -> AdbcParams.toConfig(params));
        assertEquals(AdbcStatusCode.INVALID_ARGUMENT, ex.getStatus());
        assertNotNull(ex.getCause(), "the underlying config failure should be retained as the cause");
    }

    @Test
    @DisplayName("a boxed Long port is accepted via the Number arm")
    void longPortIsAccepted() {
        Map<String, Object> params = new HashMap<>();
        params.put(AdbcParams.PARAM_HOST, "localhost");
        params.put(AdbcParams.PARAM_PORT, 9001L);

        assertEquals(9001, AdbcParams.toConfig(params).port());
    }
}
