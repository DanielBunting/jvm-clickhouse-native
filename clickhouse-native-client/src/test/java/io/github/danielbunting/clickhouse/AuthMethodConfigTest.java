package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for credential-driven {@link AuthMethod} selection and the
 * mutually-exclusive validation in {@link ClickHouseConfig.Builder}. No server.
 */
class AuthMethodConfigTest {

    @Test
    void defaultsToPasswordWithEmptyPassword() {
        ClickHouseConfig cfg = ClickHouseConfig.builder().build();
        assertEquals(AuthMethod.PASSWORD, cfg.authMethod());
        assertEquals("", cfg.password());
        assertNull(cfg.accessToken());
        assertNull(cfg.clientCertPath());
    }

    @Test
    void plainPasswordSelectsPassword() {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .username("alice")
                .password("secret")
                .build();
        assertEquals(AuthMethod.PASSWORD, cfg.authMethod());
    }

    @Test
    void accessTokenSelectsAccessToken() {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .accessToken("eyJhbGciOi.payload.sig")
                .build();
        assertEquals(AuthMethod.ACCESS_TOKEN, cfg.authMethod());
        assertEquals("eyJhbGciOi.payload.sig", cfg.accessToken());
    }

    @Test
    void clientCertWithKeySelectsCert() {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .clientCertPath("/etc/ssl/client.crt")
                .clientKeyPath("/etc/ssl/client.key")
                .build();
        assertEquals(AuthMethod.CERT, cfg.authMethod());
        assertEquals("/etc/ssl/client.crt", cfg.clientCertPath());
        assertEquals("/etc/ssl/client.key", cfg.clientKeyPath());
    }

    @Test
    void emptyAccessTokenDoesNotSelectToken() {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .accessToken("")
                .build();
        assertEquals(AuthMethod.PASSWORD, cfg.authMethod());
    }

    @Test
    void passwordPlusAccessTokenIsRejected() {
        ClickHouseConfig.Builder b = ClickHouseConfig.builder()
                .password("secret")
                .accessToken("tok");
        ConfigurationException ex = assertThrows(ConfigurationException.class, b::build);
        assertTrue(ex.getMessage().contains("Conflicting credentials"));
    }

    @Test
    void passwordPlusCertIsRejected() {
        ClickHouseConfig.Builder b = ClickHouseConfig.builder()
                .password("secret")
                .clientCertPath("/c.crt")
                .clientKeyPath("/c.key");
        assertThrows(ConfigurationException.class, b::build);
    }

    @Test
    void accessTokenPlusCertIsRejected() {
        ClickHouseConfig.Builder b = ClickHouseConfig.builder()
                .accessToken("tok")
                .clientCertPath("/c.crt")
                .clientKeyPath("/c.key");
        assertThrows(ConfigurationException.class, b::build);
    }

    @Test
    void certWithoutKeyIsRejected() {
        ClickHouseConfig.Builder b = ClickHouseConfig.builder()
                .clientCertPath("/c.crt");
        ConfigurationException ex = assertThrows(ConfigurationException.class, b::build);
        assertTrue(ex.getMessage().contains("clientKeyPath"));
    }

    @Test
    void keyWithoutCertIsRejected() {
        ClickHouseConfig.Builder b = ClickHouseConfig.builder()
                .clientKeyPath("/c.key");
        assertThrows(ConfigurationException.class, b::build);
    }
}
