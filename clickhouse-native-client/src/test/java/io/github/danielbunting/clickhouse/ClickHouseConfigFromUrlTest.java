package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClickHouseConfig#fromUrl(String)}.
 *
 * <p>Covers a full URL with all components, a minimal URL using defaults,
 * and a selection of malformed URLs that must throw {@link ClickHouseException}.
 */
final class ClickHouseConfigFromUrlTest {

    // -----------------------------------------------------------------------
    // Happy-path: full URL
    // -----------------------------------------------------------------------

    /**
     * A URL supplying every optional component — user, password, non-default port,
     * explicit database, and all supported query parameters — must be parsed into
     * the correct fields.
     */
    @Test
    void fullUrl_allComponentsParsedCorrectly() {
        String url = "chnative://alice:s3cr3t@db.example.com:9100/analytics"
                + "?compression=zstd&insertBatchSize=1024"
                + "&connectTimeout=5&socketTimeout=60";

        ClickHouseConfig cfg = ClickHouseConfig.fromUrl(url);

        assertEquals("db.example.com", cfg.host());
        assertEquals(9100, cfg.port());
        assertEquals("analytics", cfg.database());
        assertEquals("alice", cfg.username());
        assertEquals("s3cr3t", cfg.password());
        assertEquals(CompressionMethod.ZSTD, cfg.compression());
        assertEquals(1024, cfg.insertBatchSize());
        assertEquals(Duration.ofSeconds(5), cfg.connectTimeout());
        assertEquals(Duration.ofSeconds(60), cfg.socketTimeout());
    }

    // -----------------------------------------------------------------------
    // Happy-path: minimal URL (defaults kick in)
    // -----------------------------------------------------------------------

    /**
     * A minimal URL providing only the host should fall back to all builder
     * defaults: port 9000, database "default", username "default", empty
     * password, and LZ4 compression.
     */
    @Test
    void minimalUrl_defaultsApplied() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://myhost");

        assertEquals("myhost", cfg.host());
        assertEquals(9000, cfg.port());
        assertEquals("default", cfg.database());
        assertEquals("default", cfg.username());
        assertEquals("", cfg.password());
        assertEquals(CompressionMethod.LZ4, cfg.compression());
        assertEquals(65_536, cfg.insertBatchSize());
        assertEquals(Duration.ofSeconds(10), cfg.connectTimeout());
        assertEquals(Duration.ofSeconds(30), cfg.socketTimeout());
    }

    /**
     * A URL with host and explicit database but no query parameters should
     * use the database from the path and all other defaults.
     */
    @Test
    void urlWithDatabase_onlyDatabaseOverridden() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://clickhouse-node/mydb");

        assertEquals("clickhouse-node", cfg.host());
        assertEquals("mydb", cfg.database());
        assertEquals(9000, cfg.port());
        assertEquals("default", cfg.username());
    }

    /**
     * A URL with {@code compression=none} should produce {@link CompressionMethod#NONE}.
     */
    @Test
    void compressionNone_parsed() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host/db?compression=none");
        assertEquals(CompressionMethod.NONE, cfg.compression());
    }

    /**
     * A URL with {@code compression=lz4} (mixed case) should be parsed
     * case-insensitively.
     */
    @Test
    void compressionLz4CaseInsensitive_parsed() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host?compression=LZ4");
        assertEquals(CompressionMethod.LZ4, cfg.compression());
    }

    /**
     * A URL with a user but no password should set the username and leave
     * the password at the default empty string.
     */
    @Test
    void userWithoutPassword_parsedCorrectly() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://bob@host:9000/db");

        assertEquals("bob", cfg.username());
        assertEquals("", cfg.password());
    }

    // -----------------------------------------------------------------------
    // Malformed URL — must throw ClickHouseException
    // -----------------------------------------------------------------------

    /**
     * A null URL must throw immediately.
     */
    @Test
    void nullUrl_throws() {
        assertThrows(ClickHouseException.class, () -> ClickHouseConfig.fromUrl(null));
    }

    /**
     * A URL with the wrong scheme must throw.
     */
    @Test
    void wrongScheme_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("jdbc:clickhouse://host:9000/db"));
    }

    /**
     * A URL with no scheme at all must throw.
     */
    @Test
    void missingScheme_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("host:9000/db"));
    }

    /**
     * An unknown compression method must throw.
     */
    @Test
    void unknownCompressionMethod_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?compression=brotli"));
    }

    /**
     * A non-integer value for {@code insertBatchSize} must throw.
     */
    @Test
    void nonIntegerInsertBatchSize_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?insertBatchSize=big"));
    }

    /**
     * A zero value for {@code insertBatchSize} must throw (must be positive).
     */
    @Test
    void zeroInsertBatchSize_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?insertBatchSize=0"));
    }

    /**
     * A negative value for {@code connectTimeout} must throw.
     */
    @Test
    void negativeConnectTimeout_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?connectTimeout=-5"));
    }

    /**
     * An unknown query parameter must throw.
     */
    @Test
    void unknownQueryParameter_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?unknownParam=value"));
    }

    // -----------------------------------------------------------------------
    // TLS
    // -----------------------------------------------------------------------

    /** TLS is off by default and verification is on. */
    @Test
    void tlsDefaults() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host");
        assertFalse(cfg.tls());
        assertTrue(cfg.verifyHostname());
        assertFalse(cfg.insecureSkipVerify());
        assertEquals(null, cfg.trustStorePath());
        assertEquals(null, cfg.keyStorePath());
    }

    /** {@code ssl=true} enables TLS. */
    @Test
    void sslTrue_enablesTls() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host:9440?ssl=true");
        assertTrue(cfg.tls());
        assertEquals(9440, cfg.port());
    }

    /** {@code secure=true} is an alias for {@code ssl=true}. */
    @Test
    void secureTrue_enablesTls() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host?secure=true");
        assertTrue(cfg.tls());
    }

    /** {@code ssl=false} keeps TLS off. */
    @Test
    void sslFalse_keepsTlsOff() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host?ssl=false");
        assertFalse(cfg.tls());
    }

    /** A non-boolean {@code ssl} value must throw. */
    @Test
    void sslInvalidValue_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?ssl=maybe"));
    }

    /** {@code sslmode=strict} enables TLS without insecure-skip-verify. */
    @Test
    void sslmodeStrict_enablesTlsSecurely() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host?sslmode=strict");
        assertTrue(cfg.tls());
        assertFalse(cfg.insecureSkipVerify());
    }

    /** {@code sslmode=none} keeps TLS off. */
    @Test
    void sslmodeNone_disablesTls() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host?sslmode=none");
        assertFalse(cfg.tls());
    }

    /** {@code sslmode=insecure} enables TLS and the dev-only skip-verify escape hatch. */
    @Test
    void sslmodeInsecure_enablesSkipVerify() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host?sslmode=insecure");
        assertTrue(cfg.tls());
        assertTrue(cfg.insecureSkipVerify());
    }

    /** An unknown {@code sslmode} value must throw. */
    @Test
    void sslmodeUnknown_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host?sslmode=bogus"));
    }
}
