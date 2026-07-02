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
    // Credentials: colons and percent-encoding
    // (reference: v1 ClickHouseJdbcUrlParserTest#testParseCredentials)
    // -----------------------------------------------------------------------

    /**
     * Only the first colon separates user from password, so a password may itself
     * contain colons.
     */
    @Test
    void passwordContainingColon_splitAtFirstColon() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://user:a:passwd@foo.ch/test");

        assertEquals("user", cfg.username());
        assertEquals("a:passwd", cfg.password());
        assertEquals("foo.ch", cfg.host());
        assertEquals("test", cfg.database());
    }

    /**
     * Percent-encoded characters in the password are decoded ({@code %40} → {@code @},
     * {@code %3A} → {@code :}).
     */
    @Test
    void percentEncodedPassword_decoded() {
        ClickHouseConfig cfg =
                ClickHouseConfig.fromUrl("chnative://alice:let%40me%3Ain@host:9000/db");

        assertEquals("alice", cfg.username());
        assertEquals("let@me:in", cfg.password());
    }

    /**
     * A percent-encoded colon in the USERNAME ({@code let%3Ame}) decodes into the
     * username — RFC 3986 reserves only the <em>literal</em> colon as the
     * user/password separator: {@code fromUrl} splits the RAW userinfo at the first
     * literal {@code ':'} and percent-decodes each part separately (was knownBug 22;
     * reference: v1 ClickHouseJdbcUrlParserTest#testParseCredentials).
     */
    @Test
    void percentEncodedColonInUsername_decodedIntoUsername() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://let%3Ame:pw@host/db");

        assertEquals("let:me", cfg.username());
        assertEquals("pw", cfg.password());
    }

    /**
     * A percent-encoded {@code @} in the username decodes correctly, because the
     * authority is split on the last literal {@code @}.
     */
    @Test
    void percentEncodedAtInUsername_decoded() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://a%40corp:pw@host/db");

        assertEquals("a@corp", cfg.username());
        assertEquals("pw", cfg.password());
        assertEquals("host", cfg.host());
    }

    /**
     * A literal {@code '+'} in the URL userinfo survives parsing verbatim. RFC 3986
     * percent-decoding only decodes {@code %XX} escapes; {@code '+'} is an ordinary
     * character in a URL userinfo ({@code '+'}-as-space is FORM encoding,
     * {@code application/x-www-form-urlencoded}, which does not apply here), so
     * {@code fromUrl("chnative://user:p+ssword@host:9000")} yields password
     * {@code "p+ssword"}, and the same holds for a {@code '+'} in the username.
     * (was knownBug 35)
     */
    @Test
    void literalPlusInUserinfoPreserved() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://user:p+ssword@host:9000");
        assertEquals("user", cfg.username());
        assertEquals("p+ssword", cfg.password(),
                "a literal '+' in the password is data, not a form-encoded space");

        ClickHouseConfig userPlus = ClickHouseConfig.fromUrl("chnative://us+er:pw@host:9000/db");
        assertEquals("us+er", userPlus.username(),
                "a literal '+' in the username is data, not a form-encoded space");
        assertEquals("pw", userPlus.password());
    }

    // -----------------------------------------------------------------------
    // Underscore hostnames
    // (reference: client-v2 ClientBuilderTest#testAddEndpointToleratesUnderscoreHostname,
    //  HttpEndpointTest#testUnderscoreHostIsAcceptedInUri /
    //  #testUrlEndpointPreservesUnderscoreHost, HttpTransportTests#testHostnameWithUnderscore
    //  — java.net.URI.getHost() returns null for hosts containing '_', which broke the
    //  reference client until it stopped relying on getHost())
    // -----------------------------------------------------------------------

    /**
     * A hostname containing underscores (common for Docker/K8s service names) must parse
     * correctly even though {@code java.net.URI.getHost()} returns {@code null} for it.
     * This client extracts the host list with its own authority parser, so the host,
     * port, database, and query parameters all survive.
     */
    @Test
    void underscoreHostname_parsed() {
        ClickHouseConfig cfg =
                ClickHouseConfig.fromUrl("chnative://host_with_underscore:9000/db?compression=zstd");

        assertEquals("host_with_underscore", cfg.host());
        assertEquals(9000, cfg.port());
        assertEquals("db", cfg.database());
        assertEquals(CompressionMethod.ZSTD, cfg.compression());
    }

    /** An underscore hostname without an explicit port falls back to the default 9000. */
    @Test
    void underscoreHostnameWithoutPort_defaultPort() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://my_host/analytics");

        assertEquals("my_host", cfg.host());
        assertEquals(9000, cfg.port());
        assertEquals("analytics", cfg.database());
    }

    /**
     * Credentials in a URL whose host contains an underscore are parsed correctly.
     * {@code java.net.URI} refuses to server-parse such an authority (falls back to a
     * REGISTRY-based authority and returns {@code null} from {@code getUserInfo()} —
     * the reference client had this exact class of bug, client-v2
     * ClientBuilderTest#testAddEndpointToleratesUnderscoreHostname), so
     * {@code fromUrl} extracts the userinfo from the raw authority itself: the text
     * before the last literal {@code '@'}, split at the first literal {@code ':'},
     * each part percent-decoded (was knownBug 22, fixed together with
     * {@link #percentEncodedColonInUsername_decodedIntoUsername}).
     */
    @Test
    void underscoreHostWithCredentials_preservesCredentials() {
        ClickHouseConfig cfg =
                ClickHouseConfig.fromUrl("chnative://alice:s3cr3t@host_with_underscore:9000/db");

        assertEquals("host_with_underscore", cfg.host());
        assertEquals(9000, cfg.port());
        assertEquals("db", cfg.database());
        assertEquals("alice", cfg.username());
        assertEquals("s3cr3t", cfg.password());
    }

    // -----------------------------------------------------------------------
    // Database path encoding
    // (reference: jdbc-v2 JdbcConfigurationTest#testParseURLValid)
    // -----------------------------------------------------------------------

    /** A percent-encoded database path segment is decoded. */
    @Test
    void percentEncodedDatabase_decoded() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://host/my%20db");
        assertEquals("my db", cfg.database());
    }

    // -----------------------------------------------------------------------
    // IPv6 hosts (reference: jdbc-v2 JdbcConfigurationTest#testParseURLValid)
    // -----------------------------------------------------------------------

    /** A bracketed IPv6 literal with an explicit port. */
    @Test
    void ipv6HostWithPort_parsed() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://[::1]:9100/db");

        assertEquals("::1", cfg.host());
        assertEquals(9100, cfg.port());
        assertEquals("db", cfg.database());
    }

    /** A bracketed IPv6 literal without a port defaults to 9000. */
    @Test
    void ipv6HostWithoutPort_defaultPort() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl("chnative://[2001:db8::1]/db");

        assertEquals("2001:db8::1", cfg.host());
        assertEquals(9000, cfg.port());
    }

    /** An unterminated IPv6 literal must throw. */
    @Test
    void ipv6Unterminated_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://[::1:9100/db"));
    }

    /** Trailing junk after the closing bracket that is not a {@code :port} must throw. */
    @Test
    void ipv6TrailingJunk_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://[::1]x/db"));
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

    /** A URL with an empty authority (no host) must throw. */
    @Test
    void missingHost_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative:///db"));
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://"));
    }

    /** A non-numeric port must throw. */
    @Test
    void nonNumericPort_throws() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://host:port/db"));
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

    /**
     * Full {@code sslmode} alias matrix (reference: jdbc-v2
     * JdbcConfigurationTest#testSSLModeProperty): every documented alias maps to the
     * expected TLS/verification combination, case-insensitively.
     */
    @Test
    void sslmodeAliases_mapToExpectedTlsFlags() {
        // TLS off
        for (String mode : new String[] {"none", "disable", "DISABLE"}) {
            ClickHouseConfig cfg =
                    ClickHouseConfig.fromUrl("chnative://host?sslmode=" + mode);
            assertFalse(cfg.tls(), "sslmode=" + mode);
        }
        // TLS on, verification intact (incl. mixed case — the parser lowercases first)
        for (String mode : new String[] {"strict", "verify-full", "require", "true", "STRICT", "sTrIcT"}) {
            ClickHouseConfig cfg =
                    ClickHouseConfig.fromUrl("chnative://host?sslmode=" + mode);
            assertTrue(cfg.tls(), "sslmode=" + mode);
            assertFalse(cfg.insecureSkipVerify(), "sslmode=" + mode);
        }
        // TLS on, dev-only trust-all
        for (String mode : new String[] {"none-verify", "insecure"}) {
            ClickHouseConfig cfg =
                    ClickHouseConfig.fromUrl("chnative://host?sslmode=" + mode);
            assertTrue(cfg.tls(), "sslmode=" + mode);
            assertTrue(cfg.insecureSkipVerify(), "sslmode=" + mode);
        }
    }

    /**
     * A query-parameter VALUE containing "/" survives parsing intact (reference:
     * ClickHouseNodeTest/ClickHouseNodesTest#testQueryWithSlash — a slash in a param
     * value must not be mistaken for a path separator), on both single- and
     * multi-endpoint URLs.
     */
    @Test
    void paramValueContainingSlash_parsedIntact() {
        ClickHouseConfig single = ClickHouseConfig.fromUrl(
                "chnative://h1:9000/db?settings.log_comment=a/b/c");
        assertEquals("a/b/c", single.settings().get("log_comment"),
                "slash inside a query-parameter value is data, not a path");

        ClickHouseConfig multi = ClickHouseConfig.fromUrl(
                "chnative://h1:9000,h2:9001/db?settings.log_comment=x/y");
        assertEquals("x/y", multi.settings().get("log_comment"),
                "the same holds across a comma-separated endpoint list");
        assertEquals(2, multi.endpoints().size(), "both endpoints still parsed");
    }

    /**
     * Builder defaults (reference: client-v2 ClientTests#testDefaultSettings — every
     * documented config default is pinned). A bare builder must produce the documented
     * ClickHouse-sensible defaults.
     */
    @Test
    void builderDefaults_arePinned() {
        ClickHouseConfig cfg = ClickHouseConfig.builder().build();

        assertEquals(java.util.List.of(new Endpoint("localhost", 9000)), cfg.endpoints());
        assertEquals("default", cfg.database());
        assertEquals("default", cfg.username());
        assertEquals("", cfg.password());
        assertEquals(CompressionMethod.LZ4, cfg.compression());
        assertEquals(Duration.ofSeconds(10), cfg.connectTimeout());
        assertEquals(Duration.ofSeconds(30), cfg.socketTimeout());
        assertEquals(Duration.ZERO, cfg.queryTimeout());
        assertFalse(cfg.tls(), "TLS is opt-in");
        assertTrue(cfg.verifyHostname(), "hostname verification on by default");
        assertFalse(cfg.insecureSkipVerify(), "trust-all is opt-in");
        assertEquals(65_536, cfg.insertBatchSize());
        assertTrue(cfg.settings().isEmpty(), "no default server settings");
        assertEquals(LoadBalancingPolicy.FIRST_ALIVE, cfg.loadBalancingPolicy());
    }

    /**
     * A blank/null host with no endpoint list fails the build with a clear message
     * (reference: client-v2 ClientTests#testInvalidConfig — missing endpoint rejected).
     */
    @Test
    void builderWithNullHostAndNoEndpoints_throws() {
        Exception e = assertThrows(Exception.class,
                () -> ClickHouseConfig.builder().host(null).build());
        assertTrue(e.getMessage().contains("host must not be null or blank"),
                "message names the missing endpoint host: " + e.getMessage());
    }
}
