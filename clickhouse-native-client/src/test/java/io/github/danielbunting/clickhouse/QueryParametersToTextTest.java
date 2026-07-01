package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link QueryParameters#toText}, the value → ClickHouse textual form used
 * for server-side query parameters. Focuses on the InetAddress and fractional date-time
 * branches (sub-second precision preserved only when non-zero).
 */
class QueryParametersToTextTest {

    @Test
    void nullBecomesNull() {
        assertNull(QueryParameters.toText(null));
    }

    @Test
    void booleanAndDecimal() {
        assertEquals("true", QueryParameters.toText(Boolean.TRUE));
        assertEquals("false", QueryParameters.toText(Boolean.FALSE));
        assertEquals("12.50", QueryParameters.toText(new BigDecimal("12.50")));
    }

    @Test
    void inetAddressUsesCanonicalHostAddress() throws Exception {
        InetAddress v6 = InetAddress.getByName("2001:db8::1");
        assertEquals(v6.getHostAddress(), QueryParameters.toText(v6));
        InetAddress v4 = InetAddress.getByName("192.168.0.1");
        assertEquals("192.168.0.1", QueryParameters.toText(v4));
    }

    @Test
    void dateTimeKeepsFractionOnlyWhenPresent() {
        assertEquals("2026-05-30 13:45:07",
                QueryParameters.toText(Timestamp.valueOf("2026-05-30 13:45:07")));
        assertEquals("2026-05-30 13:45:07.123456",
                QueryParameters.toText(Timestamp.valueOf("2026-05-30 13:45:07.123456")));
        assertEquals("2026-05-30 13:45:07.123456",
                QueryParameters.toText(Instant.parse("2026-05-30T13:45:07.123456Z")));
        assertEquals("2026-05-30 13:45:07",
                QueryParameters.toText(LocalDateTime.of(2026, 5, 30, 13, 45, 7)));
    }
}
