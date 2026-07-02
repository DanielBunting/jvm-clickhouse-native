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
        // Nanosecond precision (DateTime64(9)) is preserved: nine fractional digits.
        assertEquals("2026-05-30 13:45:07.123456789",
                QueryParameters.toText(Timestamp.valueOf("2026-05-30 13:45:07.123456789")));
        assertEquals("2026-05-30 13:45:07.123456789",
                QueryParameters.toText(Instant.parse("2026-05-30T13:45:07.123456789Z")));
        assertEquals("2026-05-30 13:45:07",
                QueryParameters.toText(LocalDateTime.of(2026, 5, 30, 13, 45, 7)));
    }

    /**
     * Date inputs render as the bare ISO date the server expects for a Date placeholder
     * (reference: DataTypeConverterTest#testDateToString — sql.Date literal rendering;
     * the reference's java.util.Date overload has no counterpart here).
     */
    @Test
    void sqlDateAndLocalDateRenderIsoDate() {
        assertEquals("2021-01-01",
                QueryParameters.toText(java.sql.Date.valueOf("2021-01-01")));
        assertEquals("2021-01-01",
                QueryParameters.toText(java.time.LocalDate.of(2021, 1, 1)));
    }

    /**
     * Composite values render as ClickHouse literal text (reference:
     * ClickHouseValuesTest#testConvertToSqlExpression / ClickHouseParameterizedQueryTest
     * #testApplyObjects). Grammar verified against the live server: elements follow
     * SQL-literal rules — strings/temporals quoted with backslash escapes, bare NULL,
     * nested composites recursive — unlike top-level scalars, which travel unquoted.
     */
    @Test
    void listRendersClickHouseArrayText() {
        assertEquals("[1,2,3]", QueryParameters.toText(java.util.List.of(1, 2, 3)));
        assertEquals("['a','b']", QueryParameters.toText(java.util.List.of("a", "b")));
        assertEquals("['a\\'b','c\\\\d']",
                QueryParameters.toText(java.util.List.of("a'b", "c\\d")),
                "quotes and backslashes are backslash-escaped inside elements");
        assertEquals("[[1,2],[3]]", QueryParameters.toText(
                java.util.List.of(java.util.List.of(1, 2), java.util.List.of(3))));
        assertEquals("[1,NULL,3]", QueryParameters.toText(
                java.util.Arrays.asList(1, null, 3)), "null elements render as bare NULL");
        assertEquals("['2021-01-01']", QueryParameters.toText(
                java.util.List.of(java.time.LocalDate.of(2021, 1, 1))),
                "temporal elements are quoted, reusing the scalar rendering");
        assertEquals("[]", QueryParameters.toText(java.util.List.of()));
    }

    @Test
    void mapRendersClickHouseMapText() {
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("a", 1);
        m.put("b", java.util.List.of(2, 3));
        assertEquals("{'a':1,'b':[2,3]}", QueryParameters.toText(m));
        assertEquals("{}", QueryParameters.toText(java.util.Map.of()));
    }
}
