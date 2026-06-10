package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for per-connection default settings on {@link ClickHouseConfig} — the
 * builder {@code setting}/{@code settings} methods and the {@code settings.<name>=<value>}
 * URL query-parameter form.
 */
final class ClickHouseConfigSettingsTest {

    @Test
    void defaultSettingsAreEmpty() {
        ClickHouseConfig cfg = ClickHouseConfig.builder().host("h").build();
        assertTrue(cfg.settings().isEmpty());
    }

    @Test
    void builderSettingPreservesInsertionOrder() {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host("h")
                .setting("max_execution_time", "30")
                .setting("max_threads", "4")
                .build();
        assertEquals("[max_execution_time, max_threads]", cfg.settings().keySet().toString());
        assertEquals("30", cfg.settings().get("max_execution_time"));
        assertEquals("4", cfg.settings().get("max_threads"));
    }

    @Test
    void builderSettingsMapAddsAll() {
        Map<String, String> in = new LinkedHashMap<>();
        in.put("async_insert", "1");
        in.put("wait_for_async_insert", "0");
        ClickHouseConfig cfg = ClickHouseConfig.builder().host("h").settings(in).build();
        assertEquals(in, cfg.settings());
    }

    @Test
    void settingsAreImmutable() {
        ClickHouseConfig cfg = ClickHouseConfig.builder()
                .host("h").setting("a", "1").build();
        assertThrows(UnsupportedOperationException.class, () -> cfg.settings().put("b", "2"));
    }

    @Test
    void blankNameOrNullValueRejected() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.builder().setting(" ", "1"));
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.builder().setting("a", null));
    }

    @Test
    void urlSettingsParameterContributesSettings() {
        ClickHouseConfig cfg = ClickHouseConfig.fromUrl(
                "chnative://localhost/db?settings.max_execution_time=15&settings.async_insert=1");
        assertEquals("15", cfg.settings().get("max_execution_time"));
        assertEquals("1", cfg.settings().get("async_insert"));
    }

    @Test
    void unknownNonSettingsParameterStillRejected() {
        assertThrows(ClickHouseException.class,
                () -> ClickHouseConfig.fromUrl("chnative://localhost/db?bogus=1"));
    }
}
