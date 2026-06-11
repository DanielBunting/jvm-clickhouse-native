package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Cross-client timezone and DST coverage for {@code DateTime('<zone>')} and
 * {@code DateTime64(3, '<zone>')}. A ClickHouse column timezone is display
 * metadata — storage is absolute epoch ticks — so every test anchors absolute
 * {@link Instant}s and requires both clients to round-trip them unchanged
 * regardless of the column zone:
 * <ul>
 *   <li>spring-forward gaps (the skipped local hour),</li>
 *   <li>fall-back overlaps — two <em>distinct</em> instants sharing the same
 *       local wall-clock time; any layer that converts through local time
 *       without tracking the offset collapses them, which the row-level
 *       asserts catch,</li>
 *   <li>non-whole-hour offsets (+05:30, +05:45) and a 30-minute DST shift
 *       (Lord Howe Island),</li>
 *   <li>extreme offsets where the local calendar date diverges from UTC by
 *       up to +14 hours.</li>
 * </ul>
 *
 * <p>Direction A binds {@code ZonedDateTime.ofInstant(instant, columnZone)}
 * through the official client; Direction B bulk-inserts {@link Instant}
 * record fields through the native client; official reads use the typed
 * {@code OffsetDateTime} accessor (untyped reads are session-tz ambiguous).
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientTimezoneDstIT extends CrossClientRoundTripBase {

    /** Row shape shared by all tests: one DateTime + one DateTime64(3). */
    record Row(int id, Instant dt, Instant dt64) {}

    /**
     * Runs the full bidirectional round-trip for one column zone and a list
     * of anchored instants (one row per instant, same value in the DateTime
     * and DateTime64(3) columns — values must be whole-second for DateTime;
     * pass a distinct sub-second instant via {@code dt64Overrides} where
     * wanted, keyed by row index).
     */
    private void roundTrip(String prefix, String zone, List<Instant> instants,
            List<Instant> dt64Overrides) {
        roundTrip(prefix, zone, instants, dt64Overrides, false);
    }

    /**
     * @param ambiguousWallClock set for fall-back overlap tests. KNOWN BUG in
     *        clickhouse-jdbc 0.9.0: reading a {@code DateTime} column converts
     *        through the local wall clock, so the two distinct instants of a
     *        fall-back overlap collapse (typed {@code OffsetDateTime} reads
     *        resolve to the earlier offset, untyped reads to the later one).
     *        {@code DateTime64} typed reads are instant-based and correct, and
     *        a server-side {@code toUnixTimestamp(dt)} probe confirms storage
     *        is correct — so for these tests the official leg reads the
     *        DateTime column as epoch seconds instead.
     */
    private void roundTrip(String prefix, String zone, List<Instant> instants,
            List<Instant> dt64Overrides, boolean ambiguousWallClock) {
        ZoneId zoneId = ZoneId.of(zone);
        List<Object[]> expected = new ArrayList<>();
        List<Object[]> officialRows = new ArrayList<>();
        List<Row> records = new ArrayList<>();
        for (int i = 0; i < instants.size(); i++) {
            Instant dt = instants.get(i);
            Instant dt64 = dt64Overrides != null && dt64Overrides.get(i) != null
                    ? dt64Overrides.get(i) : dt;
            expected.add(row((long) (i + 1), dt, dt64));
            officialRows.add(row((long) (i + 1),
                    ZonedDateTime.ofInstant(dt, zoneId),
                    ZonedDateTime.ofInstant(dt64, zoneId)));
            records.add(new Row(i + 1, dt, dt64));
        }

        withTable(prefix, (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id   UInt32,"
                    + "  dt   DateTime('" + zone + "'),"
                    + "  dt64 DateTime64(3, '" + zone + "')"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, dt, dt64", officialRows);
            assertRowsMatch(prefix + " native decode", expected,
                    decode(conn, "SELECT id, dt, dt64 FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            // Native decode re-checks the bulk-inserted rows before the
            // official leg, so a failure pinpoints the encode side.
            assertRowsMatch(prefix + " native decode (after encode)", expected,
                    decode(conn, "SELECT id, dt, dt64 FROM " + table + " ORDER BY id"));
            if (ambiguousWallClock) {
                List<Object[]> officialExpected = new ArrayList<>();
                for (Object[] e : expected) {
                    officialExpected.add(row(e[0], ((Instant) e[1]).getEpochSecond(), e[2]));
                }
                assertRowsMatch(prefix + " official read (epoch workaround)", officialExpected,
                        officialSelect("SELECT id, toUnixTimestamp(dt), dt64 FROM " + table
                                        + " ORDER BY id",
                                null, null, OffsetDateTime.class));
            } else {
                assertRowsMatch(prefix + " official read", expected,
                        officialSelect("SELECT id, dt, dt64 FROM " + table + " ORDER BY id",
                                null, OffsetDateTime.class, OffsetDateTime.class));
            }
        });
    }

    /**
     * Europe/London spring-forward, 2024-03-31: at 01:00Z local clocks jump
     * 01:00 → 02:00, so local times 01:00–01:59 do not exist that night.
     */
    @Test
    void springForwardGapLondon() {
        roundTrip("xc_dst_gap", "Europe/London",
                List.of(
                        Instant.parse("2024-03-31T00:59:59Z"),
                        Instant.parse("2024-03-31T01:00:00Z"),
                        Instant.parse("2024-03-31T01:30:00Z")),
                java.util.Arrays.asList(
                        Instant.parse("2024-03-31T00:59:59.500Z"),
                        null,
                        null));
    }

    /**
     * Europe/London fall-back, 2024-10-27: at 01:00Z clocks fall 02:00 →
     * 01:00. The first two rows are DISTINCT instants with the SAME local
     * wall clock (01:30 BST vs 01:30 GMT) — if any layer converts through
     * local time without the offset, they collapse and the asserts fail.
     */
    @Test
    void fallBackOverlapLondon() {
        roundTrip("xc_dst_overlap", "Europe/London",
                List.of(
                        Instant.parse("2024-10-27T00:30:00Z"),
                        Instant.parse("2024-10-27T01:30:00Z"),
                        Instant.parse("2024-10-27T00:59:59Z")),
                java.util.Arrays.asList(
                        null,
                        null,
                        Instant.parse("2024-10-27T00:59:59.999Z")),
                true);
    }

    /**
     * Asia/Kolkata (+05:30) and Asia/Kathmandu (+05:45): non-whole-hour
     * offsets, instants straddling each zone's local midnight.
     */
    @Test
    void oddOffsetsIndiaNepal() {
        // One table per zone, same instants: last second of the Nepali day,
        // Nepal's midnight crossing, Kolkata's midnight crossing.
        List<Instant> instants = List.of(
                Instant.parse("2024-06-15T18:14:59Z"),
                Instant.parse("2024-06-15T18:15:00Z"),
                Instant.parse("2024-06-15T18:30:00Z"));
        roundTrip("xc_tz_kolkata", "Asia/Kolkata", instants, null);
        roundTrip("xc_tz_kathmandu", "Asia/Kathmandu", instants, null);
    }

    /**
     * Southern-hemisphere DST (Australia/Sydney fall-back 2024-04-07, both
     * rows local 02:30) and Australia/Lord_Howe, whose DST shift is only 30
     * minutes (+10:30 ↔ +11:00, spring-forward 2024-10-06 at 15:30Z).
     */
    @Test
    void southernHemisphereAndHalfHourDst() {
        roundTrip("xc_dst_sydney", "Australia/Sydney",
                List.of(
                        Instant.parse("2024-04-06T15:30:00Z"),
                        Instant.parse("2024-04-06T16:30:00Z")),
                null, true);
        roundTrip("xc_dst_lordhowe", "Australia/Lord_Howe",
                List.of(
                        Instant.parse("2024-10-05T15:29:59Z"),
                        Instant.parse("2024-10-05T15:30:00Z")),
                null);
    }

    /**
     * Extreme offsets: Pacific/Kiritimati (+14, no DST) and America/Anchorage
     * (-09/-08). The instants land on different local calendar dates in the
     * two zones (including across a New Year) — the absolute instants must
     * still round-trip unchanged.
     */
    @Test
    void dateBoundaryExtremeOffsets() {
        List<Instant> instants = List.of(
                Instant.parse("2024-07-04T09:59:59Z"),
                Instant.parse("2024-07-04T10:00:00Z"),
                Instant.parse("2024-12-31T10:00:00Z"));
        roundTrip("xc_tz_kiritimati", "Pacific/Kiritimati", instants, null);
        roundTrip("xc_tz_anchorage", "America/Anchorage", instants, null);
    }
}
