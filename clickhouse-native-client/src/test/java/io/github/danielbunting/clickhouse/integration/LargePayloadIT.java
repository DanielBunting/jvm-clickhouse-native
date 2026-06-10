package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import io.github.danielbunting.clickhouse.types.Column;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Large-payload / scale integration tests for the variable-width container and
 * string codecs against a real ClickHouse 25.6 server.
 *
 * <p>The motivating risk: {@code Array(T)} and {@code Map(K, V)} decode reads
 * cumulative {@code UInt64} offsets and materialises element counts via
 * {@code Math.toIntExact(...)}; the String codec reads a VarUInt length per value.
 * The existing per-type tests use tiny arrays/maps and short strings inside a
 * single block, so they cannot exercise:
 * <ul>
 *   <li><b>offset/length materialisation</b> for large cumulative totals
 *       (int-narrowing or buffer-growth bugs),</li>
 *   <li><b>big variable-width payloads</b> (very long strings, very large arrays)
 *       crossing native block boundaries (~65k rows per block), and</li>
 *   <li><b>many entries per Map</b> and deeply/largely populated arrays.</li>
 * </ul>
 *
 * <p>All data is generated server-side via {@code numbers(N)} +
 * {@code range}/{@code mapFromArrays}/{@code repeat} so no giant Java literals are
 * shipped. Multi-block tests assert more than one non-empty block is observed.
 * For the huge single-row probes (a 1,000,000-element array; a &ge;1 MiB string)
 * only counts, lengths, and boundary elements are asserted — never every element.
 *
 * <p><b>Compression note.</b> These offset/length-at-scale tests connect with
 * {@link CompressionMethod#NONE} to isolate the codec logic from the transport. The
 * default ({@link CompressionMethod#LZ4}) path over multi-frame compressed blocks (a
 * block whose serialised section exceeds the per-frame ~1 MiB limit, e.g. a full
 * ~65k-row block of repeated strings or a large array) is covered separately by
 * {@link #lz4MultiFrameCompressedBlocksRoundTrip()}. Decoupling the codec
 * coverage from compression keeps the green tests honest: they prove the
 * container/string decode holds under realistic volume; the disabled test pins the
 * separate compression defect.
 *
 * <p>Run with:
 * {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*LargePayloadIT"}
 */
@Tag("integration")
class LargePayloadIT extends TypeRoundTripBase {

    /** A config with compression disabled (see the class-level compression note). */
    private ClickHouseConfig uncompressedConfig() {
        return ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .compression(CompressionMethod.NONE)
                .build();
    }

    /**
     * {@code Array(UInt64)} at volume: {@code range(number % 1000)} over
     * {@code numbers(20000)} (arrays of length {@code id % 1000}, ~10M total
     * elements) plus a deliberate single-row probe of {@code range(1000000)}.
     *
     * <p>{@code range(n)} yields {@code [0 .. n-1]}. Asserts every row's array
     * length ({@code id % 1000}) and the total element count across all rows;
     * spot-checks boundary/interior elements and a few full arrays; and for the
     * 1M-element probe asserts size + first/last elements only (the
     * offset/materialisation-at-scale probe — a single {@code UInt64} end-offset
     * of 1e6 and a 1e6-length {@code toIntExact} + {@code ArrayList} allocation).
     */
    @Test
    void largeArraysRoundTrip() {
        long rowCount = 20_000L;
        try (ClickHouseConnection conn = ClickHouseConnection.open(uncompressedConfig())) {
            long seen = 0L;
            long totalElements = 0L;
            long expectedTotalElements = 0L;
            for (long n = 0; n < rowCount; n++) {
                expectedTotalElements += n % 1000;
            }

            try (QueryResult result = conn.query(
                    "SELECT number AS id, range(number % 1000) AS a"
                            + " FROM numbers(" + rowCount + ") ORDER BY id")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    Column idCol = block.column(0);
                    Column arrCol = block.column(1);
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        long id = ((Number) idCol.value(r)).longValue();
                        List<?> arr = assertInstanceOf(List.class, arrCol.value(r),
                                "row id=" + id + ": Array cell must decode to a List");
                        int expectedLen = (int) (id % 1000);
                        assertEquals(expectedLen, arr.size(),
                                "row id=" + id + ": array length (range(id % 1000))");
                        if (expectedLen > 0) {
                            assertEquals(0L, ((Number) arr.get(0)).longValue(),
                                    "row id=" + id + ": first element of range");
                            assertEquals(expectedLen - 1L,
                                    ((Number) arr.get(expectedLen - 1)).longValue(),
                                    "row id=" + id + ": last element of range");
                            if (expectedLen > 2) {
                                int mid = expectedLen / 2;
                                assertEquals((long) mid, ((Number) arr.get(mid)).longValue(),
                                        "row id=" + id + ": mid element of range");
                            }
                        }
                        // A few exhaustive full-array checks (small rows, keeps runtime sane).
                        if (id == 1 || id == 5 || id == 17) {
                            for (int j = 0; j < expectedLen; j++) {
                                assertEquals((long) j, ((Number) arr.get(j)).longValue(),
                                        "row id=" + id + " full check element[" + j + "]");
                            }
                        }
                        totalElements += arr.size();
                        seen++;
                    }
                }
            }

            assertEquals(rowCount, seen, "decoded row count");
            assertEquals(expectedTotalElements, totalElements,
                    "total flattened element count across all rows");

            // Deliberate single-row probe: a 1,000,000-element array in one row.
            try (QueryResult probe = conn.query("SELECT range(1000000) AS a")) {
                List<Object[]> rows = materialize(probe);
                assertEquals(1, rows.size(), "1M-element probe: one row");
                List<?> big = assertInstanceOf(List.class, rows.get(0)[0],
                        "1M-element probe: cell must decode to a List");
                assertEquals(1_000_000, big.size(), "1M-element probe: array size");
                assertEquals(0L, ((Number) big.get(0)).longValue(),
                        "1M-element probe: first element");
                assertEquals(999_999L, ((Number) big.get(999_999)).longValue(),
                        "1M-element probe: last element");
            }
        }
    }

    /**
     * {@code Map(String, UInt64)} with many entries per row: keys
     * {@code toString(0..k-1)} and values {@code 0..k-1} where {@code k = number % 200}
     * over {@code numbers(20000)} (maps of up to 199 entries, including empty maps
     * where {@code number % 200 == 0}).
     *
     * <p>On the wire {@code Map} is {@code Array(Tuple(K, V))}: the same cumulative
     * offset section drives entry counts. Asserts each row's entry count and the
     * total entry count; spot-checks boundary key/value pairs and a couple of full
     * maps.
     */
    @Test
    void largeMapsRoundTrip() {
        long rowCount = 20_000L;
        try (ClickHouseConnection conn = ClickHouseConnection.open(uncompressedConfig())) {
            long seen = 0L;
            long emptyMaps = 0L;
            long totalEntries = 0L;
            long expectedTotalEntries = 0L;
            for (long n = 0; n < rowCount; n++) {
                expectedTotalEntries += n % 200;
            }

            try (QueryResult result = conn.query(
                    "SELECT number AS id,"
                            + " mapFromArrays("
                            + "   arrayMap(x -> toString(x), range(number % 200)),"
                            + "   range(number % 200)) AS m"
                            + " FROM numbers(" + rowCount + ") ORDER BY id")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    Column idCol = block.column(0);
                    Column mapCol = block.column(1);
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        long id = ((Number) idCol.value(r)).longValue();
                        Map<?, ?> m = assertInstanceOf(Map.class, mapCol.value(r),
                                "row id=" + id + ": Map cell must decode to a java.util.Map");
                        int expectedEntries = (int) (id % 200);
                        assertEquals(expectedEntries, m.size(),
                                "row id=" + id + ": map entry count (number % 200)");
                        if (expectedEntries == 0) {
                            emptyMaps++;
                        } else {
                            assertEquals(0L, ((Number) m.get("0")).longValue(),
                                    "row id=" + id + ": entry key \"0\"");
                            String lastKey = Integer.toString(expectedEntries - 1);
                            assertEquals(expectedEntries - 1L,
                                    ((Number) m.get(lastKey)).longValue(),
                                    "row id=" + id + ": entry key \"" + lastKey + "\"");
                        }
                        if (id == 1 || id == 7) {
                            for (int k = 0; k < expectedEntries; k++) {
                                assertEquals((long) k,
                                        ((Number) m.get(Integer.toString(k))).longValue(),
                                        "row id=" + id + " full map check key \"" + k + "\"");
                            }
                        }
                        totalEntries += m.size();
                        seen++;
                    }
                }
            }

            assertEquals(rowCount, seen, "decoded row count");
            assertTrue(emptyMaps > 0, "expected some empty maps (number % 200 == 0)");
            assertEquals(expectedTotalEntries, totalEntries,
                    "total map entry count across all rows");
        }
    }

    /**
     * A {@code String} column over a multi-block result with varying and large
     * values: {@code repeat(toString(number), 1 + number % 50)} over
     * {@code numbers(300000)} (lengths vary; the scan spans several native blocks),
     * plus a deliberate &ge;1 MiB single-row probe to exercise the multi-byte
     * VarUInt length path.
     *
     * <p>Asserts more than one non-empty block, every row's decoded length against
     * its closed-form expected length, the total row count, and content of a few
     * spot-checked rows. The &ge;1 MiB probe uses
     * {@code concat(repeat('x', 1000000), repeat('x', 100000))} = 1,100,000 bytes
     * (the server caps a single {@code repeat} at 1,000,000), asserting length +
     * boundary chars only.
     */
    @Test
    void largeAndMultibyteStringsMultiBlock() {
        long rowCount = 300_000L;
        try (ClickHouseConnection conn = ClickHouseConnection.open(uncompressedConfig())) {
            long seen = 0L;
            int nonEmptyBlocks = 0;

            try (QueryResult result = conn.query(
                    "SELECT number AS id,"
                            + " repeat(toString(number), 1 + number % 50) AS s"
                            + " FROM numbers(" + rowCount + ") ORDER BY id")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    nonEmptyBlocks++;
                    Column idCol = block.column(0);
                    Column sCol = block.column(1);
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        long id = ((Number) idCol.value(r)).longValue();
                        String s = assertInstanceOf(String.class, sCol.value(r),
                                "row id=" + id + ": String cell must decode to a String");
                        String token = Long.toString(id);
                        int reps = (int) (1 + id % 50);
                        assertEquals((long) token.length() * reps, s.length(),
                                "row id=" + id + ": repeated string length");
                        if (id == 0 || id == 49 || id == 123_456 || id == rowCount - 1) {
                            assertEquals(token.repeat(reps), s,
                                    "row id=" + id + ": repeated string content");
                        }
                        seen++;
                    }
                }
            }

            assertTrue(nonEmptyBlocks > 1,
                    "numbers(" + rowCount + ") String scan must span more than one block, saw "
                            + nonEmptyBlocks);
            assertEquals(rowCount, seen, "decoded row count");

            // Deliberate single-row probe: a >=1 MiB string (1,100,000 bytes) to
            // exercise the multi-byte VarUInt length path on the String codec.
            try (QueryResult probe = conn.query(
                    "SELECT concat(repeat('x', 1000000), repeat('x', 100000)) AS s")) {
                List<Object[]> rows = materialize(probe);
                assertEquals(1, rows.size(), ">=1 MiB string probe: one row");
                String big = assertInstanceOf(String.class, rows.get(0)[0],
                        ">=1 MiB string probe: cell must decode to a String");
                assertEquals(1_100_000, big.length(), ">=1 MiB string probe: length");
                assertTrue(big.length() >= 1024 * 1024,
                        ">=1 MiB string probe: must be at least 1 MiB");
                assertEquals('x', big.charAt(0), ">=1 MiB string probe: first char");
                assertEquals('x', big.charAt(1_099_999), ">=1 MiB string probe: last char");
            }
        }
    }

    /**
     * {@code Array(String)} at volume: {@code arrayMap(x -> toString(x), range(number % 100))}
     * over {@code numbers(300000)} — nested variable-width values inside arrays across
     * multiple native blocks. Exercises the recursive Array offsets section combined
     * with per-element VarUInt String lengths in the flattened value section.
     *
     * <p>Asserts more than one non-empty block, each row's array length, boundary
     * elements (string forms of {@code 0} and {@code len-1}), and the total element
     * count.
     */
    @Test
    void arrayOfStringMultiBlock() {
        long rowCount = 300_000L;
        try (ClickHouseConnection conn = ClickHouseConnection.open(uncompressedConfig())) {
            long seen = 0L;
            int nonEmptyBlocks = 0;
            long totalElements = 0L;
            long expectedTotalElements = 0L;
            for (long n = 0; n < rowCount; n++) {
                expectedTotalElements += n % 100;
            }

            try (QueryResult result = conn.query(
                    "SELECT number AS id,"
                            + " arrayMap(x -> toString(x), range(number % 100)) AS a"
                            + " FROM numbers(" + rowCount + ") ORDER BY id")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    nonEmptyBlocks++;
                    Column idCol = block.column(0);
                    Column arrCol = block.column(1);
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        long id = ((Number) idCol.value(r)).longValue();
                        List<?> arr = assertInstanceOf(List.class, arrCol.value(r),
                                "row id=" + id + ": Array(String) cell must decode to a List");
                        int expectedLen = (int) (id % 100);
                        assertEquals(expectedLen, arr.size(),
                                "row id=" + id + ": Array(String) length");
                        if (expectedLen > 0) {
                            assertEquals("0", arr.get(0),
                                    "row id=" + id + ": first string element");
                            assertEquals(Integer.toString(expectedLen - 1),
                                    arr.get(expectedLen - 1),
                                    "row id=" + id + ": last string element");
                        }
                        if (id == 3 || id == 42) {
                            for (int j = 0; j < expectedLen; j++) {
                                assertEquals(Integer.toString(j), arr.get(j),
                                        "row id=" + id + " full check element[" + j + "]");
                            }
                        }
                        totalElements += arr.size();
                        seen++;
                    }
                }
            }

            assertTrue(nonEmptyBlocks > 1,
                    "numbers(" + rowCount + ") Array(String) scan must span more than one block, saw "
                            + nonEmptyBlocks);
            assertEquals(rowCount, seen, "decoded row count");
            assertEquals(expectedTotalElements, totalElements,
                    "total Array(String) element count across all rows");
        }
    }

    /**
     * Regression guard for the multi-frame compressed-block fix. With the default
     * {@link CompressionMethod#LZ4} transport a single native data block can serialise to
     * more than one compressed frame (ClickHouse splits a block's bytes across consecutive
     * frames once they exceed the per-frame ~1 MiB limit). The read path now reassembles
     * those frames ({@code CompressedFrameInputStream}); previously it consumed one frame
     * per block read and desynced. This test forces a multi-frame block under LZ4 and
     * asserts every row decodes.
     *
     * <p><b>Precise repro</b> (all fail with LZ4, all PASS with
     * {@link CompressionMethod#NONE} — see the four enabled tests):
     * <ul>
     *   <li>{@code SELECT repeat(toString(number), 1 + number % 50) FROM numbers(65000)}
     *       — fails at the first ~65k-row block boundary
     *       ({@code EOFException: Unexpected end of stream: needed 172 bytes, got 134});
     *       the same query at {@code numbers(1000)} succeeds.</li>
     *   <li>{@code SELECT range(number % 1000) FROM numbers(100000)},
     *       {@code arrayMap(x -> toString(x), range(number % 100))}, and the
     *       {@code mapFromArrays(...)} map query — all
     *       {@code EOFException}/desync under LZ4.</li>
     *   <li>{@code SELECT range(1000000)} (single 1M-element row) —
     *       {@code EOFException} under LZ4, succeeds under NONE.</li>
     * </ul>
     *
     * <p>The defect is in the compressed-frame read path, independent of the
     * Array/Map/String offset/length codecs (which are proven correct at scale by the
     * enabled tests). It needs a production fix to reassemble multi-frame compressed
     * blocks; do not weaken or shrink the probes to dodge it.
     */
    @Test
    void lz4MultiFrameCompressedBlocksRoundTrip() {
        ClickHouseConfig lz4 = ClickHouseConfig.builder()
                .host(clickHouseHost())
                .port(clickHousePort())
                .compression(CompressionMethod.LZ4)
                .build();
        try (ClickHouseConnection conn = ClickHouseConnection.open(lz4)) {
            long seen = 0L;
            try (QueryResult result = conn.query(
                    "SELECT repeat(toString(number), 1 + number % 50) AS s"
                            + " FROM numbers(65000) ORDER BY number")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    Column sCol = block.column(0);
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        assertInstanceOf(String.class, sCol.value(r), "String cell");
                        seen++;
                    }
                }
            }
            assertEquals(65_000L, seen, "all rows decoded under LZ4");
        }
    }
}
