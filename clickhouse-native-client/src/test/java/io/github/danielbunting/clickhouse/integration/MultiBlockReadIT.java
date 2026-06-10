package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import io.github.danielbunting.clickhouse.types.Column;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates block-boundary integrity on the read path. A {@code numbers(2_000_000)} scan
 * is returned by the server as multiple data blocks; this test iterates
 * {@link QueryResult#blocks()}, asserts more than one non-empty block is seen, and checks
 * that summing every {@code n} across all blocks yields the exact closed-form sum and that
 * the total row count matches. A block-boundary bug (dropped/duplicated rows, mis-sliced
 * value section across blocks) would corrupt either the count or the sum.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class MultiBlockReadIT extends TypeRoundTripBase {

    @Test
    void multiBlockSumAndCountAreExact() {
        long rowCount = 2_000_000L;
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            long sum = 0L;
            long seen = 0L;
            int nonEmptyBlocks = 0;

            try (QueryResult result =
                         conn.query("SELECT number AS n FROM numbers(" + rowCount + ")")) {
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    nonEmptyBlocks++;
                    Column col = block.column(0);
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        sum += ((Number) col.value(r)).longValue();
                        seen++;
                    }
                }
            }

            assertTrue(nonEmptyBlocks > 1,
                    "numbers(" + rowCount + ") must span more than one server block, saw "
                            + nonEmptyBlocks);
            assertEquals(rowCount, seen, "total decoded row count");
            assertEquals(rowCount * (rowCount - 1) / 2, sum,
                    "sum of 0.." + (rowCount - 1) + " across all blocks");
        }
    }
}
