package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Round-trips the ClickHouse wide-integer types ({@code Int128}, {@code Int256},
 * {@code UInt128}, {@code UInt256}) through a real server in both directions.
 *
 * <p>For each type:
 * <ul>
 *   <li><b>DECODE</b>: {@code INSERT ... VALUES} with decimal literals (server
 *       encodes), then {@code decode(...)} reads the column back and we assert the
 *       boxed {@link BigInteger} equals the expected value.</li>
 *   <li><b>ENCODE</b>: bulk-insert a {@code record R(int id, BigInteger v)} via
 *       {@code createBulkInserter} (client encodes), then decode back and assert.</li>
 *   <li><b>MAPPED-READ</b>: {@code query(sql, R.class)} returns the same records.</li>
 * </ul>
 *
 * <p>Edge values per type: 0, 1, -1 (signed only), type min/max, and a large mid
 * value. All asserted via {@link BigInteger#equals}.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*WideIntTypesIT"}
 */
@Tag("integration")
class WideIntTypesIT extends TypeRoundTripBase {

    /** Row carrying a single wide-integer value keyed by id. */
    record R(int id, BigInteger v) {}

    private static final BigInteger TWO = BigInteger.valueOf(2);

    // Type bounds.
    private static final BigInteger INT128_MIN = TWO.pow(127).negate();           // -2^127
    private static final BigInteger INT128_MAX = TWO.pow(127).subtract(BigInteger.ONE); // 2^127 - 1
    private static final BigInteger INT256_MIN = TWO.pow(255).negate();           // -2^255
    private static final BigInteger INT256_MAX = TWO.pow(255).subtract(BigInteger.ONE); // 2^255 - 1
    private static final BigInteger UINT128_MAX = TWO.pow(128).subtract(BigInteger.ONE); // 2^128 - 1
    private static final BigInteger UINT256_MAX = TWO.pow(256).subtract(BigInteger.ONE); // 2^256 - 1

    @Test
    void int128RoundTrip() {
        roundTrip("Int128", List.of(
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.valueOf(-1),
                INT128_MIN,
                INT128_MAX,
                new BigInteger("170141183460469231731687303715884105720"))); // near max
    }

    @Test
    void int256RoundTrip() {
        roundTrip("Int256", List.of(
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.valueOf(-1),
                INT256_MIN,
                INT256_MAX,
                TWO.pow(200).add(BigInteger.valueOf(12345)))); // large mid
    }

    @Test
    void uint128RoundTrip() {
        roundTrip("UInt128", List.of(
                BigInteger.ZERO,
                BigInteger.ONE,
                UINT128_MAX,
                TWO.pow(127).add(BigInteger.valueOf(99)))); // high-bit-set mid
    }

    @Test
    void uint256RoundTrip() {
        roundTrip("UInt256", List.of(
                BigInteger.ZERO,
                BigInteger.ONE,
                UINT256_MAX,
                TWO.pow(255).add(BigInteger.valueOf(77)))); // high-bit-set mid
    }

    /**
     * Exercises DECODE, ENCODE, and MAPPED-READ for {@code chType} against the
     * supplied edge values (one row per value, {@code id} = index).
     */
    private void roundTrip(String chType, List<BigInteger> values) {
        // ---- DECODE: server encodes literals, client decodes ----
        withTable("wide_decode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, v " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");

            StringBuilder sql = new StringBuilder("INSERT INTO " + table + " (id, v) VALUES");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append(" (").append(i).append(", ").append(values.get(i).toString()).append(')');
            }
            conn.execute(sql.toString());

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(values.size(), rows.size(), chType + " decode row count");
            for (int i = 0; i < values.size(); i++) {
                assertEquals(values.get(i), rows.get(i)[0],
                        chType + " DECODE row " + i + " (" + values.get(i) + ")");
            }
        });

        // ---- ENCODE (bulk insert) + MAPPED-READ ----
        withTable("wide_encode", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (id UInt32, v " + chType
                    + ") ENGINE = MergeTree() ORDER BY id");

            List<R> input = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                input.add(new R(i, values.get(i)));
            }

            try (BulkInserter<R> inserter = conn.createBulkInserter(table, R.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            // (a) block-API decode back
            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(values.size(), rows.size(), chType + " encode row count");
            for (int i = 0; i < values.size(); i++) {
                assertEquals(values.get(i), rows.get(i)[0],
                        chType + " ENCODE row " + i + " (" + values.get(i) + ")");
            }

            // (b) mapped-read via query(sql, Class)
            List<R> mapped;
            try (var stream = conn.query(
                    "SELECT id, v FROM " + table + " ORDER BY id", R.class)) {
                mapped = stream.toList();
            }
            assertEquals(input, mapped, chType + " mapped-read records");
        });
    }
}
