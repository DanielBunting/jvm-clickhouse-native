package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.protocol.Block;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import io.github.danielbunting.clickhouse.types.Column;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Nullable null-map correctness matrix: for <em>every</em> supported scalar inner type
 * {@code T}, builds a {@code (id UInt32, v Nullable(T))} table, inserts three rows
 * {@code (1, nonNull1), (2, NULL), (3, nonNull2)}, selects ordered by id, and asserts:
 * <ul>
 *   <li>row 2's value is {@code NULL} — both {@code column.nulls()[1] == true} and the
 *       materialised value is {@code null};</li>
 *   <li>rows 1 and 3 decode to the correct <em>non-null</em> values.</li>
 * </ul>
 * The non-null assertions on the rows flanking the NULL are the point: a null-map that is
 * read with the wrong offset (or that shifts the value section) corrupts an adjacent
 * non-null value, which this matrix would catch for each type independently.
 *
 * <p>One extra bulk-encode case ({@code Nullable(Int64)} with a {@code record R(int id, Long v)}
 * where the middle row's {@code v} is {@code null}) proves the client-side encode null-map.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class NullableScalarMatrixIT extends TypeRoundTripBase {

    /**
     * One row of the matrix: a ClickHouse inner type, two non-null SQL literals, and a
     * verifier that asserts a decoded non-null cell equals an expected logical value.
     */
    record Case(String chType, String lit1, String lit2,
                BiConsumer<Object, Object> verify, Object expect1, Object expect2) {
        @Override
        public String toString() {
            return chType;
        }
    }

    /** Number equality via longValue (signed/unsigned integer widths). */
    private static final BiConsumer<Object, Object> LONG_EQ =
            (actual, exp) -> assertEquals(((Number) exp).longValue(),
                    ((Number) actual).longValue(), "numeric value");

    /** Number equality via doubleValue (Float32/Float64). */
    private static final BiConsumer<Object, Object> DOUBLE_EQ =
            (actual, exp) -> assertEquals(((Number) exp).doubleValue(),
                    ((Number) actual).doubleValue(), 1e-9, "float value");

    /** BigDecimal equality via compareTo (ignores scale representation). */
    private static final BiConsumer<Object, Object> DECIMAL_EQ =
            (actual, exp) -> assertEquals(0,
                    ((BigDecimal) actual).compareTo((BigDecimal) exp), "decimal value");

    /** Direct {@link Object#equals} for String/temporal/uuid/enum-name. */
    private static final BiConsumer<Object, Object> OBJ_EQ =
            (actual, exp) -> assertEquals(exp, actual, "value");

    private static Stream<Case> cases() {
        return Stream.of(
                new Case("Int8", "-5", "5", LONG_EQ, -5L, 5L),
                new Case("Int16", "-300", "300", LONG_EQ, -300L, 300L),
                new Case("Int32", "-70000", "70000", LONG_EQ, -70000L, 70000L),
                new Case("Int64", "-5000000000", "5000000000", LONG_EQ,
                        -5_000_000_000L, 5_000_000_000L),
                new Case("UInt8", "200", "1", LONG_EQ, 200L, 1L),
                new Case("UInt16", "40000", "1", LONG_EQ, 40000L, 1L),
                new Case("UInt32", "3000000000", "1", LONG_EQ, 3_000_000_000L, 1L),
                new Case("UInt64", "9999999999", "1", LONG_EQ, 9_999_999_999L, 1L),
                new Case("Float32", "1.5", "2.5", DOUBLE_EQ, 1.5, 2.5),
                new Case("Float64", "2.5", "3.5", DOUBLE_EQ, 2.5, 3.5),
                new Case("String", "'x'", "'y'", OBJ_EQ, "x", "y"),
                new Case("FixedString(8)", "'abc'", "'defg'", OBJ_EQ, "abc", "defg"),
                new Case("Date", "'2024-03-15'", "'1970-01-02'", OBJ_EQ,
                        LocalDate.of(2024, 3, 15), LocalDate.of(1970, 1, 2)),
                new Case("DateTime('UTC')", "'2024-03-15 12:00:00'", "'2000-01-01 00:00:00'",
                        OBJ_EQ, Instant.parse("2024-03-15T12:00:00Z"),
                        Instant.parse("2000-01-01T00:00:00Z")),
                new Case("DateTime64(3, 'UTC')", "'2024-03-15 12:00:00.123'",
                        "'2000-01-01 00:00:00.000'", OBJ_EQ,
                        Instant.parse("2024-03-15T12:00:00.123Z"),
                        Instant.parse("2000-01-01T00:00:00Z")),
                new Case("Decimal(18,4)", "12.3456", "-1.0", DECIMAL_EQ,
                        new BigDecimal("12.3456"), new BigDecimal("-1.0")),
                new Case("Enum8('a' = 1, 'b' = 2)", "'a'", "'b'", OBJ_EQ, "a", "b"),
                new Case("UUID", "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'",
                        "'00000000-0000-0000-0000-000000000000'", OBJ_EQ,
                        UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0"),
                        new UUID(0L, 0L)));
    }

    /**
     * For each inner type: insert (nonNull1, NULL, nonNull2), select ordered, and assert
     * the null-map flags the middle row only, with both flanking values intact.
     */
    @ParameterizedTest(name = "Nullable({0})")
    @MethodSource("cases")
    void nullableScalarRoundTrip(Case c) {
        withTable("nmatrix", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Nullable(" + c.chType() + ")) ENGINE = MergeTree() ORDER BY id");
            conn.execute("INSERT INTO " + table + " (id, v) VALUES"
                    + " (1, " + c.lit1() + "), (2, NULL), (3, " + c.lit2() + ")");

            try (QueryResult result =
                         conn.query("SELECT v FROM " + table + " ORDER BY id")) {
                // Inspect the raw column null-map alongside materialised values.
                boolean[] nulls = null;
                Object[] values = new Object[3];
                int idx = 0;
                Iterator<Block> blocks = result.blocks();
                while (blocks.hasNext()) {
                    Block block = blocks.next();
                    if (block.isEmpty()) {
                        continue;
                    }
                    Column col = block.column(0);
                    boolean[] colNulls = col.nulls();
                    int rc = block.rowCount();
                    for (int r = 0; r < rc; r++) {
                        boolean isNull = colNulls != null && colNulls[r];
                        // Track the null-map flag for the middle (NULL) row.
                        if (idx == 1) {
                            nulls = colNulls;
                        }
                        values[idx] = isNull ? null : col.value(r);
                        idx++;
                    }
                }

                assertEquals(3, idx, "Nullable(" + c.chType() + "): expected 3 rows");
                assertTrue(nulls != null && nulls[1],
                        "Nullable(" + c.chType() + "): null-map bit for the NULL row must be set");
                assertNull(values[1],
                        "Nullable(" + c.chType() + "): NULL row must materialise to null");

                assertTrue(values[0] != null,
                        "Nullable(" + c.chType() + "): row 1 must be non-null (null-map shift)");
                c.verify().accept(values[0], c.expect1());
                assertTrue(values[2] != null,
                        "Nullable(" + c.chType() + "): row 3 must be non-null (null-map shift)");
                c.verify().accept(values[2], c.expect2());
            }
        });
    }

    /**
     * Record for the {@code Nullable(Int64)} encode case; the middle row's {@code v} is
     * {@code null} to exercise the client-side encode null-map.
     */
    record NullRow(int id, Long v) {}

    /**
     * ENCODE: bulk-inserts a {@code Nullable(Int64)} column where the middle value is
     * {@code null}, then reads it back and asserts the written null-map flags only that
     * row while the flanking values survive.
     */
    @Test
    void nullableInt64BulkEncodeNullMap() {
        withTable("nmatrix_enc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table
                    + " (id UInt32, v Nullable(Int64)) ENGINE = MergeTree() ORDER BY id");

            List<NullRow> input = Arrays.asList(
                    new NullRow(1, 111L),
                    new NullRow(2, null),
                    new NullRow(3, 333L));

            try (BulkInserter<NullRow> inserter =
                         conn.createBulkInserter(table, NullRow.class)) {
                inserter.init();
                inserter.addRange(input);
                inserter.complete();
            }

            List<Object[]> rows = decode(conn,
                    "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(3, rows.size(), "expected 3 encoded rows");
            assertEquals(111L, ((Number) rows.get(0)[0]).longValue(), "row 1");
            assertNull(rows.get(1)[0], "row 2 must be null (encode null-map)");
            assertEquals(333L, ((Number) rows.get(2)[0]).longValue(), "row 3");
        });
    }
}
