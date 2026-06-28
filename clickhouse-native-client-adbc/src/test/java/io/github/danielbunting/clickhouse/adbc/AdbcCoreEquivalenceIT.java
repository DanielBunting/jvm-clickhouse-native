package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Keystone test: for each query, assert the ADBC Arrow output is value-equivalent to the core
 * client's {@code QueryResult}. This inherits the core's whole type matrix transitively rather
 * than re-specifying it. Types with Arrow representation choices (UUID/IP/FixedString/Enum)
 * are covered by {@link AdbcTypeRepresentationIT}, not here.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcCoreEquivalenceIT extends AdbcRoundTripBase {

    static Stream<Arguments> queries() {
        return Stream.of(
                q("signed ints", "SELECT toInt8(number)-2 AS a, toInt16(number)-3 AS b, "
                        + "toInt32(number)-4 AS c, toInt64(number)*1000000000-5 AS d "
                        + "FROM numbers(5) ORDER BY number"),
                q("unsigned ints", "SELECT toUInt8(number) AS a, toUInt16(number)*7 AS b, "
                        + "toUInt32(number)*100000 AS c, toUInt64(number)*9000000000000000000 AS d "
                        + "FROM numbers(5) ORDER BY number"),
                q("floats", "SELECT toFloat32(number)/2 AS f32, toFloat64(number)/3 AS f64 "
                        + "FROM numbers(5) ORDER BY number"),
                q("string", "SELECT toString(number) AS s, concat('row-', toString(number)) AS s2 "
                        + "FROM numbers(4) ORDER BY number"),
                q("bool", "SELECT (number % 2 = 0) AS b FROM numbers(4) ORDER BY number"),
                q("date/date32", "SELECT toDate('2020-01-01') + number AS d, "
                        + "toDate32('1950-03-04') + number AS d32 FROM numbers(4) ORDER BY number"),
                q("datetime", "SELECT toDateTime('2021-06-15 12:34:56', 'UTC') + number AS dt "
                        + "FROM numbers(3) ORDER BY number"),
                q("datetime64", "SELECT toDateTime64('2021-06-15 12:34:56.789123', 6, 'UTC') + number AS dt "
                        + "FROM numbers(3) ORDER BY number"),
                q("decimal64", "SELECT toDecimal64(number * 1000 + 123, 3) AS d FROM numbers(4) ORDER BY number"),
                q("decimal128", "SELECT toDecimal128(number * 7 + 1, 4) AS d FROM numbers(4) ORDER BY number"),
                q("nullable", "SELECT if(number % 2 = 0, toNullable(toInt32(number)), NULL) AS n "
                        + "FROM numbers(6) ORDER BY number"),
                q("lowcardinality", "SELECT toLowCardinality(toString(number % 3)) AS lc "
                        + "FROM numbers(6) ORDER BY number"),
                q("lowcardinality nullable", "SELECT toLowCardinality(if(number % 2 = 0, "
                        + "toNullable(toString(number)), NULL)) AS lc FROM numbers(5) ORDER BY number"),
                q("array", "SELECT range(number) AS arr FROM numbers(5) ORDER BY number"),
                q("array nullable", "SELECT arrayMap(x -> if(x % 2 = 0, toNullable(toInt32(x)), NULL), "
                        + "range(number)) AS arr FROM numbers(4) ORDER BY number"),
                q("array of array", "SELECT arrayMap(x -> range(x), range(number)) AS arr "
                        + "FROM numbers(4) ORDER BY number"),
                q("map", "SELECT map('a', toInt32(number), 'b', toInt32(number) + 1) AS m "
                        + "FROM numbers(4) ORDER BY number"),
                q("tuple", "SELECT (toInt32(number), toString(number)) AS t FROM numbers(4) ORDER BY number"),
                q("tuple nested", "SELECT (toInt32(number), (toString(number), range(number))) AS t "
                        + "FROM numbers(4) ORDER BY number"));
    }

    private static Arguments q(String name, String sql) {
        return Arguments.of(name, sql);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void adbcOutputEqualsCore(String name, String sql, BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            List<List<Object>> expected = viaCore(core, sql);
            List<List<Object>> actual = viaAdbc(adbc, sql);

            assertFalse(expected.isEmpty(), "query produced no rows: " + sql);
            assertEquals(expected, actual, "ADBC vs core mismatch for [" + name + "]: " + sql);
        } finally {
            database.close();
        }
    }
}
