package io.github.danielbunting.clickhouse.integration;

import com.clickhouse.data.Tuple;
import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.test.CrossClientRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Cross-client round-trips for the geo types. On the wire these map to
 * composites of Float64 — {@code Point} = {@code Tuple(Float64, Float64)},
 * {@code Ring} = {@code Array(Point)}, {@code Polygon} = {@code Array(Ring)},
 * {@code MultiPolygon} = {@code Array(Polygon)} — so anchors are nested lists
 * of doubles. The native client encodes only {@code Point} via
 * {@code BulkInserter}; the deeper types use a neutral raw-SQL insert and
 * cross-validate both decoders.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest
 * --tests "*.integration.CrossClient*"}
 */
@Tag("integration")
class CrossClientGeoTypesIT extends CrossClientRoundTripBase {

    /** Point: both directions. */
    @Test
    void point() {
        record Row(int id, List<Double> p) {}
        List<Object[]> expected = rowsOf(
                row(1L, List.of(0.0, 0.0)),
                row(2L, List.of(1.5, -2.5)),
                row(3L, List.of(-180.0, 90.0)));

        withTable("xc_point", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  p  Point"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, p", rowsOf(
                    row(1L, new Tuple(0.0, 0.0)),
                    row(2L, new Tuple(1.5, -2.5)),
                    row(3L, new Tuple(-180.0, 90.0))));
            assertRowsMatch("point native decode", expected,
                    decode(conn, "SELECT id, p FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            List<Row> records = List.of(
                    new Row(1, List.of(0.0, 0.0)),
                    new Row(2, List.of(1.5, -2.5)),
                    new Row(3, List.of(-180.0, 90.0)));
            try (BulkInserter<Row> inserter = conn.createBulkInserter(table, Row.class)) {
                inserter.init();
                inserter.addRange(records);
                inserter.complete();
            }
            assertRowsMatch("point official read", expected,
                    officialSelect("SELECT id, p FROM " + table + " ORDER BY id"));
        });
    }

    /** Ring (Array(Point)): official insert + both-clients read. */
    @Test
    void ring() {
        List<Object[]> expected = rowsOf(
                row(1L, List.of(
                        List.of(1.0, 2.0), List.of(3.0, 4.0), List.of(5.0, 6.0))),
                row(2L, List.of()));

        withTable("xc_ring", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  r  Ring"
                    + ") ENGINE = MergeTree() ORDER BY id");

            // Direction A: official client renders [(x,y),...] literals.
            officialInsert(table, "id, r", rowsOf(
                    row(1L, List.of(new Tuple(1.0, 2.0), new Tuple(3.0, 4.0),
                            new Tuple(5.0, 6.0))),
                    row(2L, List.of())));
            assertRowsMatch("ring native decode", expected,
                    decode(conn, "SELECT id, r FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            // Neutral leg (no native encode for deep geo nesting): raw literals,
            // both clients cross-validate the decode.
            conn.execute("INSERT INTO " + table + " (id, r) VALUES"
                    + " (1, [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]),"
                    + " (2, [])");
            assertBothClientsRead("ring", conn,
                    "SELECT id, r FROM " + table + " ORDER BY id", expected);
        });
    }

    /** Polygon (Array(Ring)) — outer boundary plus a hole. */
    @Test
    void polygon() {
        List<Object[]> expected = rowsOf(
                row(1L, List.of(
                        List.of(List.of(0.0, 0.0), List.of(0.0, 4.0),
                                List.of(4.0, 4.0), List.of(4.0, 0.0)),
                        List.of(List.of(1.0, 1.0), List.of(1.0, 2.0),
                                List.of(2.0, 2.0), List.of(2.0, 1.0)))));

        withTable("xc_poly", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  pg Polygon"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, pg", rowsOf(
                    row(1L, List.of(
                            List.of(new Tuple(0.0, 0.0), new Tuple(0.0, 4.0),
                                    new Tuple(4.0, 4.0), new Tuple(4.0, 0.0)),
                            List.of(new Tuple(1.0, 1.0), new Tuple(1.0, 2.0),
                                    new Tuple(2.0, 2.0), new Tuple(2.0, 1.0))))));
            assertRowsMatch("polygon native decode", expected,
                    decode(conn, "SELECT id, pg FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            conn.execute("INSERT INTO " + table + " (id, pg) VALUES"
                    + " (1, [[(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)],"
                    + "      [(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)]])");
            assertBothClientsRead("polygon", conn,
                    "SELECT id, pg FROM " + table + " ORDER BY id", expected);
        });
    }

    /** MultiPolygon (Array(Polygon)) — two polygons, one with a hole. */
    @Test
    void multiPolygon() {
        List<Object[]> expected = rowsOf(
                row(1L, List.of(
                        List.of(List.of(List.of(0.0, 0.0), List.of(0.0, 1.0),
                                List.of(1.0, 1.0))),
                        List.of(
                                List.of(List.of(10.0, 10.0), List.of(10.0, 14.0),
                                        List.of(14.0, 14.0), List.of(14.0, 10.0)),
                                List.of(List.of(11.0, 11.0), List.of(11.0, 12.0),
                                        List.of(12.0, 12.0))))));

        withTable("xc_mpoly", (conn, table) -> {
            conn.execute("SET allow_experimental_geo_types = 1");
            conn.execute("CREATE TABLE " + table + " ("
                    + "  id UInt32,"
                    + "  mp MultiPolygon"
                    + ") ENGINE = MergeTree() ORDER BY id");

            officialInsert(table, "id, mp", rowsOf(
                    row(1L, List.of(
                            List.of(List.of(new Tuple(0.0, 0.0), new Tuple(0.0, 1.0),
                                    new Tuple(1.0, 1.0))),
                            List.of(
                                    List.of(new Tuple(10.0, 10.0), new Tuple(10.0, 14.0),
                                            new Tuple(14.0, 14.0), new Tuple(14.0, 10.0)),
                                    List.of(new Tuple(11.0, 11.0), new Tuple(11.0, 12.0),
                                            new Tuple(12.0, 12.0)))))));
            assertRowsMatch("multipolygon native decode", expected,
                    decode(conn, "SELECT id, mp FROM " + table + " ORDER BY id"));

            conn.execute("TRUNCATE TABLE " + table);

            conn.execute("INSERT INTO " + table + " (id, mp) VALUES"
                    + " (1, [[[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]],"
                    + "      [[(10.0, 10.0), (10.0, 14.0), (14.0, 14.0), (14.0, 10.0)],"
                    + "       [(11.0, 11.0), (11.0, 12.0), (12.0, 12.0)]]])");
            assertBothClientsRead("multipolygon", conn,
                    "SELECT id, mp FROM " + table + " ORDER BY id", expected);
        });
    }
}
