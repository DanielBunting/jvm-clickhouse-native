package io.github.danielbunting.clickhouse.integration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import io.github.danielbunting.clickhouse.types.DefaultTypeParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Sweeps the LIVE server's type catalog ({@code system.data_type_families}) against
 * {@link DefaultTypeParser} (reference: client-v2 DataTypeTests#testAllDataTypesKnown —
 * the client must know every server-advertised type). Two guarantees:
 * <ol>
 *   <li>Every family in the client's supported set parses (with canonical parameters
 *       where the family is parametric).</li>
 *   <li>No family from the live catalog makes the parser CRASH: each one either parses
 *       or is rejected with a deliberate {@link ClickHouseException} (which covers the
 *       {@code UnsupportedTypeException} contract for known-unsupported families) — never
 *       an unchecked parser error. This is how new server-side types surface as an
 *       explicit, diagnosable gap rather than a decode-time crash.</li>
 * </ol>
 */
@Tag("integration")
class TypeCatalogSweepIT extends TypeRoundTripBase {

    /** Canonical parameter lists for parametric families, keyed case-insensitively. */
    private static final Map<String, String> CANONICAL_PARAMS = Map.ofEntries(
            Map.entry("decimal", "Decimal(9, 2)"),
            Map.entry("decimal32", "Decimal32(4)"),
            Map.entry("decimal64", "Decimal64(6)"),
            Map.entry("decimal128", "Decimal128(10)"),
            Map.entry("decimal256", "Decimal256(20)"),
            Map.entry("datetime64", "DateTime64(3)"),
            Map.entry("time64", "Time64(3)"),
            Map.entry("fixedstring", "FixedString(4)"),
            Map.entry("enum", "Enum('a' = 1)"),
            Map.entry("enum8", "Enum8('a' = 1)"),
            Map.entry("enum16", "Enum16('a' = 1)"),
            Map.entry("array", "Array(UInt32)"),
            Map.entry("map", "Map(String, UInt32)"),
            Map.entry("tuple", "Tuple(UInt32, String)"),
            Map.entry("nullable", "Nullable(UInt32)"),
            Map.entry("lowcardinality", "LowCardinality(String)"),
            Map.entry("variant", "Variant(UInt32, String)"),
            Map.entry("nested", "Nested(a UInt32)"),
            Map.entry("aggregatefunction", "AggregateFunction(sum, UInt64)"),
            Map.entry("simpleaggregatefunction", "SimpleAggregateFunction(sum, UInt64)"));

    /** Families the client documents full support for (must parse, no exception). */
    private static final List<String> MUST_SUPPORT = List.of(
            "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
            "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
            "Float32", "Float64", "Bool", "String", "UUID",
            "Date", "Date32", "DateTime", "IPv4", "IPv6",
            "Decimal", "DateTime64", "FixedString", "Enum8", "Enum16",
            "Array", "Map", "Tuple", "Nullable", "LowCardinality",
            "Point", "Ring", "LineString", "Polygon", "MultiLineString", "MultiPolygon",
            "Variant", "Dynamic", "JSON", "Nested", "SimpleAggregateFunction");

    private final DefaultTypeParser parser = new DefaultTypeParser();

    private String canonicalize(String family) {
        return CANONICAL_PARAMS.getOrDefault(family.toLowerCase(java.util.Locale.ROOT), family);
    }

    @Test
    void everyServerAdvertisedFamilyParsesOrIsDeliberatelyRejected() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            List<Object[]> rows = decode(conn,
                    "SELECT name FROM system.data_type_families WHERE alias_to = '' ORDER BY name");
            assertTrue(rows.size() > 30, "expected a populated type catalog, got " + rows.size());

            List<String> unknown = new ArrayList<>();
            for (Object[] row : rows) {
                String family = String.valueOf(row[0]);
                String typeString = canonicalize(family);
                try {
                    assertNotNull(parser.parse(typeString), typeString);
                } catch (ClickHouseException deliberate) {
                    // Known-unsupported or parameter-required: an explicit, typed rejection
                    // is the documented contract. Track it for the diagnostic message only.
                    unknown.add(family + " (" + deliberate.getClass().getSimpleName() + ")");
                }
                // Anything else (NPE, ClassCast, StackOverflow...) propagates and fails.
            }
            System.out.println("Type-catalog sweep: " + rows.size() + " families, "
                    + unknown.size() + " deliberately rejected: " + unknown);
        }
    }

    @Test
    void everySupportedFamilyIsAdvertisedByTheServerAndParses() {
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            List<Object[]> rows = decode(conn,
                    "SELECT name FROM system.data_type_families");
            List<String> catalog = new ArrayList<>();
            for (Object[] row : rows) {
                catalog.add(String.valueOf(row[0]));
            }
            for (String family : MUST_SUPPORT) {
                assertTrue(catalog.contains(family),
                        "server no longer advertises supported family " + family);
                assertNotNull(parser.parse(canonicalize(family)),
                        "supported family must parse: " + family);
            }
        }
    }
}
