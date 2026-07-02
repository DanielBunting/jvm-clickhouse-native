package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The losslessness keystone: for every storable ClickHouse type this client maps, edge-case
 * values must survive a full <b>read → Arrow → ingest</b> round trip with nothing lost — neither
 * the values (proven by comparing source and sink tables through the CORE client, so the check
 * is independent of the ADBC read path) nor the column type (proven by comparing the
 * server-reported types of the source and the metadata-recreated sink table).
 *
 * <p>Flow per type: {@code CREATE src (v T)} + INSERT edge literals → ADBC reads {@code src} →
 * bulk-ingest CREATE into {@code sink} (the table is recreated from the Arrow field's
 * {@code clickhouse.type} metadata) → assert {@code system.columns} types match and core-read
 * rows match. Two connections are used because one native connection allows a single in-flight
 * statement (the reader must stay open while the ingest runs).
 *
 * <p>Deliberately excluded: {@code Interval*}/{@code Nothing} (not storable as table columns —
 * their transform fidelity is covered by {@code BlockToArrowTemporalTest}), and
 * {@code Dynamic}/{@code Variant}, whose string rendering is documented as lossy for the
 * runtime type — pinned separately in {@link #dynamicAndVariantAreDocumentedLossy}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcLosslessRoundTripIT extends AdbcRoundTripBase {

    static Stream<Arguments> typeRows() {
        return Stream.of(
                row("Int8", "Int8", "(-128), (0), (127)"),
                row("Int16", "Int16", "(-32768), (32767)"),
                row("Int32", "Int32", "(-2147483648), (2147483647)"),
                row("Int64", "Int64", "(-9223372036854775808), (9223372036854775807)"),
                row("UInt8", "UInt8", "(0), (255)"),
                row("UInt16", "UInt16", "(0), (65535)"),
                row("UInt32", "UInt32", "(0), (4294967295)"),
                row("UInt64", "UInt64", "(0), (18446744073709551615)"),
                row("Int128", "Int128",
                        "(-170141183460469231731687303715884105728), "
                        + "(170141183460469231731687303715884105727)"),
                row("UInt128", "UInt128", "(0), (340282366920938463463374607431768211455)"),
                row("Int256", "Int256",
                        "(-57896044618658097711785492504343953926634992332820282019728792003956564819968), "
                        + "(57896044618658097711785492504343953926634992332820282019728792003956564819967)"),
                row("UInt256", "UInt256",
                        "(0), (115792089237316195423570985008687907853269984665640564039457584007913129639935)"),
                row("Float32", "Float32", "(-3.4028235e38), (1.5), (3.4028235e38)"),
                row("Float64", "Float64",
                        "(-1.7976931348623157e308), (2.5), (1.7976931348623157e308)"),
                row("Float64 specials", "Float64", "(inf), (-inf), (nan), (0)"),
                row("BFloat16", "BFloat16", "(1.5), (-2.0), (0)",
                        "SET allow_experimental_bfloat16_type = 1"),
                row("Decimal(9, 2)", "Decimal(9, 2)", "(9999999.99), (-9999999.99), (0.01)"),
                row("Decimal(18, 8)", "Decimal(18, 8)", "(9999999999.99999999), (-0.00000001)"),
                row("Decimal(38, 10)", "Decimal(38, 10)",
                        "(9999999999999999999999999999.9999999999), (-0.0000000001)"),
                row("Decimal(76, 20)", "Decimal(76, 20)",
                        "(99999999999999999999999999999999999999999999999999999999.99999999999999999999), "
                        + "(-0.00000000000000000001)"),
                row("String", "String",
                        "(''), ('héllo 世界'), ('it''s'), ('line\\nbreak')"),
                row("FixedString(8)", "FixedString(8)", "('abc'), ('exactly8')"),
                row("Date", "Date", "('1970-01-01'), ('2149-06-06')"),
                row("Date32", "Date32", "('1900-01-01'), ('2299-12-31')"),
                row("DateTime('UTC')", "DateTime('UTC')",
                        "('1970-01-02 00:00:00'), ('2106-02-01 06:28:15')"),
                row("DateTime64(3, 'UTC')", "DateTime64(3, 'UTC')",
                        "('1900-01-01 00:00:00.123'), ('2021-06-15 12:34:56.789')"),
                row("DateTime64(6, 'UTC')", "DateTime64(6, 'UTC')",
                        "('2021-06-15 12:34:56.789123'), ('1969-12-31 23:59:59.999999')"),
                row("DateTime64(9, 'UTC')", "DateTime64(9, 'UTC')",
                        "('2021-06-15 12:34:56.789123456')"),
                row("Time", "Time", "('12:30:45'), ('100:00:01')",
                        "SET enable_time_time64_type = 1"),
                row("Time64(9)", "Time64(9)", "('12:30:45.123456789')",
                        "SET enable_time_time64_type = 1"),
                row("Enum8", "Enum8('a' = 1, 'b' = 2)", "('a'), ('b')"),
                row("Enum16", "Enum16('lo' = -32768, 'hi' = 32767)", "('lo'), ('hi')"),
                row("UUID", "UUID",
                        "('00000000-0000-0000-0000-000000000000'), "
                        + "('61f0c404-5cb3-11e7-907b-a6006ad3dba0')"),
                row("IPv4", "IPv4", "('0.0.0.0'), ('192.168.0.1'), ('255.255.255.255')"),
                row("IPv6", "IPv6", "('::1'), ('2001:db8::1'), ('::ffff:1.2.3.4')"),
                row("Bool", "Bool", "(true), (false)"),
                row("Nullable(Int64)", "Nullable(Int64)", "(NULL), (42), (NULL)"),
                row("Nullable(String)", "Nullable(String)", "(NULL), ('')"),
                row("LowCardinality(String)", "LowCardinality(String)", "('x'), ('y'), ('x')"),
                row("LowCardinality(Nullable(String))", "LowCardinality(Nullable(String))",
                        "('a'), (NULL), ('a')"),
                row("Array(Int32)", "Array(Int32)", "([]), ([1, 2, 3])"),
                row("Array(Nullable(String))", "Array(Nullable(String))", "(['a', NULL, ''])"),
                row("Array(Array(Int64))", "Array(Array(Int64))", "([[1], [2, 3]]), ([])"),
                row("Map(String, Int32)", "Map(String, Int32)",
                        "(map('a', 1, 'b', 2)), (map())"),
                row("Map(String, Array(Int32))", "Map(String, Array(Int32))",
                        "(map('k', [1, 2], 'e', []))"),
                row("Tuple positional", "Tuple(Int32, String)", "((1, 'x')), ((-2, ''))"),
                row("Tuple named", "Tuple(a Int32, b String)", "((7, 'y'))"),
                row("Point", "Point", "((1.5, 2.5))", "SET allow_experimental_geo_types = 1"),
                row("Ring", "Ring", "([(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)])",
                        "SET allow_experimental_geo_types = 1"),
                row("JSON", "JSON", "('{\"k\": 1, \"s\": \"v\"}'), ('{\"k\": 2}')",
                        "SET allow_experimental_json_type = 1"));
    }

    private static Arguments row(String name, String type, String valuesSql, String... setup) {
        return Arguments.of(name, type, valuesSql, List.of(setup));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("typeRows")
    @DisplayName("read → Arrow → ingest loses neither the values nor the column type")
    void typeRoundTripsLosslessly(
            String name, String type, String valuesSql, List<String> setup,
            BufferAllocator allocator) throws Exception {
        String src = uniqueTable("lossless_src");
        String sink = uniqueTable("lossless_sink");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection readerConn = database.connect();
                AdbcConnection ingestConn = database.connect()) {
            for (String settings : setup) {
                execDdl(ingestConn, settings);
                execDdl(readerConn, settings);
            }
            execDdl(ingestConn, "CREATE TABLE " + src + " (v " + type + ") ENGINE = Memory");
            try {
                execDdl(ingestConn, "INSERT INTO " + src + " VALUES " + valuesSql);

                // ADBC read of src → bulk-ingest CREATE into sink while the reader is live.
                try (AdbcStatement select = readerConn.createStatement()) {
                    select.setSqlQuery("SELECT v FROM " + src);
                    try (AdbcStatement.QueryResult result = select.executeQuery()) {
                        ArrowReader reader = result.getReader();
                        assertTrue(reader.loadNextBatch(), "the source rows must arrive");
                        try (AdbcStatement ingest = ingestConn.bulkIngest(sink, BulkIngestMode.CREATE)) {
                            ingest.bind(reader.getVectorSchemaRoot());
                            ingest.executeUpdate();
                        }
                        assertFalse(reader.loadNextBatch(),
                                "test invariant: the tiny source must arrive as one block");
                    }
                }

                // Type losslessness: the metadata-recreated sink column is the same type.
                String typeProbe = "SELECT type FROM system.columns WHERE database = currentDatabase()"
                        + " AND table = '%s' AND name = 'v'";
                assertEquals(
                        viaCore(core, String.format(typeProbe, src)),
                        viaCore(core, String.format(typeProbe, sink)),
                        "the sink column type must match the source exactly");

                // Value losslessness: both tables read identically through the CORE client,
                // so any loss in the ADBC read or ingest path shows as a diff here.
                assertEquals(
                        viaCore(core, "SELECT v FROM " + src),
                        viaCore(core, "SELECT v FROM " + sink),
                        "values must survive the read→Arrow→ingest round trip: " + name);
            } finally {
                execDdl(ingestConn, "DROP TABLE IF EXISTS " + src);
                execDdl(ingestConn, "DROP TABLE IF EXISTS " + sink);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("DOCUMENTED LOSS: Dynamic/Variant re-ingest preserves rendered values, not runtime types")
    void dynamicAndVariantAreDocumentedLossy(BufferAllocator allocator) throws Exception {
        // Dynamic and Variant columns bridge to Utf8 (the rendered string), so a re-ingested
        // sink stores every member as a String: the VALUES survive in rendered form, the
        // per-row runtime TYPE does not. This is the documented boundary (docs/adbc.md); if a
        // typed representation ever replaces the string rendering, this pin should flip.
        String src = uniqueTable("lossy_dyn_src");
        String sink = uniqueTable("lossy_dyn_sink");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection readerConn = database.connect();
                AdbcConnection ingestConn = database.connect()) {
            for (AdbcConnection conn : List.of(readerConn, ingestConn)) {
                execDdl(conn, "SET allow_experimental_dynamic_type = 1");
            }
            execDdl(ingestConn, "CREATE TABLE " + src + " (v Dynamic) ENGINE = Memory");
            try {
                execDdl(ingestConn, "INSERT INTO " + src + " VALUES ('str7'), (42), (0.5)");

                try (AdbcStatement select = readerConn.createStatement()) {
                    select.setSqlQuery("SELECT v FROM " + src);
                    try (AdbcStatement.QueryResult result = select.executeQuery()) {
                        ArrowReader reader = result.getReader();
                        assertTrue(reader.loadNextBatch());
                        try (AdbcStatement ingest = ingestConn.bulkIngest(sink, BulkIngestMode.CREATE)) {
                            ingest.bind(reader.getVectorSchemaRoot());
                            ingest.executeUpdate();
                        }
                    }
                }

                assertEquals(viaAdbc(readerConn, "SELECT v FROM " + src),
                        viaAdbc(readerConn, "SELECT v FROM " + sink),
                        "the rendered string values must survive");
                assertEquals(
                        List.of(List.of("String"), List.of("String"), List.of("String")),
                        viaAdbc(readerConn, "SELECT dynamicType(v) FROM " + sink),
                        "the runtime types collapse to String — the documented loss");
            } finally {
                execDdl(ingestConn, "DROP TABLE IF EXISTS " + src);
                execDdl(ingestConn, "DROP TABLE IF EXISTS " + sink);
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }
}
