package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Concrete clickhouse-java issues, ported from the JDBC module's {@code ChJdbcIssuesTest} where
 * the failure mode lives below the driver surface (codec/wire/compression) and therefore applies
 * to ADBC too. Parameter-binding-specific issues (1373, 2299, 2327, 2329, 402) have no analogue —
 * ADBC's only binding surface is ingest; the ingest-reuse analogue of batch-reuse-after-failure
 * lives in {@code AdbcIngestExtrasIT#statementReusableAfterIngestFailure}.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcIssuesIT extends AdbcRoundTripBase {

    /** clickhouse-java#2723 — nested-array decodes (getString NPE'd; structure was misread). */
    @Test
    @DisplayName("#2723: nested and deeply nested arrays decode structurally equal to core")
    void issue2723NestedArrays(BufferAllocator allocator) throws Exception {
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            for (String sql : new String[] {
                    "SELECT [[1, 2, 3], [4, 5, 6]] AS nested_array",
                    "SELECT splitByChar('_', 'field1_field2_field3') AS split_result, "
                            + "CASE WHEN splitByChar('_', 'field1_field2_field3')[1] IN ('field1', 'field2') "
                            + "AND match(splitByChar('_', 'field1_field2_field3')[2], '(field1|field2|field3)') "
                            + "THEN 'Matched' ELSE 'NotMatched' END AS action_to_do",
                    "SELECT [[['a', 'b'], ['c']], [['d', 'e', 'f']]] AS deep_nested"}) {
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql), sql);
            }
        } finally {
            database.close();
        }
    }

    /** clickhouse-java#2657 — Array(Map(LowCardinality(String), String)) with empty maps (intermittent). */
    @Test
    @DisplayName("#2657: arrays of maps with empty maps decode stably across repeated reads")
    void issue2657ArrayOfMapsWithEmptyMaps(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_issue_2657");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (traits Array(Map(LowCardinality(String), String)))"
                    + " ENGINE = MergeTree ORDER BY tuple()");
            try {
                execDdl(adbc, "INSERT INTO " + table + " (traits) VALUES (["
                        + "  map(), "
                        + "  map('RandomKey1','Value1','RandomKey2','Value2','RandomKey3','Value3',"
                        + "      'RandomKey4','Value4','RandomKey5','Value5','RandomKey6','Value6',"
                        + "      'RandomKey7','Value7','RandomKey8','Value8'), "
                        + "  map(), map(), map(), map(), map(), map()"
                        + "])");
                // The original decode bug was intermittent; iterate like the JDBC port does.
                List<List<Object>> expected = viaCore(core, "SELECT traits FROM " + table);
                for (int attempt = 0; attempt < 10; attempt++) {
                    assertEquals(expected, viaAdbc(adbc, "SELECT traits FROM " + table),
                            "attempt " + attempt);
                }
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    /** clickhouse-java#612 — UUID + DateTime64(6) write/read; the microsecond fraction survives. */
    @Test
    @DisplayName("#612: UUID and DateTime64(6) ingest and read back with microsecond fidelity")
    void issue612UuidAndDateTime64(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_issue_612");
        UUID uuid = UUID.fromString("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        Instant ts = Instant.parse("2026-05-30T13:45:07.123456Z");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id UUID, ts DateTime64(6, 'UTC')) ENGINE = Memory");
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(
                            List.of("id", "ts"), List.of("UUID", "DateTime64(6, 'UTC')")),
                    allocator)) {
                FixedSizeBinaryVector id = (FixedSizeBinaryVector) root.getVector("id");
                id.setSafe(0, ByteBuffer.allocate(16)
                        .putLong(uuid.getMostSignificantBits())
                        .putLong(uuid.getLeastSignificantBits())
                        .array());
                ((TimeStampMicroTZVector) root.getVector("ts")).setSafe(
                        0, ts.getEpochSecond() * 1_000_000L + ts.getNano() / 1_000L);
                root.setRowCount(1);
                try (AdbcStatement ingest = adbc.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    ingest.executeUpdate();
                }

                String sql = "SELECT id, ts FROM " + table + " WHERE id = toUUID('" + uuid + "')";
                List<List<Object>> viaCore = viaCore(core, sql);
                assertEquals(1, viaCore.size(), "the ingested row must be found by its UUID");
                assertEquals(viaCore, viaAdbc(adbc, sql));
                assertEquals(ts.getEpochSecond() + ":" + ts.getNano(), viaCore.get(0).get(1),
                        "the microsecond fraction must survive the round trip");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    /** clickhouse-java#315 — IPv6 write/read. */
    @Test
    @DisplayName("#315: an IPv6 address ingests and reads back byte-identical")
    void issue315Ipv6RoundTrip(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_issue_315");
        byte[] addr = java.net.InetAddress.getByName("2001:db8::1").getAddress();
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table + " (id Int64, addr IPv6) ENGINE = Memory");
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("id", "addr"), List.of("Int64", "IPv6")),
                    allocator)) {
                ((BigIntVector) root.getVector("id")).setSafe(0, 1);
                ((FixedSizeBinaryVector) root.getVector("addr")).setSafe(0, addr);
                root.setRowCount(1);
                try (AdbcStatement ingest = adbc.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    ingest.executeUpdate();
                }

                try (AdbcStatement select = adbc.createStatement()) {
                    select.setSqlQuery("SELECT addr FROM " + table);
                    try (AdbcStatement.QueryResult result = select.executeQuery()) {
                        ArrowReader reader = result.getReader();
                        assertTrue(reader.loadNextBatch());
                        byte[] read = ((FixedSizeBinaryVector)
                                reader.getVectorSchemaRoot().getVector("addr")).get(0);
                        assertTrue(java.util.Arrays.equals(addr, read),
                                "the 16 address bytes must be identical");
                    }
                }
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    /** Decompression regression — large string payloads spanning compression-block sizes. */
    @Test
    @DisplayName("large string payloads (1 B .. 100 kB) ingest and read back through compression")
    void decompressLargeStringIngest(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_issue_decompress");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table + " (event_id String) ENGINE = Memory");
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("event_id"), List.of("String")),
                    allocator)) {
                VarCharVector v = (VarCharVector) root.getVector("event_id");
                int rows = 0;
                for (int size = 1; size <= 100_000; size *= 10) {
                    v.setSafe(rows++, "*".repeat(size).getBytes(StandardCharsets.UTF_8));
                }
                root.setRowCount(rows);
                try (AdbcStatement ingest = adbc.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    assertEquals(rows, ingest.executeUpdate().getAffectedRows());
                }

                String sql = "SELECT length(event_id) AS len FROM " + table + " ORDER BY len";
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql));
                assertEquals(
                        List.of(List.of(100_000L)),
                        viaAdbc(adbc, "SELECT max(length(event_id)) FROM " + table));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    /** End-to-end smoke through AdbcDriverManager registration — the documented entry point. */
    @Test
    @DisplayName("driver-manager registration under DRIVER_NAME connects and queries")
    void driverManagerSmoke(BufferAllocator allocator) throws Exception {
        AdbcDriverManager manager = AdbcDriverManager.getInstance();
        manager.registerDriver(ChAdbcDriver.DRIVER_NAME, ChAdbcDriver.FACTORY);
        AdbcDatabase database = manager.connect(ChAdbcDriver.DRIVER_NAME, allocator, connectParams());
        try (AdbcConnection conn = database.connect();
                AdbcStatement statement = conn.createStatement()) {
            statement.setSqlQuery("SELECT toInt64(1) AS n");
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                assertTrue(reader.loadNextBatch());
                assertEquals(1L, ((BigIntVector) reader.getVectorSchemaRoot().getVector("n")).get(0));
                assertFalse(reader.loadNextBatch());
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
