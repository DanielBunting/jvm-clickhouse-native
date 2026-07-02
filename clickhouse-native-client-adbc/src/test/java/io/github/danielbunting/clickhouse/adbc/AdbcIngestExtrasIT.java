package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Bulk-ingest coverage beyond {@code AdbcIngestRoundTripIT}: decimal and temporal fidelity, the
 * multi-batch bind loop, statement reuse after a failed ingest, nullable nulls, the reported
 * affected-row count, and CREATE-mode conflicts. The ADBC analogue of the write-path half of the
 * JDBC module's {@code JdbcDataTypeExtrasIT}/{@code JdbcPreparedStatementIT} batch coverage
 * (ADBC's only binding surface is ingest).
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcIngestExtrasIT extends AdbcRoundTripBase {

    @Test
    @DisplayName("Decimal values ingest exactly at 128-bit widths and read back equal to core")
    void decimalIngestRoundTrip(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_dec");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int64, d Decimal(9, 2), wide Decimal(38, 10)) ENGINE = Memory");
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(
                            List.of("id", "d", "wide"),
                            List.of("Int64", "Decimal(9, 2)", "Decimal(38, 10)")),
                    allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                DecimalVector d = (DecimalVector) root.getVector("d");
                DecimalVector wide = (DecimalVector) root.getVector("wide");
                id.setSafe(0, 1);
                d.setSafe(0, new BigDecimal("1234567.89"));
                wide.setSafe(0, new BigDecimal("1234567890123456789012345678.0123456789"));
                id.setSafe(1, 2);
                d.setSafe(1, new BigDecimal("-0.01"));
                wide.setSafe(1, new BigDecimal("-0.0000000001"));
                root.setRowCount(2);

                ingest(adbc, table, BulkIngestMode.APPEND, root);
                String sql = "SELECT id, d, wide FROM " + table + " ORDER BY id";
                assertEquals(viaCore(core, sql), viaAdbc(adbc, sql));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("DateTime64(6) instants ingest at microsecond fidelity")
    void dateTime64IngestRoundTrip(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_dt64");
        Instant instant = Instant.parse("2021-06-15T12:34:56.789123Z");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig());
                AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int64, t DateTime64(6, 'UTC')) ENGINE = Memory");
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(
                            List.of("id", "t"), List.of("Int64", "DateTime64(6, 'UTC')")),
                    allocator)) {
                ((BigIntVector) root.getVector("id")).setSafe(0, 1);
                ((TimeStampMicroTZVector) root.getVector("t")).setSafe(
                        0, instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L);
                root.setRowCount(1);

                ingest(adbc, table, BulkIngestMode.APPEND, root);
                String sql = "SELECT t FROM " + table;
                List<List<Object>> viaCore = viaCore(core, sql);
                assertEquals(viaCore, viaAdbc(adbc, sql));
                assertEquals(instant.getEpochSecond() + ":" + instant.getNano(),
                        viaCore.get(0).get(0), "microsecond fidelity must survive the ingest");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("re-binding and re-executing an ingest statement appends batch after batch")
    void multiBatchBindLoopAppends(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_loop");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table + " (n Int64) ENGINE = Memory");
            try (AdbcStatement statement = adbc.bulkIngest(table, BulkIngestMode.APPEND);
                    VectorSchemaRoot root = VectorSchemaRoot.create(
                            ClickHouseArrowTypes.schema(List.of("n"), List.of("Int64")), allocator)) {
                BigIntVector n = (BigIntVector) root.getVector("n");
                long expectedSum = 0;
                for (int batch = 0; batch < 3; batch++) {
                    for (int r = 0; r < 4; r++) {
                        n.setSafe(r, batch * 10L + r);
                        expectedSum += batch * 10L + r;
                    }
                    root.setRowCount(4);
                    statement.bind(root);
                    assertEquals(4, statement.executeUpdate().getAffectedRows(),
                            "each executeUpdate ingests the currently bound batch");
                }
                List<List<Object>> read = viaAdbc(adbc,
                        "SELECT count() AS c, sum(n) AS s FROM " + table);
                assertEquals(List.of(List.of(12L, expectedSum)), read);
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("an ingest statement stays usable after a failed attempt (missing table → then created)")
    void statementReusableAfterIngestFailure(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_retry");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect();
                AdbcStatement statement = adbc.bulkIngest(table, BulkIngestMode.APPEND);
                VectorSchemaRoot root = VectorSchemaRoot.create(
                        ClickHouseArrowTypes.schema(List.of("n"), List.of("Int64")), allocator)) {
            ((BigIntVector) root.getVector("n")).setSafe(0, 42);
            root.setRowCount(1);
            statement.bind(root);

            AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
            assertEquals(AdbcStatusCode.IO, ex.getStatus(), "APPEND into a missing table fails");

            execDdl(adbc, "CREATE TABLE " + table + " (n Int64) ENGINE = Memory");
            try {
                assertEquals(1, statement.executeUpdate().getAffectedRows(),
                        "the same statement must succeed once the target exists");
                assertEquals(List.of(List.of(42L)), viaAdbc(adbc, "SELECT n FROM " + table));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("nullable columns ingest null cells as SQL NULLs, not fillers")
    void nullableColumnsIngestNulls(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_nulls");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table
                    + " (id Int64, n Nullable(Int64), s Nullable(String)) ENGINE = Memory");
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(
                            List.of("id", "n", "s"),
                            List.of("Int64", "Nullable(Int64)", "Nullable(String)")),
                    allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                BigIntVector n = (BigIntVector) root.getVector("n");
                VarCharVector s = (VarCharVector) root.getVector("s");
                id.setSafe(0, 1);
                n.setNull(0);
                s.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
                id.setSafe(1, 2);
                n.setSafe(1, 7);
                s.setNull(1);
                root.setRowCount(2);

                ingest(adbc, table, BulkIngestMode.APPEND, root);
                assertEquals(
                        List.of(Arrays.asList(null, "x"), Arrays.asList(7L, null)),
                        viaAdbc(adbc, "SELECT n, s FROM " + table + " ORDER BY id"));
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("ingest reports the actual ingested row count — unlike SQL updates' -1")
    void ingestReportsAffectedRows(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_count");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("n"), List.of("Int64")), allocator)) {
                BigIntVector n = (BigIntVector) root.getVector("n");
                for (int r = 0; r < 5; r++) {
                    n.setSafe(r, r);
                }
                root.setRowCount(5);
                try (AdbcStatement statement = adbc.bulkIngest(table, BulkIngestMode.CREATE)) {
                    statement.bind(root);
                    assertEquals(5, statement.executeUpdate().getAffectedRows());
                }
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("CREATE mode fails when the target table already exists")
    void createModeFailsOnExistingTable(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_ing_conflict");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = database.connect()) {
            execDdl(adbc, "CREATE TABLE " + table + " (n Int64) ENGINE = Memory");
            try (AdbcStatement statement = adbc.bulkIngest(table, BulkIngestMode.CREATE);
                    VectorSchemaRoot root = VectorSchemaRoot.create(
                            ClickHouseArrowTypes.schema(List.of("n"), List.of("Int64")), allocator)) {
                ((BigIntVector) root.getVector("n")).setSafe(0, 1);
                root.setRowCount(1);
                statement.bind(root);
                AdbcException ex = assertThrows(AdbcException.class, statement::executeUpdate);
                assertEquals(AdbcStatusCode.IO, ex.getStatus());
                assertEquals(57, ex.getVendorCode(), "TABLE_ALREADY_EXISTS (57) as the vendor code");
            } finally {
                execDdl(adbc, "DROP TABLE IF EXISTS " + table);
            }
        } finally {
            database.close();
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static void ingest(
            AdbcConnection adbc, String table, BulkIngestMode mode, VectorSchemaRoot root)
            throws Exception {
        try (AdbcStatement statement = adbc.bulkIngest(table, mode)) {
            statement.bind(root);
            statement.executeUpdate();
        }
    }

    private static void execDdl(AdbcConnection connection, String sql) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery(sql);
            ddl.executeUpdate();
        }
    }
}
