package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.test.ScriptedConnection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Server-free unit coverage for {@link ChAdbcConnection#getInfo}/{@link
 * ChAdbcConnection#getTableTypes} and the single-batch metadata reader contract — the ADBC
 * analogue of the JDBC module's scalar {@code ChDatabaseMetaDataTest} (whose bulk is
 * JDBC-surface-only and intentionally unported). The server version comes from a scripted
 * {@code SELECT version()} response.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class ChAdbcMetadataUnitTest {

    private static ScriptedConnection coreWithVersion(String version) {
        ScriptedConnection core = new ScriptedConnection();
        core.respondTo("version()", SystemTableBlocks.version(version));
        return core;
    }

    // ---- getInfo -----------------------------------------------------------------------------

    @Test
    @DisplayName("getInfo(null) reports all four supported codes with their values")
    void getInfoReportsAllCodes(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(coreWithVersion("25.8.1.1"), allocator)) {
            Map<Integer, String> info = readInfo(connection.getInfo((int[]) null));
            assertEquals("ClickHouse", info.get(AdbcInfoCode.VENDOR_NAME.getValue()));
            assertEquals("25.8.1.1", info.get(AdbcInfoCode.VENDOR_VERSION.getValue()));
            assertEquals(ChAdbcDriver.DRIVER_NAME, info.get(AdbcInfoCode.DRIVER_NAME.getValue()));
            assertEquals(ChAdbcDriver.DRIVER_VERSION, info.get(AdbcInfoCode.DRIVER_VERSION.getValue()));
            assertEquals(4, info.size());
        }
    }

    @Test
    @DisplayName("getInfo with an empty code array also means \"all\"")
    void getInfoEmptyArrayMeansAll(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(coreWithVersion("25.8.1.1"), allocator)) {
            assertEquals(4, readInfo(connection.getInfo(new int[0])).size());
        }
    }

    @Test
    @DisplayName("getInfo with a subset returns only the requested codes")
    void getInfoFiltersToRequestedCodes(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(coreWithVersion("25.8.1.1"), allocator)) {
            Map<Integer, String> info =
                    readInfo(connection.getInfo(new int[] {AdbcInfoCode.VENDOR_NAME.getValue()}));
            assertEquals(Map.of(AdbcInfoCode.VENDOR_NAME.getValue(), "ClickHouse"), info);
        }
    }

    @Test
    @DisplayName("unknown info codes are silently dropped")
    void getInfoDropsUnknownCodes(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(coreWithVersion("25.8.1.1"), allocator)) {
            assertTrue(readInfo(connection.getInfo(new int[] {0x7FFF_0000})).isEmpty());
        }
    }

    @Test
    @DisplayName("a failure reading the server version maps to IO")
    void getInfoVersionFailureIsIo(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        core.failNextQueryWith(new ClickHouseException("connection lost"));
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            AdbcException ex = assertThrows(AdbcException.class, () -> connection.getInfo((int[]) null));
            assertEquals(AdbcStatusCode.IO, ex.getStatus());
        }
    }

    @Test
    @DisplayName("info values are encoded on the dense union's string child (type id 0)")
    void getInfoUsesStringUnionChild(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(coreWithVersion("25.8.1.1"), allocator)) {
            try (ArrowReader reader = connection.getInfo((int[]) null)) {
                assertTrue(reader.loadNextBatch());
                DenseUnionVector values =
                        (DenseUnionVector) reader.getVectorSchemaRoot().getVector("info_value");
                for (int i = 0; i < reader.getVectorSchemaRoot().getRowCount(); i++) {
                    assertEquals(0, values.getTypeId(i), "info_value must use the string child");
                }
            }
        }
    }

    // ---- getTableTypes -------------------------------------------------------------------------

    @Test
    @DisplayName("getTableTypes lists exactly TABLE and VIEW")
    void getTableTypesRows(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            List<String> types = new ArrayList<>();
            try (ArrowReader reader = connection.getTableTypes()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    VarCharVector vector = (VarCharVector) root.getVector("table_type");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        types.add(String.valueOf(vector.getObject(i)));
                    }
                }
            }
            assertEquals(List.of("TABLE", "VIEW"), types);
        }
    }

    @Test
    @DisplayName("getTableTypes needs no server round-trip")
    void getTableTypesIsOffline(BufferAllocator allocator) throws Exception {
        ScriptedConnection core = new ScriptedConnection();
        try (ChAdbcConnection connection = AdbcTestConnections.connection(core, allocator)) {
            try (ArrowReader reader = connection.getTableTypes()) {
                reader.loadNextBatch();
            }
            assertTrue(core.queried.isEmpty(), "table types are a static contract, not a query");
        }
    }

    // ---- metadata reader contract ---------------------------------------------------------------

    @Test
    @DisplayName("a metadata reader delivers exactly one batch, then reports exhaustion")
    void metadataReaderDeliversSingleBatch(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            try (ArrowReader reader = connection.getTableTypes()) {
                assertTrue(reader.loadNextBatch(), "first call delivers the batch");
                assertFalse(reader.loadNextBatch(), "second call reports exhaustion");
                assertFalse(reader.loadNextBatch(), "exhaustion is stable");
            }
        }
    }

    @Test
    @DisplayName("closing a metadata reader twice is safe and frees its buffers")
    void metadataReaderDoubleCloseIsSafe(BufferAllocator allocator) throws Exception {
        try (ChAdbcConnection connection =
                AdbcTestConnections.connection(new ScriptedConnection(), allocator)) {
            ArrowReader reader = connection.getTableTypes();
            assertTrue(reader.loadNextBatch());
            reader.close();
            reader.close();
            // The leak-check extension proves at teardown that the reader freed everything.
        }
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Drains a GET_INFO reader into a code → string-value map. */
    private static Map<Integer, String> readInfo(ArrowReader reader) throws Exception {
        Map<Integer, String> info = new LinkedHashMap<>();
        try (reader) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                UInt4Vector names = (UInt4Vector) root.getVector("info_name");
                DenseUnionVector values = (DenseUnionVector) root.getVector("info_value");
                for (int i = 0; i < root.getRowCount(); i++) {
                    info.put(names.get(i), String.valueOf(values.getObject(i)));
                }
            }
        }
        return info;
    }
}
