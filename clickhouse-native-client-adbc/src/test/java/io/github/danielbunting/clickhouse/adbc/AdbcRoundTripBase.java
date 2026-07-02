package io.github.danielbunting.clickhouse.adbc;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.BlockMaterializer;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Equivalence harness: runs the same SQL through the core client and through ADBC, reducing
 * both to a {@link Canonicalizer}-normalised {@code List<Object[]>} so they can be asserted
 * equal regardless of the differing Java representations Arrow and the core client return.
 */
abstract class AdbcRoundTripBase extends AdbcIntegrationTest {

    protected static ClickHouseConfig coreConfig() {
        return ClickHouseConfig.builder()
                .host(CLICKHOUSE.getHost())
                .port(CLICKHOUSE.getMappedPort(NATIVE_PORT))
                .build();
    }

    /** Runs {@code sql} through the core client and canonicalises each cell. */
    protected static List<List<Object>> viaCore(ClickHouseConnection core, String sql) {
        try (QueryResult result = core.query(sql)) {
            List<List<Object>> rows = new ArrayList<>();
            for (Object[] row : BlockMaterializer.materialize(result)) {
                List<Object> canonical = new ArrayList<>(row.length);
                for (Object cell : row) {
                    canonical.add(Canonicalizer.canonical(cell));
                }
                rows.add(canonical);
            }
            return rows;
        }
    }

    /** Runs {@code sql} through ADBC, draining the reader and canonicalising each cell. */
    protected static List<List<Object>> viaAdbc(AdbcConnection adbc, String sql) throws Exception {
        try (AdbcStatement statement = adbc.createStatement()) {
            statement.setSqlQuery(sql);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                List<List<Object>> rows = new ArrayList<>();
                while (reader.loadNextBatch()) {
                    int rowCount = root.getRowCount();
                    List<FieldVector> vectors = root.getFieldVectors();
                    for (int r = 0; r < rowCount; r++) {
                        List<Object> canonical = new ArrayList<>(vectors.size());
                        for (FieldVector vector : vectors) {
                            // Decode via the production reverse bridge so the equivalence harness
                            // can never drift from the real Arrow→Java mapping.
                            canonical.add(Canonicalizer.canonical(ArrowToBlock.toJavaValue(vector, r)));
                        }
                        rows.add(canonical);
                    }
                }
                return rows;
            }
        }
    }

}
