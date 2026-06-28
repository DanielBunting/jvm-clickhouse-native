package io.github.danielbunting.clickhouse.adbc;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.test.BlockMaterializer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
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
                            canonical.add(Canonicalizer.canonical(arrowValue(vector, r)));
                        }
                        rows.add(canonical);
                    }
                }
                return rows;
            }
        }
    }

    /**
     * Extracts an Arrow cell as a core-comparable Java object (the same families the core
     * client returns: {@code Long}/{@code Double}/{@code String}/{@code Instant}/
     * {@code LocalDate}/{@code BigDecimal}/{@code List}/{@code Map}); {@link Canonicalizer}
     * then normalises both sides into one space.
     */
    static Object arrowValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }
        if (vector instanceof BitVector v) {
            return v.get(index) != 0;
        }
        if (vector instanceof Float4Vector v) {
            return (double) v.get(index);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(index);
        }
        if (vector instanceof BaseIntVector v) {
            return v.getValueAsLong(index);
        }
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), java.nio.charset.StandardCharsets.UTF_8);
        }
        if (vector instanceof DateDayVector v) {
            return LocalDate.ofEpochDay(v.get(index));
        }
        if (vector instanceof TimeStampSecTZVector v) {
            return Instant.ofEpochSecond(v.get(index));
        }
        if (vector instanceof TimeStampMilliTZVector v) {
            return Instant.ofEpochMilli(v.get(index));
        }
        if (vector instanceof TimeStampMicroTZVector v) {
            long t = v.get(index);
            return Instant.ofEpochSecond(Math.floorDiv(t, 1_000_000L), Math.floorMod(t, 1_000_000L) * 1_000L);
        }
        if (vector instanceof TimeStampNanoTZVector v) {
            long t = v.get(index);
            return Instant.ofEpochSecond(Math.floorDiv(t, 1_000_000_000L), Math.floorMod(t, 1_000_000_000L));
        }
        if (vector instanceof DecimalVector v) {
            return v.getObject(index);
        }
        if (vector instanceof Decimal256Vector v) {
            return v.getObject(index);
        }
        if (vector instanceof FixedSizeBinaryVector v) {
            return v.get(index);
        }
        if (vector instanceof MapVector v) {
            return readMap(v, index);
        }
        if (vector instanceof ListVector v) {
            return readList(v, index);
        }
        if (vector instanceof StructVector v) {
            return readStruct(v, index);
        }
        throw new UnsupportedOperationException("No decode for " + vector.getClass().getSimpleName());
    }

    private static List<Object> readList(ListVector vector, int index) {
        int start = vector.getElementStartIndex(index);
        int end = vector.getElementEndIndex(index);
        FieldVector data = (FieldVector) vector.getDataVector();
        List<Object> out = new ArrayList<>(end - start);
        for (int j = start; j < end; j++) {
            out.add(arrowValue(data, j));
        }
        return out;
    }

    private static List<Object> readStruct(StructVector vector, int index) {
        List<Object> out = new ArrayList<>();
        for (int i = 0; i < vector.getField().getChildren().size(); i++) {
            out.add(arrowValue((FieldVector) vector.getChildByOrdinal(i), index));
        }
        return out;
    }

    private static Map<Object, Object> readMap(MapVector vector, int index) {
        int start = vector.getElementStartIndex(index);
        int end = vector.getElementEndIndex(index);
        StructVector entries = (StructVector) vector.getDataVector();
        FieldVector keys = (FieldVector) entries.getChildByOrdinal(0);
        FieldVector values = (FieldVector) entries.getChildByOrdinal(1);
        Map<Object, Object> out = new LinkedHashMap<>();
        for (int j = start; j < end; j++) {
            out.put(arrowValue(keys, j), arrowValue(values, j));
        }
        return out;
    }
}
