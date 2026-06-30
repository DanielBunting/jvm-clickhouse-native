package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.danielbunting.clickhouse.ClickHouseConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import java.time.Duration;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Slice 4: bind an Arrow {@link VectorSchemaRoot} and ingest it through the native bulk inserter
 * (the reverse bridge), including nulls and an {@code Array} column, then read the table back and
 * assert the values survive the Arrow → native → Arrow round trip.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcIngestRoundTripIT extends AdbcRoundTripBase {

    private static final List<String> COLUMNS =
            List.of("id", "label", "score", "tags");
    private static final List<String> TYPES =
            List.of("Int64", "String", "Nullable(Float64)", "Array(String)");

    @Test
    @DisplayName("Ingest 10k Arrow rows (nulls + Array), then read them back value-equal")
    void ingestThenReadBack(BufferAllocator allocator) throws Exception {
        int rows = 10_000;
        String table = uniqueTable("adbc_ingest");

        List<List<Object>> expected = new ArrayList<>(rows);

        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            createTable(connection, table);

            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(COLUMNS, TYPES), allocator)) {
                populate(root, rows, expected);

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.APPEND)) {
                    ingest.bind(root);
                    AdbcStatement.UpdateResult result = ingest.executeUpdate();
                    assertEquals(rows, result.getAffectedRows(), "ingested row count");
                }
            }
        } finally {
            database.close();
        }

        // Read back: via core for the count, and via ADBC for full value equivalence.
        try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
            assertEquals(rows, core.executeScalar("SELECT count() FROM " + table));
        }

        AdbcDatabase readDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = readDb.connect()) {
            List<List<Object>> actual =
                    viaAdbc(adbc, "SELECT id, label, score, tags FROM " + table + " ORDER BY id");
            assertEquals(expected, actual, "Arrow → native → Arrow round trip");
        } finally {
            readDb.close();
        }
    }

    @Test
    @DisplayName("CREATE then REPLACE build the table from the Arrow schema")
    void ingestCreatesAndReplacesTable(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_create");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            // CREATE: table does not exist yet; ingest builds it from root.getSchema().
            ingestSimple(connection, allocator, table, BulkIngestMode.CREATE, 0, 100);
            try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
                assertEquals(100, core.executeScalar("SELECT count() FROM " + table));
                // The schema the inserter created round-trips back to the same Arrow schema.
                assertEquals(
                        ClickHouseArrowTypes.schema(List.of("id", "label"), List.of("Int64", "String")),
                        connection.getTableSchema(null, "default", table));
            }

            // REPLACE: drop and recreate with a different row set.
            ingestSimple(connection, allocator, table, BulkIngestMode.REPLACE, 500, 3);
            try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
                assertEquals(3, core.executeScalar("SELECT count() FROM " + table));
                assertEquals(500, core.executeScalar("SELECT min(id) FROM " + table));
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("CREATE_APPEND creates the table on first use, then appends on the second")
    void createAppendCreatesThenAppends(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_create_append");
        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            // First call: table is absent, so CREATE_APPEND builds it from the Arrow schema and inserts.
            ingestSimple(connection, allocator, table, BulkIngestMode.CREATE_APPEND, 0, 4);
            // Second call: table now exists; CREATE_APPEND must append (not error like CREATE would).
            ingestSimple(connection, allocator, table, BulkIngestMode.CREATE_APPEND, 100, 6);

            try (ClickHouseConnection core = ClickHouseConnection.open(coreConfig())) {
                assertEquals(10, core.executeScalar("SELECT count() FROM " + table), "both batches retained");
                assertEquals(0, core.executeScalar("SELECT min(id) FROM " + table));
                assertEquals(105, core.executeScalar("SELECT max(id) FROM " + table));
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("Time64(3) Arrow (DurationVector) → ingest → read back as equal Durations")
    void ingestTime64RoundTrip(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_time64");
        List<Duration> values = List.of(
                Duration.ZERO,
                Duration.ofHours(12).plusMinutes(30).plusSeconds(45).plusMillis(123),
                Duration.ofHours(100).plusSeconds(1));

        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            // Gate the experimental Time64 type on this session so the ingest CREATE and read-back see it.
            try (AdbcStatement set = connection.createStatement()) {
                set.setSqlQuery("SET enable_time_time64_type = 1");
                set.executeUpdate();
            }

            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("id", "t"), List.of("Int64", "Time64(3)")), allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                DurationVector t = (DurationVector) root.getVector("t");
                for (int i = 0; i < values.size(); i++) {
                    id.setSafe(i, i);
                    t.setSafe(i, values.get(i).toMillis()); // vector unit is MILLISECOND for Time64(3)
                }
                id.setValueCount(values.size());
                t.setValueCount(values.size());
                root.setRowCount(values.size());

                // CREATE_APPEND builds the Time64 table from the Arrow schema (metadata-preserved), then inserts.
                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.CREATE_APPEND)) {
                    ingest.bind(root);
                    assertEquals(values.size(), ingest.executeUpdate().getAffectedRows());
                }
            }

            List<List<Object>> actual = viaAdbc(connection, "SELECT t FROM " + table + " ORDER BY id");
            List<List<Object>> expected = new ArrayList<>();
            for (Duration d : values) {
                expected.add(List.of(Canonicalizer.canonical(d)));
            }
            assertEquals(expected, actual, "Time64 Arrow → native → Arrow round trip");
        } finally {
            database.close();
        }
    }

    private void ingestSimple(
            AdbcConnection connection, BufferAllocator allocator, String table,
            BulkIngestMode mode, long startId, int rows) throws Exception {
        org.apache.arrow.vector.types.pojo.Schema schema =
                ClickHouseArrowTypes.schema(List.of("id", "label"), List.of("Int64", "String"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector label = (VarCharVector) root.getVector("label");
            for (int i = 0; i < rows; i++) {
                id.setSafe(i, startId + i);
                label.setSafe(i, ("label-" + (startId + i)).getBytes(StandardCharsets.UTF_8));
            }
            id.setValueCount(rows);
            label.setValueCount(rows);
            root.setRowCount(rows);

            try (AdbcStatement ingest = connection.bulkIngest(table, mode)) {
                ingest.bind(root);
                assertEquals(rows, ingest.executeUpdate().getAffectedRows());
            }
        }
    }

    private void createTable(AdbcConnection connection, String table) throws Exception {
        try (AdbcStatement ddl = connection.createStatement()) {
            ddl.setSqlQuery("CREATE TABLE " + table + " ("
                    + "id Int64, label String, score Nullable(Float64), tags Array(String)) "
                    + "ENGINE = MergeTree ORDER BY id");
            ddl.executeUpdate();
        }
    }

    /** Fills [root] with [rows] rows and records the canonical expectation for each row. */
    private void populate(VectorSchemaRoot root, int rows, List<List<Object>> expected) {
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarCharVector label = (VarCharVector) root.getVector("label");
        Float8Vector score = (Float8Vector) root.getVector("score");
        ListVector tags = (ListVector) root.getVector("tags");
        VarCharVector tagsData = (VarCharVector) tags.getDataVector();

        for (int i = 0; i < rows; i++) {
            id.setSafe(i, i);

            String labelText = "row-" + i;
            label.setSafe(i, labelText.getBytes(StandardCharsets.UTF_8));

            Double scoreValue;
            if (i % 5 == 0) {
                score.setNull(i);
                scoreValue = null;
            } else {
                double v = i * 1.5;
                score.setSafe(i, v);
                scoreValue = v;
            }

            String first = "t" + i;
            int offset = tags.startNewValue(i);
            tagsData.setSafe(offset, first.getBytes(StandardCharsets.UTF_8));
            tagsData.setSafe(offset + 1, "x".getBytes(StandardCharsets.UTF_8));
            tags.endValue(i, 2);

            expected.add(Arrays.asList(
                    Canonicalizer.canonical((long) i),
                    Canonicalizer.canonical(labelText),
                    Canonicalizer.canonical(scoreValue),
                    Canonicalizer.canonical(List.of(first, "x"))));
        }

        id.setValueCount(rows);
        label.setValueCount(rows);
        score.setValueCount(rows);
        tags.setValueCount(rows);
        root.setRowCount(rows);
    }
}
