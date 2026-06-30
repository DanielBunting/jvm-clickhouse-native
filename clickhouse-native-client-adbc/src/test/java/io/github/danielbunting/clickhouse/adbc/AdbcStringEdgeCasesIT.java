package io.github.danielbunting.clickhouse.adbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * String-payload edge cases (empty, multi-byte UTF-8, embedded NUL, long) through the full
 * Arrow → native → Arrow round trip. {@code String} round-trips have only basic ASCII coverage
 * elsewhere; ClickHouse {@code String} is an arbitrary byte sequence, so these confirm the
 * VarChar bridge neither truncates at NUL nor mangles non-ASCII.
 */
@ExtendWith(ArrowAllocatorExtension.class)
class AdbcStringEdgeCasesIT extends AdbcRoundTripBase {

    /** A single NUL char, built in code so the source file itself stays NUL-free (see SourceHygieneTest). */
    private static final String NUL = String.valueOf((char) 0);

    private static final List<String> STRINGS = List.of(
            "",
            "plain-ascii",
            "héllo, 世界 — naïve ☃",
            "before" + NUL + "after" + NUL + "end",
            "x".repeat(10_000));

    @Test
    @DisplayName("empty, UTF-8, embedded-NUL and long strings survive the round trip byte-for-byte")
    void stringEdgeCasesRoundTrip(BufferAllocator allocator) throws Exception {
        String table = uniqueTable("adbc_strings");

        AdbcDatabase database = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection connection = database.connect()) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(
                    ClickHouseArrowTypes.schema(List.of("id", "s"), List.of("Int64", "String")), allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector s = (VarCharVector) root.getVector("s");
                for (int i = 0; i < STRINGS.size(); i++) {
                    id.setSafe(i, i);
                    s.setSafe(i, STRINGS.get(i).getBytes(StandardCharsets.UTF_8));
                }
                id.setValueCount(STRINGS.size());
                s.setValueCount(STRINGS.size());
                root.setRowCount(STRINGS.size());

                try (AdbcStatement ingest = connection.bulkIngest(table, BulkIngestMode.CREATE)) {
                    ingest.bind(root);
                    assertEquals(STRINGS.size(), ingest.executeUpdate().getAffectedRows());
                }
            }
        } finally {
            database.close();
        }

        List<List<Object>> expected = new ArrayList<>();
        for (String value : STRINGS) {
            expected.add(List.of(Canonicalizer.canonical(value)));
        }

        AdbcDatabase readDb = new ChAdbcDriver(allocator).open(connectParams());
        try (AdbcConnection adbc = readDb.connect()) {
            assertEquals(expected, viaAdbc(adbc, "SELECT s FROM " + table + " ORDER BY id"));
        } finally {
            readDb.close();
        }
    }
}
