package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reads a column that ClickHouse stores with <b>Sparse</b> serialization back through the client,
 * end-to-end against a real 25.6 server.
 *
 * <p><b>What this pins.</b> Sparse is a per-part on-disk serialization kind, not a data type: a
 * mostly-default column is stored as a default value plus the offsets of the non-default rows
 * (verified: {@code system.parts_columns.serialization_kind = 'Sparse'} on 25.6). The open
 * question the dossiers left unverified was whether 25.6 hands a sparse-stored column to a Native
 * client in sparse framing or materializes it to dense. This test answers it empirically by
 * forcing sparse storage and reading every value back through the client; whatever framing 25.6
 * uses on the wire, the decoded values must be exactly correct.
 */
@Tag("integration")
class SparseSerializationIT extends TypeRoundTripBase {

    @Test
    void sparseStoredColumnReadsBackExactly() {
        withTable("sparse_col", (conn, table) -> {
            // ratio_of_defaults_for_sparse_serialization=0.1 => a column that is >90% default is
            // stored sparsely. Only every 100th row is non-zero, so `v` is ~99% default.
            conn.execute("CREATE TABLE " + table + " (id UInt32, v UInt64) ENGINE = MergeTree "
                    + "ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1");
            conn.execute("INSERT INTO " + table
                    + " SELECT number, if(number % 100 = 0, number, 0) FROM numbers(10000)");
            conn.execute("OPTIMIZE TABLE " + table + " FINAL");

            List<Object[]> rows = decode(conn, "SELECT v FROM " + table + " ORDER BY id");
            assertEquals(10000, rows.size(), "row count");
            for (int i = 0; i < rows.size(); i++) {
                long expected = (i % 100 == 0) ? i : 0L;
                assertEquals(expected, ((Number) rows.get(i)[0]).longValue(),
                        "sparse-stored value at row " + i);
            }
        });
    }
}
