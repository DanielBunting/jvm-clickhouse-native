package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ClickHouseException;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.test.IntegrationTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Connection-pool hygiene after failures: a connection whose protocol stream desynced or
 * whose socket broke must be <b>poisoned</b> and discarded (not recycled), while a clean
 * server query exception must leave the connection reusable. These tests pin that contract,
 * including with {@code validateOnBorrow} disabled — proving the invalidate-on-return path is
 * the primary safeguard, not just the {@code SELECT 1} borrow-time backstop.
 *
 * <p>Run with: {@code ./gradlew :clickhouse-native-client:integrationTest}
 */
@Tag("integration")
class PoolResilienceIT extends IntegrationTestBase {

    private ClickHouseConfig config() {
        return ClickHouseConfig.builder().host(clickHouseHost()).port(clickHousePort()).build();
    }

    /** Record for the bulk-insert-into-Enum failure case (a value not in the enum throws). */
    record EnumRow(long id, String e) {}

    // ---------------------------------------------------------------------------------------
    // A clean server query exception must NOT poison — the connection stays usable.
    // ---------------------------------------------------------------------------------------
    @Test
    void serverQueryExceptionKeepsConnectionUsable() {
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(false).build()) {
            try (ClickHouseConnection conn = pool.borrow()) {
                // Bad SQL -> server EXCEPTION packet (wire stays in spec).
                assertThrows(ClickHouseException.class,
                        () -> conn.executeScalar("SELECT * FROM a_table_that_does_not_exist_xyz"));
                assertFalse(conn.isPoisoned(),
                        "a clean server query exception must NOT poison the connection");
                // Same connection is immediately reusable.
                assertEquals(1L, conn.executeScalar("SELECT 1"),
                        "connection must still work after a server query exception");
            }
            // Borrow again (validateOnBorrow OFF): the same, healthy connection comes back and works.
            try (ClickHouseConnection conn = pool.borrow()) {
                assertEquals(1L, conn.executeScalar("SELECT 1"));
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // A protocol/decode error poisons the connection; with validateOnBorrow OFF the pool still
    // discards+replaces it on return, so the next borrower gets a clean connection.
    // ---------------------------------------------------------------------------------------
    @Test
    void protocolErrorPoisonsAndIsDiscardedEvenWithValidateOff() {
        String table = "pool_poison_" + System.nanoTime();
        try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
            admin.execute("CREATE TABLE " + table
                    + " (k UInt32, v AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k");
            admin.execute("INSERT INTO " + table + " SELECT 1, sumState(toUInt64(5))");
            try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                    .size(1).validateOnBorrow(false).build()) {
                try (ClickHouseConnection conn = pool.borrow()) {
                    // Selecting the raw AggregateFunction state fails at codec resolution inside
                    // readMessage -> the stream offset is now unknown -> poisoned.
                    assertThrows(ClickHouseException.class, () -> {
                        try (QueryResult r = conn.query("SELECT v FROM " + table)) {
                            r.blocks().forEachRemaining(b -> { });
                        }
                    });
                    assertTrue(conn.isPoisoned(),
                            "a protocol/decode error must poison the connection");
                }
                // The poisoned connection was discarded+replaced on return; the replacement is clean.
                try (ClickHouseConnection conn = pool.borrow()) {
                    assertFalse(conn.isPoisoned(), "borrowed connection must be a clean replacement");
                    assertEquals(1L, conn.executeScalar("SELECT 1"),
                            "pool must hand out a working connection after a poisoned one was returned");
                }
                assertEquals(1, pool.available() + 0, "pool keeps its size after invalidate-and-replace");
            } finally {
                admin.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // A bulk INSERT that fails mid-stream leaves the connection mid-INSERT (dirty). It must be
    // poisoned and not recycled — the recurring "dirty connection" hazard.
    // ---------------------------------------------------------------------------------------
    @Test
    void failedBulkInsertPoisonsAndPoolRecovers() {
        String table = "pool_bulkfail_" + System.nanoTime();
        try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
            admin.execute("CREATE TABLE " + table
                    + " (id UInt32, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY id");
            try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                    .size(1).validateOnBorrow(false).build()) {
                try (ClickHouseConnection conn = pool.borrow()) {
                    // add() with a value not in the enum throws (client-side) mid-INSERT.
                    assertThrows(RuntimeException.class, () -> {
                        try (BulkInserter<EnumRow> ins = conn.createBulkInserter(table, EnumRow.class)) {
                            ins.init();
                            ins.add(new EnumRow(1, "not_a_valid_enum_name"));
                            ins.complete();
                        }
                    });
                    assertTrue(conn.isPoisoned(),
                            "a bulk INSERT abandoned mid-stream must poison the connection");
                }
                // Replacement is clean and usable.
                try (ClickHouseConnection conn = pool.borrow()) {
                    assertEquals(0L, conn.executeScalar("SELECT count() FROM " + table),
                            "pool recovered; the failed insert committed nothing");
                }
            } finally {
                admin.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // A streaming result abandoned without close() (no error, so NOT poisoned) leaves the
    // connection's guard held / stream undrained. validate-on-borrow is the backstop that
    // detects this and replaces it, so the next borrower still gets a working connection.
    // ---------------------------------------------------------------------------------------
    @Test
    void abandonedStreamRecoveredByValidateOnBorrow() {
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(true).build()) {
            try (ClickHouseConnection conn = pool.borrow()) {
                // Open a lazy result and DON'T close it (leaks the guard / leaves the stream dirty).
                QueryResult leaked = conn.query("SELECT number FROM numbers(1000)");
                assertFalse(conn.isPoisoned(), "merely abandoning a stream is not a poisoning error");
                // (intentionally not closing 'leaked' nor consuming it)
            } // pooled close() returns the (dirty, guard-held) underlying connection
            // Next borrow: validate-on-borrow runs SELECT 1, which fails on the dirty/guard-held
            // connection, so the pool transparently replaces it with a fresh one.
            try (ClickHouseConnection conn = pool.borrow()) {
                assertEquals(42L, conn.executeScalar("SELECT 42"),
                        "validate-on-borrow must hand out a clean connection after a leaked stream");
            }
        }
    }
}
