package io.github.danielbunting.clickhouse.integration;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.test.TypeRoundTripBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Insert-semantics integration tests ported from the official client-v2 suite
 * ({@code insert/InsertTests}, {@code datatypes/RowBinaryFormatWriterTest},
 * {@code api/data_formats/RowBinaryTest}): routing an insert to a database other
 * than the connection default, {@code insert_deduplication_token}, and inserts
 * into tables with {@code MATERIALIZED} / {@code ALIAS} / {@code EPHEMERAL} /
 * {@code DEFAULT &lt;expr&gt;} columns where the server must compute the derived
 * values.
 *
 * <p>API mapping to this client:
 * <ul>
 *   <li>Per-insert server settings travel via
 *       {@link ClickHouseConnection#execute(String, Map)} (there is no settings
 *       overload on {@code createBulkInserter}).</li>
 *   <li>The {@link BulkInserter} target string is used verbatim in
 *       {@code INSERT INTO <target> VALUES}, so both a qualified
 *       {@code db.table} and a {@code table (col, ...)} column list are legal
 *       targets; the server's sample block then defines the bound columns.</li>
 *   <li>For an INSERT without a column list the sample block contains ordinary
 *       and {@code DEFAULT} columns only — {@code MATERIALIZED}, {@code ALIAS}
 *       and {@code EPHEMERAL} columns are never client-supplied.</li>
 * </ul>
 *
 * <p>Run: {@code ./gradlew :clickhouse-native-client:integrationTest --tests '*InsertSemanticsIT'}
 */
@Tag("integration")
class InsertSemanticsIT extends TypeRoundTripBase {

    /** Simple two-column row used by the cross-database insert tests. */
    record NamedRow(long id, String name) {}

    /** Row for the DEFAULT-expression table: only the non-default columns. */
    record DefaultsRow(String name, String comments) {}

    /** Row matching the real (insertable, non-derived) columns of the materialized-column table. */
    record MatRow(long id, String name) {}

    /**
     * POJO for the SimplePOJO-style table (client-v2
     * {@code insertSimplePOJOsWithMaterializeColumn}): supplies the ordinary
     * columns ({@code int32}, {@code str}) plus the {@code DEFAULT}-kind column
     * {@code hexed} (which the sample block includes), while {@code int64}
     * (MATERIALIZED), {@code str_lower} (ALIAS) and {@code unhexed} (EPHEMERAL)
     * are computed/ignored server-side and must not be client-supplied.
     */
    static class MaterializePojo {
        int int32;
        String str;
        String hexed;

        MaterializePojo() {
            // no-arg constructor for the POJO mapping path
        }

        MaterializePojo(int int32, String str, String hexed) {
            this.int32 = int32;
            this.str = str;
            this.hexed = hexed;
        }
    }

    // =======================================================================================
    // 1. Insert routed to another database (client-v2 InsertTests#testInsertSettingsAddDatabase)
    // =======================================================================================

    /**
     * Inserts into a table living in a database other than the connection's
     * default, through all three routing surfaces this client offers:
     * <ol>
     *   <li>a SQL {@code INSERT INTO db.table VALUES} on the default-db connection,</li>
     *   <li>a {@link BulkInserter} opened with the qualified {@code db.table} target,</li>
     *   <li>a {@link BulkInserter} on a connection whose configured default
     *       database is the other database (bare table name).</li>
     * </ol>
     * All three rows must land in {@code db.table}, and nothing may appear in the
     * default database.
     */
    @Test
    void insertRoutedToAnotherDatabase() {
        String db = "ins_sem_db_" + System.nanoTime();
        String table = "routed_target";
        String qualified = db + "." + table;
        try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
            try {
                conn.execute("CREATE DATABASE " + db);
                conn.execute("CREATE TABLE " + qualified
                        + " (id Int64, name String) ENGINE = MergeTree ORDER BY id");

                // (1) Qualified SQL VALUES insert from the default-db connection.
                conn.execute("INSERT INTO " + qualified + " (id, name) VALUES (1, 'sql')");

                // (2) BulkInserter with a qualified db.table target string.
                try (BulkInserter<NamedRow> ins =
                             conn.createBulkInserter(qualified, NamedRow.class)) {
                    ins.init();
                    ins.add(new NamedRow(2, "bulk-qualified"));
                    ins.complete();
                }

                // (3) A connection whose DEFAULT database is `db`, bare table name.
                ClickHouseConfig cfg = ClickHouseConfig.builder()
                        .host(clickHouseHost())
                        .port(clickHousePort())
                        .database(db)
                        .build();
                try (ClickHouseConnection dbConn = ClickHouseConnection.open(cfg)) {
                    try (BulkInserter<NamedRow> ins =
                                 dbConn.createBulkInserter(table, NamedRow.class)) {
                        ins.init();
                        ins.add(new NamedRow(3, "bulk-default-db"));
                        ins.complete();
                    }
                }

                List<Object[]> rows = decode(conn,
                        "SELECT id, name FROM " + qualified + " ORDER BY id");
                assertEquals(3, rows.size(),
                        "all three routing surfaces must land in " + qualified);
                assertEquals("sql", String.valueOf(rows.get(0)[1]), "row 1: SQL insert");
                assertEquals("bulk-qualified", String.valueOf(rows.get(1)[1]),
                        "row 2: BulkInserter with qualified target");
                assertEquals("bulk-default-db", String.valueOf(rows.get(2)[1]),
                        "row 3: BulkInserter via connection-default database");

                // Nothing must have leaked into the connection's default database.
                assertEquals(0L, conn.executeScalar(
                        "SELECT count() FROM system.tables"
                                + " WHERE database = currentDatabase() AND name = '" + table + "'"),
                        "the routed table must not exist in the default database");
            } finally {
                conn.execute("DROP DATABASE IF EXISTS " + db);
            }
        }
    }

    // =======================================================================================
    // 2. insert_deduplication_token (client-v2 InsertTests#testInsertSettingsDeduplicationToken)
    // =======================================================================================

    /**
     * Three inserts carrying the same {@code insert_deduplication_token} into a
     * MergeTree table with {@code non_replicated_deduplication_window} must
     * collapse to the FIRST insert only (dedup is by token, not by data), while a
     * subsequent insert with a different token is kept.
     */
    @Test
    void insertDeduplicationTokenDeduplicatesRepeatedInserts() {
        withTable("ins_sem_dedup", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (a Int64) ENGINE = MergeTree ORDER BY a"
                    + " SETTINGS non_replicated_deduplication_window = 100");

            String token = "tok_" + System.nanoTime();
            for (int i = 0; i < 3; i++) {
                // Different data each time; the shared token alone must deduplicate.
                conn.execute("INSERT INTO " + table + " VALUES (" + i + ")",
                        Map.of("insert_deduplication_token", token));
            }

            assertEquals(1L, conn.executeScalar("SELECT count() FROM " + table),
                    "3 same-token inserts must be deduplicated down to the first one");
            assertEquals(0L, conn.executeScalar("SELECT min(a) FROM " + table),
                    "the surviving row must be the FIRST insert's value (0)");

            // A different token must NOT be deduplicated.
            conn.execute("INSERT INTO " + table + " VALUES (100)",
                    Map.of("insert_deduplication_token", token + "_other"));
            assertEquals(2L, conn.executeScalar("SELECT count() FROM " + table),
                    "an insert with a fresh token must be committed");
        });
    }

    // =======================================================================================
    // 3. Bulk insert into a table with MATERIALIZED/ALIAS columns
    //    (client-v2 InsertTests#testWriterWithMaterialize)
    // =======================================================================================

    /**
     * A bulk insert supplying only the real (ordinary) columns of a table with
     * {@code MATERIALIZED} and {@code ALIAS} columns must succeed — the server's
     * INSERT sample block excludes derived columns — and the derived values must
     * be computed server-side from the inserted data.
     */
    @Test
    void bulkInsertIntoTableWithMaterializedAndAliasColumns() {
        withTable("ins_sem_mat", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " id Int64,"
                    + " name String,"
                    + " name_lower String MATERIALIZED lower(name),"
                    + " name_upper String ALIAS upper(name)"
                    + ") ENGINE = MergeTree ORDER BY id");

            try (BulkInserter<MatRow> ins = conn.createBulkInserter(table, MatRow.class)) {
                ins.init(); // sample block must be (id, name) only
                ins.add(new MatRow(1, "Alpha"));
                ins.add(new MatRow(2, "BETA"));
                ins.complete();
            }

            // MATERIALIZED/ALIAS columns are excluded from SELECT *; ask explicitly.
            List<Object[]> rows = decode(conn,
                    "SELECT id, name, name_lower, name_upper FROM " + table + " ORDER BY id");
            assertEquals(2, rows.size(), "both bulk rows must commit");
            assertEquals("Alpha", String.valueOf(rows.get(0)[1]), "row 1 name");
            assertEquals("alpha", String.valueOf(rows.get(0)[2]),
                    "row 1 MATERIALIZED lower(name) computed server-side");
            assertEquals("ALPHA", String.valueOf(rows.get(0)[3]),
                    "row 1 ALIAS upper(name) computed server-side");
            assertEquals("beta", String.valueOf(rows.get(1)[2]),
                    "row 2 MATERIALIZED lower(name) computed server-side");
            assertEquals("BETA", String.valueOf(rows.get(1)[3]),
                    "row 2 ALIAS upper(name) computed server-side");
        });
    }

    // =======================================================================================
    // 4. POJO insert path with a MATERIALIZED column
    //    (client-v2 InsertTests#insertSimplePOJOsWithMaterializeColumn / SimplePOJO)
    // =======================================================================================

    /**
     * The mapped POJO insert path (a plain class with fields — not a record)
     * against the reference SimplePOJO table shape: {@code MATERIALIZED},
     * {@code ALIAS}, {@code EPHEMERAL} and {@code DEFAULT unhex(unhexed)}
     * columns. The sample block is (int32, str, hexed); every derived column must
     * be filled server-side and be consistent with the inserted data.
     */
    @Test
    void pojoInsertIntoTableWithMaterializedColumn() {
        withTable("ins_sem_pojo_mat", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " int32 Int32,"
                    + " str String,"
                    + " int64 Int64 MATERIALIZED abs(toInt64(int32)),"
                    + " str_lower String ALIAS lower(str),"
                    + " unhexed String EPHEMERAL,"
                    + " hexed String DEFAULT unhex(unhexed)"
                    + ") ENGINE = MergeTree ORDER BY tuple()");

            int rowCount = 100;
            try (BulkInserter<MaterializePojo> ins =
                         conn.createBulkInserter(table, MaterializePojo.class)) {
                ins.init();
                for (int i = 0; i < rowCount; i++) {
                    // hexed is a DEFAULT-kind column: present in the sample block, so the
                    // POJO supplies it directly (mirrors the reference SimplePOJO).
                    ins.add(new MaterializePojo(i - 50, "Str" + i, "hex" + i));
                }
                ins.complete();
            }

            assertEquals(rowCount, conn.executeScalar("SELECT count() FROM " + table),
                    "all POJO rows must commit");
            assertEquals(rowCount, conn.executeScalar(
                    "SELECT count() FROM " + table + " WHERE int64 = abs(toInt64(int32))"),
                    "MATERIALIZED int64 must equal abs(int32) for every row");
            assertEquals(rowCount, conn.executeScalar(
                    "SELECT count() FROM " + table + " WHERE str_lower = lower(str)"),
                    "ALIAS str_lower must equal lower(str) for every row");
            assertEquals(rowCount, conn.executeScalar(
                    "SELECT count() FROM " + table + " WHERE startsWith(hexed, 'hex')"),
                    "the client-supplied hexed values must be stored verbatim");
        });
    }

    // =======================================================================================
    // 5. Omitted DEFAULT <expr> columns are filled by the server
    //    (client-v2 RowBinaryTest#testDefaultWithFunction)
    // =======================================================================================

    /**
     * Inserting only ({@code name}, {@code comments}) into a table whose other
     * columns are {@code v Int64 DEFAULT 10} and
     * {@code fingerPrint UInt64 DEFAULT xxHash64(name)} must let the server fill
     * both defaults — via the SQL VALUES path AND via a {@link BulkInserter}
     * whose target carries an explicit column list (so the sample block excludes
     * the defaulted columns).
     */
    @Test
    void omittedDefaultExpressionColumnsAreFilledByServer() {
        withTable("ins_sem_defaults", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " ("
                    + " name String,"
                    + " v Int64 DEFAULT 10,"
                    + " fingerPrint UInt64 DEFAULT xxHash64(name),"
                    + " comments String"
                    + ") ENGINE = MergeTree ORDER BY name");

            // (a) SQL VALUES insert omitting the defaulted columns.
            conn.execute("INSERT INTO " + table + " (name, comments) VALUES ('sqlrow', 'c0')");

            // (b) Bulk insert with an explicit column list in the target string:
            //     the server's sample block is then exactly (name, comments).
            try (BulkInserter<DefaultsRow> ins =
                         conn.createBulkInserter(table + " (name, comments)", DefaultsRow.class)) {
                ins.init();
                ins.add(new DefaultsRow("bulkrow1", "c1"));
                ins.add(new DefaultsRow("bulkrow2", "c2"));
                ins.complete();
            }

            assertEquals(3L, conn.executeScalar("SELECT count() FROM " + table),
                    "both insert paths must commit");
            assertEquals(3L, conn.executeScalar(
                    "SELECT count() FROM " + table + " WHERE v = 10"),
                    "constant DEFAULT (v = 10) must be filled for every omitted row");
            assertEquals(3L, conn.executeScalar(
                    "SELECT count() FROM " + table + " WHERE fingerPrint = xxHash64(name)"),
                    "functional DEFAULT xxHash64(name) must be computed per-row server-side");
            assertEquals(0L, conn.executeScalar(
                    "SELECT count() FROM " + table + " WHERE fingerPrint = 0"),
                    "xxHash64 defaults must be real hashes, not zero-filled");
        });
    }

    /**
     * An INSERT's {@code log_comment} lands in {@code system.query_log} (reference:
     * client-v2 InsertTests#testLogComment).
     */
    @Test
    void insertLogCommentIsRecordedInQueryLog() {
        withTable("ins_sem_logc", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (a Int64) ENGINE = MergeTree ORDER BY a");
            String comment = "ins_log_comment_" + System.nanoTime();
            conn.execute("INSERT INTO " + table + " VALUES (1)", Map.of("log_comment", comment));

            conn.execute("SYSTEM FLUSH LOGS");
            org.junit.jupiter.api.Assertions.assertTrue(conn.executeScalar(
                            "SELECT count() FROM system.query_log WHERE log_comment = '" + comment + "'") >= 1,
                    "the INSERT's log_comment must be recorded in system.query_log");
        });
    }

    /**
     * The caller's per-insert settings map is not mutated (reference: client-v2
     * InsertTests#testInsertSettingsNotChanged).
     */
    @Test
    void insertSettingsMapIsNotMutatedByExecution() {
        withTable("ins_sem_immut", (conn, table) -> {
            conn.execute("CREATE TABLE " + table + " (a Int64) ENGINE = MergeTree ORDER BY a");
            java.util.Map<String, String> settings = new java.util.HashMap<>();
            settings.put("insert_deduplication_token", "tok_immutable");
            java.util.Map<String, String> snapshot = new java.util.HashMap<>(settings);

            conn.execute("INSERT INTO " + table + " VALUES (1)", settings);
            assertEquals(snapshot, settings, "execute() must not mutate the caller's settings map");
        });
    }
}
