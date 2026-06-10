package io.github.danielbunting.clickhouse.integration;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.QueryResult;
import io.github.danielbunting.clickhouse.compress.CompressionMethod;
import io.github.danielbunting.clickhouse.pool.ClickHouseConnectionPool;
import io.github.danielbunting.clickhouse.test.ClickHouseImages;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Network-chaos integration tests: a real ClickHouse server sits behind a
 * <a href="https://github.com/Shopify/toxiproxy">Toxiproxy</a> proxy on a shared
 * Docker network, and the client connects only through the proxy. By injecting
 * toxics (TCP reset, latency, bandwidth throttling, full stall) we validate the
 * connection-pool resilience contract under genuine network faults:
 *
 * <ul>
 *   <li>A broken/desynced socket (reset_peer mid-query / mid-bulk-insert) must
 *       <b>poison</b> the borrowed connection so the pool discards+replaces it on
 *       return — proven by a subsequent borrow succeeding once the toxic is removed.</li>
 *   <li>Mere slowness (latency / bandwidth) must <b>not</b> poison: the query still
 *       completes with the correct result.</li>
 *   <li>A connect-time stall must surface as a prompt failure within the configured
 *       socket/connect timeout, never an indefinite hang.</li>
 * </ul>
 *
 * <p>This class manages its own containers (it does NOT extend
 * {@code IntegrationTestBase}) because it needs a user-defined Docker network so
 * the Toxiproxy container can reach ClickHouse by alias.
 *
 * <p>Run with:
 * {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*ToxiproxyChaosIT"}
 */
@Tag("integration")
class ToxiproxyChaosIT {

    /** Opens the default user to any source IP so the proxied (container-network) client can auth. */
    private static final String OPEN_DEFAULT_USER_XML =
            "<clickhouse>\n"
            + "  <users>\n"
            + "    <default>\n"
            + "      <networks replace=\"replace\"><ip>::/0</ip></networks>\n"
            + "    </default>\n"
            + "  </users>\n"
            + "</clickhouse>\n";

    /** Toxiproxy listen port inside the container that fronts clickhouse:9000. */
    private static final int PROXY_LISTEN_PORT = 8666;

    private static final Network NET = Network.newNetwork();

    @SuppressWarnings("resource")
    private static final GenericContainer<?> CLICKHOUSE =
            new GenericContainer<>(ClickHouseImages.SERVER)
                    .withNetwork(NET)
                    .withNetworkAliases("clickhouse")
                    .withExposedPorts(9000)
                    .withCopyToContainer(
                            Transferable.of(OPEN_DEFAULT_USER_XML),
                            "/etc/clickhouse-server/users.d/zz-open-default.xml")
                    .waitingFor(Wait.forListeningPort());

    @SuppressWarnings("resource")
    private static final ToxiproxyContainer TOXIPROXY =
            new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0").withNetwork(NET);

    /** Eclipse/rekawek control client and the single proxy fronting ClickHouse. */
    private static ToxiproxyClient toxiClient;
    private static Proxy proxy;

    /** Host/port the test JVM uses to reach ClickHouse THROUGH the proxy. */
    private static String proxyHost;
    private static int proxyPort;

    @BeforeAll
    static void startContainers() throws IOException {
        CLICKHOUSE.start();
        TOXIPROXY.start();

        toxiClient = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort());
        // Proxy listens on PROXY_LISTEN_PORT inside the toxiproxy container and forwards to
        // clickhouse:9000 over the shared Docker network.
        proxy = toxiClient.createProxy("ch", "0.0.0.0:" + PROXY_LISTEN_PORT, "clickhouse:9000");

        proxyHost = TOXIPROXY.getHost();
        proxyPort = TOXIPROXY.getMappedPort(PROXY_LISTEN_PORT);
    }

    @AfterAll
    static void stopContainers() {
        TOXIPROXY.stop();
        CLICKHOUSE.stop();
    }

    /** Remove any toxics + ensure the proxy is enabled, so each test starts from a clean network. */
    @AfterEach
    void resetToxics() throws IOException {
        proxy.enable();
        for (Toxic t : proxy.toxics().getAll()) {
            t.remove();
        }
    }

    /**
     * Short timeouts so faults surface fast. LZ4 is off (NONE) so reset_peer reliably lands
     * mid-stream on raw, sizeable result blocks rather than tiny compressed frames.
     */
    private ClickHouseConfig config() {
        return ClickHouseConfig.builder()
                .host(proxyHost)
                .port(proxyPort)
                .compression(CompressionMethod.NONE)
                .connectTimeout(Duration.ofSeconds(4))
                .socketTimeout(Duration.ofSeconds(4))
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 1. reset_peer mid-query: a TCP reset while reading a large result -> socket IOException
    //    -> ConnectionException -> connection poisoned. Pool discards it; after the toxic is
    //    removed a fresh borrow runs SELECT 1.
    // ---------------------------------------------------------------------------------------
    @Test
    void resetPeerMidQueryPoisonsAndPoolRecovers() throws IOException {
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(false).build()) {
            // reset_peer with timeout 0 = drop the connection (RST) immediately when data flows.
            proxy.toxics().resetPeer("reset_query", ToxicDirection.DOWNSTREAM, 0);

            // Borrow explicitly (not try-with-resources): the pool replaces a poisoned connection
            // *on return* by opening a fresh one, so the network must be healthy at the moment we
            // close it. We therefore remove the toxic before returning the poisoned connection.
            ClickHouseConnection conn = pool.borrow();
            // A big-ish streamed scan: the RST lands while the result block is in flight.
            assertThrows(RuntimeException.class, () -> {
                try (QueryResult r = conn.query("SELECT number FROM numbers(50000000)")) {
                    r.blocks().forEachRemaining(b -> { });
                }
            });
            assertTrue(conn.isPoisoned(),
                    "a mid-query TCP reset must poison the connection (socket IOException)");

            // Heal the network, then return the poisoned connection: the pool discards + replaces it.
            proxy.toxics().get("reset_query").remove();
            conn.close();

            // Prove the pool handed out a clean, working replacement.
            try (ClickHouseConnection fresh = pool.borrow()) {
                assertFalse(fresh.isPoisoned(), "replacement connection must be clean");
                assertEquals(1L, fresh.executeScalar("SELECT 1"),
                        "pool must recover after a poisoned connection was returned");
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // 2. reset_peer mid-bulk-insert: the RST breaks the write/ack stream -> poisoned. The pool
    //    recovers and the table reflects a CLEAN failure (no silent partial commit).
    // ---------------------------------------------------------------------------------------
    record EventRow(long id, String label, double value) {}

    @Test
    void resetPeerMidBulkInsertPoisonsAndPoolRecovers() throws IOException {
        String table = "toxi_bulkfail_" + System.nanoTime();
        try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
            admin.execute("CREATE TABLE " + table
                    + " (id UInt64, label String, value Float64) ENGINE = MergeTree ORDER BY id");
            try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                    .size(1).validateOnBorrow(false).build()) {

                proxy.toxics().resetPeer("reset_insert", ToxicDirection.DOWNSTREAM, 0);

                // Borrow explicitly so we can heal the network before returning the poisoned
                // connection (the pool opens the replacement on return — see test 1).
                ClickHouseConnection conn = pool.borrow();
                List<EventRow> rows = new ArrayList<>();
                for (int i = 0; i < 200_000; i++) {
                    rows.add(new EventRow(i, "label-" + i, i * 0.5));
                }
                assertThrows(RuntimeException.class, () -> {
                    try (BulkInserter<EventRow> ins = conn.createBulkInserter(table, EventRow.class)) {
                        ins.init();
                        ins.addRange(rows);
                        ins.complete();
                    }
                });
                assertTrue(conn.isPoisoned(),
                        "a bulk INSERT broken mid-stream by a TCP reset must poison the connection");

                proxy.toxics().get("reset_insert").remove();
                conn.close(); // discarded + replaced over the now-healthy network

                try (ClickHouseConnection fresh = pool.borrow()) {
                    assertFalse(fresh.isPoisoned(), "replacement connection must be clean");
                    long count = fresh.executeScalar("SELECT count() FROM " + table);
                    // Clean failure: an aborted INSERT must not silently succeed. ClickHouse
                    // commits a data part atomically on the terminating empty block, so a reset
                    // before completion commits nothing (count == 0). Pin "not the full dataset".
                    assertTrue(count < 200_000L,
                            "aborted bulk INSERT must not be a silent full commit; got " + count);
                    assertEquals(0L, count,
                            "a reset before INSERT completion commits no data part");
                }
            } finally {
                admin.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // 3. latency: a slow link must NOT break a normal query — resilience to latency, no poison.
    // ---------------------------------------------------------------------------------------
    @Test
    void latencyDoesNotPoisonAndQuerySucceeds() throws IOException {
        proxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 200);
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(false).build()) {
            try (ClickHouseConnection conn = pool.borrow()) {
                assertEquals(123L, conn.executeScalar("SELECT 123"),
                        "a query over a high-latency link must still return the correct result");
                assertFalse(conn.isPoisoned(), "latency is slowness, not breakage — must not poison");
            }
            // Connection recycled cleanly and is reusable.
            try (ClickHouseConnection conn = pool.borrow()) {
                assertEquals(1L, conn.executeScalar("SELECT 1"));
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // 4. bandwidth throttle: a few thousand rows over a 64 KB/s link completes within a generous
    //    bound with the right row count; no poison. socketTimeout is per-read, so a steady (if
    //    slow) byte stream does not trip it.
    // ---------------------------------------------------------------------------------------
    record NumRow(long n) {}

    @Test
    void bandwidthThrottleCompletesWithCorrectCount() throws IOException {
        proxy.toxics().bandwidth("throttle", ToxicDirection.DOWNSTREAM, 64); // 64 KB/s
        final long rowCount = 5_000;
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(false).build()) {
            assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
                try (ClickHouseConnection conn = pool.borrow()) {
                    long seen = conn.query(
                                    "SELECT number AS n FROM numbers(" + rowCount + ")", NumRow.class)
                            .count();
                    assertEquals(rowCount, seen,
                            "throttled query must still deliver every row");
                    assertFalse(conn.isPoisoned(),
                            "a slow-but-complete byte stream must not poison");
                }
            });
        }
    }

    // ---------------------------------------------------------------------------------------
    // 5. handshake stall: disable the proxy so a connect attempt cannot reach the server. The
    //    open must FAIL within ~connect/socket timeout (a few seconds), never hang indefinitely.
    // ---------------------------------------------------------------------------------------
    @Test
    void handshakeStallFailsPromptlyDoesNotHang() throws IOException {
        proxy.disable(); // accepts the TCP connection at the proxy but forwards nothing -> handshake stalls
        // Timeouts are 4s; allow generous headroom but far below an indefinite hang.
        assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                assertThrows(RuntimeException.class, () -> {
                    try (ClickHouseConnection conn = ClickHouseConnection.open(config())) {
                        conn.executeScalar("SELECT 1");
                    }
                }, "opening a connection through a stalled proxy must throw, not hang"));
    }
}
