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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial network-chaos integration tests for {@link ClickHouseConnectionPool}, focused on the
 * <b>lazy self-healing</b> contract of the Semaphore-permit pool design: a poisoned/dead connection
 * is closed and its permit freed <i>without</i> opening a replacement at return time; the NEXT
 * borrow lazily opens a fresh connection over a (hopefully) healthy network. The key invariant
 * proven here is that <b>a transient outage can never permanently shrink the pool</b> and never
 * leaks a permit ({@code available()} returns to {@code size} after recovery).
 *
 * <p>Wiring mirrors {@link ToxiproxyChaosIT}: a real ClickHouse server sits behind a Toxiproxy
 * proxy on a shared Docker network; the client connects only through the proxy. These tests cover
 * the modes {@code ToxiproxyChaosIT} does <b>not</b>:
 *
 * <ol>
 *   <li>Self-heal through a <i>persistent</i> outage: return a poisoned connection while the
 *       network is still down, then heal and prove the next borrow recovers.</li>
 *   <li>Fault during validate-on-borrow (a dead idle connection / a down network at borrow time).</li>
 *   <li>Pool-wide outage under concurrent load + full recovery (permit-leak regression guard).</li>
 *   <li>Half-open / timeout toxic: an op must fail within the socket timeout, not hang.</li>
 *   <li>Throttled large bulk INSERT: complete with the exact count or fail with nothing committed.</li>
 * </ol>
 *
 * <p>Run with:
 * {@code ./gradlew :clickhouse-native-client:integrationTest --tests "*ToxiproxyResilienceIT"}
 */
@Tag("integration")
class ToxiproxyResilienceIT {

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

    private static ToxiproxyClient toxiClient;
    private static Proxy proxy;

    private static String proxyHost;
    private static int proxyPort;

    @BeforeAll
    static void startContainers() throws IOException {
        CLICKHOUSE.start();
        TOXIPROXY.start();

        toxiClient = new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort());
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

    /** Drops the network entirely: the proxy refuses to forward, so any in-flight/new I/O breaks. */
    private void cutNetwork() throws IOException {
        proxy.disable();
    }

    /** Restores the network. */
    private void healNetwork() throws IOException {
        proxy.enable();
    }

    // =======================================================================================
    // 1. SELF-HEAL THROUGH A PERSISTENT OUTAGE  (the key test)
    //
    //    size 1. Borrow, poison via reset_peer mid-query, then RETURN (close) the poisoned
    //    connection WHILE the network is STILL DOWN. Returning must NOT hang or throw and must
    //    NOT force a replacement at return time (the new lazy design). Then HEAL the network and
    //    assert the NEXT borrow lazily opens a fresh, working connection and available() == size.
    //    Proves a transient outage cannot permanently shrink the pool.
    // =======================================================================================
    @Test
    void selfHealsThroughPersistentOutage() throws IOException {
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(false).build()) {

            assertEquals(1, pool.available(), "fresh pool of size 1 has 1 free permit");

            // Poison the connection with a mid-query TCP reset.
            proxy.toxics().resetPeer("reset_query", ToxicDirection.DOWNSTREAM, 0);
            ClickHouseConnection conn = pool.borrow();
            assertEquals(0, pool.available(), "permit is checked out while borrowed");

            assertThrows(RuntimeException.class, () -> {
                try (QueryResult r = conn.query("SELECT number FROM numbers(50000000)")) {
                    r.blocks().forEachRemaining(b -> { });
                }
            });
            assertTrue(conn.isPoisoned(), "a mid-query TCP reset must poison the connection");

            // --- The point of the test: cut the network HARD, then return the poisoned
            //     connection WHILE STILL DOWN. The lazy design must NOT try to open a
            //     replacement here, so close() must return promptly (no hang, no escape).
            cutNetwork(); // remove the toxic AND disable the proxy so the link is fully dead
            proxy.toxics().get("reset_query").remove();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                    conn.close(), // returnConnection: discard poisoned + free permit, NO reconnect
                    "returning a poisoned connection during an outage must not block on a reconnect");

            assertEquals(1, pool.available(),
                    "permit must be freed on return even though the slot has no live connection");

            // --- Heal the network; the NEXT borrow must lazily open a fresh working connection.
            healNetwork();

            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                try (ClickHouseConnection fresh = pool.borrow()) {
                    assertFalse(fresh.isPoisoned(), "lazily-opened replacement must be clean");
                    assertEquals(1L, fresh.executeScalar("SELECT 1"),
                            "pool must recover to a working connection after a persistent outage");
                }
            });
            assertEquals(1, pool.available(),
                    "after recovery the permit returns to the pool: no permanent shrinkage");
        }
    }

    // =======================================================================================
    // 2. FAULT DURING VALIDATE-ON-BORROW
    //
    //    validateOnBorrow(true). The idle connection is opened healthy at construction, then the
    //    network is cut so the borrow-time SELECT 1 (or the open of a replacement) fails.
    //    (a) Network down during validation -> borrow fails PROMPTLY (within timeout), permit
    //        is released (no leak).
    //    (b) Heal -> a later borrow transparently opens a fresh connection; available() == size.
    // =======================================================================================
    @Test
    void faultDuringValidateOnBorrowRecovers() throws IOException {
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                .size(1)
                .validateOnBorrow(true)
                .borrowTimeout(Duration.ofSeconds(8))
                .build()) {

            assertEquals(1, pool.available());

            // Cut the network. The idle connection cached at construction is now dead; the
            // borrow-time SELECT 1 must fail, the dead connection is discarded, and opening a
            // fresh replacement also fails (network still down) -> borrow throws PROMPTLY.
            cutNetwork();

            assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                    assertThrows(RuntimeException.class, pool::borrow,
                            "borrow over a dead network must fail, not hang"));

            assertEquals(1, pool.available(),
                    "a failed borrow must release its permit (no permit leak on validate/open failure)");

            // Heal; a later borrow validates a freshly-opened connection and succeeds.
            healNetwork();
            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                try (ClickHouseConnection conn = pool.borrow()) {
                    assertFalse(conn.isPoisoned());
                    assertEquals(1L, conn.executeScalar("SELECT 1"),
                            "validate-on-borrow must transparently open a fresh connection after healing");
                }
            });
            assertEquals(1, pool.available(), "permit returns after recovery: no leak");
        }
    }

    // =======================================================================================
    // 3. POOL-WIDE OUTAGE + RECOVERY UNDER LOAD
    //
    //    size N. Drive N concurrent borrows each running a long scan; reset_peer kills them all
    //    (all poisoned). Return them all WHILE down; heal; assert the pool fully recovers to N
    //    working connections and available() == N. Permit-leak / lost-slot regression guard.
    // =======================================================================================
    @Test
    void poolWideOutageRecoversUnderLoad() throws IOException, InterruptedException {
        final int n = 4;
        try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                .size(n)
                .validateOnBorrow(false)
                .borrowTimeout(Duration.ofSeconds(8))
                .build()) {

            assertEquals(n, pool.available());

            // Kill every connection mid-query with a downstream reset.
            proxy.toxics().resetPeer("reset_all", ToxicDirection.DOWNSTREAM, 0);

            ExecutorService exec = Executors.newFixedThreadPool(n);
            try {
                CountDownLatch borrowed = new CountDownLatch(n);
                AtomicInteger poisoned = new AtomicInteger();
                List<Future<?>> futures = new ArrayList<>();
                List<ClickHouseConnection> borrowedConns = new ArrayList<>();

                // Borrow all N up front (single-threaded) so we can hold them, then break them
                // concurrently. Borrowing succeeds because connections were opened pre-outage.
                for (int i = 0; i < n; i++) {
                    borrowedConns.add(pool.borrow());
                }
                assertEquals(0, pool.available(), "all N permits checked out");

                for (ClickHouseConnection conn : borrowedConns) {
                    futures.add(exec.submit(() -> {
                        borrowed.countDown();
                        try {
                            try (QueryResult r = conn.query("SELECT number FROM numbers(50000000)")) {
                                r.blocks().forEachRemaining(b -> { });
                            }
                        } catch (RuntimeException expected) {
                            // reset mid-scan
                        }
                        if (conn.isPoisoned()) {
                            poisoned.incrementAndGet();
                        }
                        return null;
                    }));
                }

                assertTrue(borrowed.await(20, TimeUnit.SECONDS), "all borrows started");
                assertTimeoutPreemptively(Duration.ofSeconds(40), () -> {
                    for (Future<?> f : futures) {
                        f.get();
                    }
                });
                assertEquals(n, poisoned.get(), "every connection must be poisoned by the reset");

                // Return all poisoned connections WHILE the network is down. Each close() must
                // discard + free its permit without a synchronous reconnect (no hang/throw).
                cutNetwork();
                proxy.toxics().get("reset_all").remove();
                assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                    for (ClickHouseConnection conn : borrowedConns) {
                        conn.close();
                    }
                });
                assertEquals(n, pool.available(),
                        "all N permits freed on return despite no live connections (no lost slots)");

                // Heal and prove full recovery: N concurrent borrows each open a fresh working
                // connection and all succeed.
                healNetwork();
                AtomicInteger ok = new AtomicInteger();
                CountDownLatch done = new CountDownLatch(n);
                List<Future<?>> recover = new ArrayList<>();
                assertTimeoutPreemptively(Duration.ofSeconds(40), () -> {
                    for (int i = 0; i < n; i++) {
                        recover.add(exec.submit(() -> {
                            try (ClickHouseConnection conn = pool.borrow()) {
                                if (conn.executeScalar("SELECT 1") == 1L && !conn.isPoisoned()) {
                                    ok.incrementAndGet();
                                }
                            } finally {
                                done.countDown();
                            }
                            return null;
                        }));
                    }
                    assertTrue(done.await(35, TimeUnit.SECONDS), "all recovery borrows finished");
                    for (Future<?> f : recover) {
                        f.get();
                    }
                });
                assertEquals(n, ok.get(), "pool must recover to N working connections");
            } finally {
                exec.shutdownNow();
            }

            assertEquals(n, pool.available(),
                    "after full recovery every permit is back: no permit leak under the Semaphore model");
        }
    }

    // =======================================================================================
    // 4. SLOW-CLOSE / TIMEOUT TOXIC (half-open)
    //
    //    A `timeout` toxic stalls the connection mid-query: bytes stop flowing but the socket
    //    stays half-open. The read must trip the 4s socketTimeout and fail (no indefinite hang),
    //    poisoning the connection. After healing the pool recovers.
    // =======================================================================================
    @Test
    void timeoutToxicFailsWithinSocketTimeoutAndRecovers() throws IOException {
        try (ClickHouseConnectionPool pool =
                     ClickHouseConnectionPool.builder(config()).size(1).validateOnBorrow(false).build()) {

            // `timeout` toxic with 0 = stop all data and hold the connection open (no RST):
            // the classic half-open hang the socket timeout must defend against.
            proxy.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0);

            ClickHouseConnection conn = pool.borrow();
            // socketTimeout is 4s; bound well above it but far below an indefinite hang.
            assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                    assertThrows(RuntimeException.class, () -> {
                        try (QueryResult r = conn.query("SELECT number FROM numbers(50000000)")) {
                            r.blocks().forEachRemaining(b -> { });
                        }
                    }, "a half-open stall must trip the socket timeout, not hang"));
            assertTrue(conn.isPoisoned(),
                    "a read that times out leaves the protocol stream at an unknown offset -> poisoned");

            // Heal and recover.
            proxy.toxics().get("stall").remove();
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> conn.close());
            assertEquals(1, pool.available(), "permit freed after timeout-poisoned return");

            assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
                try (ClickHouseConnection fresh = pool.borrow()) {
                    assertFalse(fresh.isPoisoned());
                    assertEquals(1L, fresh.executeScalar("SELECT 1"),
                            "pool must recover after a timeout-toxic poisoned a connection");
                }
            });
        }
    }

    // =======================================================================================
    // 5. BANDWIDTH/LATENCY ON A LARGE BULK INSERT
    //
    //    Throttle the link and run a sizable bulk INSERT. It must EITHER complete with the exact
    //    row count OR fail cleanly with NOTHING half-committed (count == 0) — never a silent
    //    partial commit, never a hang.
    // =======================================================================================
    record EventRow(long id, String label, double value) {}

    @Test
    void throttledLargeBulkInsertIsAllOrNothing() throws IOException {
        final long rowCount = 100_000L;
        String table = "toxi_throttle_bulk_" + System.nanoTime();

        try (ClickHouseConnection admin = ClickHouseConnection.open(config())) {
            admin.execute("CREATE TABLE " + table
                    + " (id UInt64, label String, value Float64) ENGINE = MergeTree ORDER BY id");
            try {
                // Throttle + add latency: a slow, steady link. socketTimeout is per-read, so a
                // steady (if slow) stream should not trip it — the insert should complete.
                proxy.toxics().bandwidth("bulk_throttle_up", ToxicDirection.UPSTREAM, 256); // 256 KB/s
                proxy.toxics().latency("bulk_latency", ToxicDirection.DOWNSTREAM, 50);

                try (ClickHouseConnectionPool pool = ClickHouseConnectionPool.builder(config())
                        // generous per-read timeout headroom for the throttled stream
                        .size(1).validateOnBorrow(false).build()) {

                    List<EventRow> rows = new ArrayList<>((int) rowCount);
                    for (long i = 0; i < rowCount; i++) {
                        rows.add(new EventRow(i, "label-" + i, i * 0.5));
                    }

                    boolean[] completed = {false};
                    assertTimeoutPreemptively(Duration.ofSeconds(120), () -> {
                        try (ClickHouseConnection conn = pool.borrow()) {
                            try {
                                try (BulkInserter<EventRow> ins =
                                             conn.createBulkInserter(table, EventRow.class)) {
                                    ins.init();
                                    ins.addRange(rows);
                                    ins.complete();
                                }
                                completed[0] = true;
                            } catch (RuntimeException failed) {
                                // a clean failure is acceptable; we assert all-or-nothing below
                                completed[0] = false;
                            }
                        }
                    });

                    long count = admin.executeScalar("SELECT count() FROM " + table);
                    if (completed[0]) {
                        assertEquals(rowCount, count,
                                "a completed throttled INSERT must commit exactly every row");
                    } else {
                        assertEquals(0L, count,
                                "a failed throttled INSERT must commit nothing (no silent partial commit)");
                    }
                }
            } finally {
                admin.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }
}
