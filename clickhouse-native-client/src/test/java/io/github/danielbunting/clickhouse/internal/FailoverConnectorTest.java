package io.github.danielbunting.clickhouse.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.BulkInserter;
import io.github.danielbunting.clickhouse.ClickHouseConnection;
import io.github.danielbunting.clickhouse.ConnectionException;
import io.github.danielbunting.clickhouse.Endpoint;
import io.github.danielbunting.clickhouse.LoadBalancingPolicy;
import io.github.danielbunting.clickhouse.QueryResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link FailoverConnector}'s connect-time failover logic. Exercises the
 * injected connection-factory seam (no sockets, no server): {@link EndpointSelector}
 * ordering is covered by {@link EndpointSelectorTest}; this verifies how that order is
 * <em>consumed</em> — first-alive wins, {@link ConnectionException} fails over, all-fail
 * aggregates with suppressed causes, and a non-{@code ConnectionException} propagates
 * un-retried.
 */
class FailoverConnectorTest {

    private static final Endpoint A = new Endpoint("a", 9000);
    private static final Endpoint B = new Endpoint("b", 9000);
    private static final Endpoint C = new Endpoint("c", 9000);
    private static final List<Endpoint> THREE = List.of(A, B, C);

    /** FIRST_ALIVE keeps configured order, so attempts run a, b, c deterministically. */
    private static EndpointSelector firstAlive() {
        return new EndpointSelector(THREE, LoadBalancingPolicy.FIRST_ALIVE);
    }

    @Test
    void firstEndpointConnects_returnsIt_withoutTryingOthers() {
        List<Endpoint> attempted = new ArrayList<>();
        StubConnection live = new StubConnection();
        FailoverConnector connector = new FailoverConnector(firstAlive(), endpoint -> {
            attempted.add(endpoint);
            return live;
        });

        ClickHouseConnection opened = connector.open();

        assertSame(live, opened, "the first endpoint's connection is returned");
        assertEquals(List.of(A), attempted, "no further endpoints are attempted after success");
    }

    @Test
    void failsOverToNextEndpointOnConnectionException() {
        List<Endpoint> attempted = new ArrayList<>();
        StubConnection live = new StubConnection();
        FailoverConnector connector = new FailoverConnector(firstAlive(), endpoint -> {
            attempted.add(endpoint);
            if (endpoint.equals(A)) {
                throw new ConnectionException("refused: " + endpoint);
            }
            return live;
        });

        ClickHouseConnection opened = connector.open();

        assertSame(live, opened, "failover lands on the first endpoint that connects");
        assertEquals(List.of(A, B), attempted, "stopped at the first success, did not try C");
    }

    @Test
    void allEndpointsFail_throwsAggregatedConnectionExceptionWithSuppressedCauses() {
        List<Endpoint> attempted = new ArrayList<>();
        FailoverConnector connector = new FailoverConnector(firstAlive(), endpoint -> {
            attempted.add(endpoint);
            throw new ConnectionException("refused: " + endpoint);
        });

        ConnectionException thrown = assertThrows(ConnectionException.class, connector::open);

        assertEquals(THREE, attempted, "every endpoint is attempted when all fail");
        assertEquals(3, thrown.getSuppressed().length,
                "each per-attempt failure is attached as a suppressed exception");
        for (Throwable suppressed : thrown.getSuppressed()) {
            assertTrue(suppressed instanceof ConnectionException,
                    "suppressed causes are the individual connect failures");
        }
        String message = thrown.getMessage();
        assertTrue(message.contains(A.toString())
                        && message.contains(B.toString())
                        && message.contains(C.toString()),
                "aggregated message lists every attempted endpoint: " + message);
    }

    @Test
    void nonConnectionExceptionPropagatesWithoutFailover() {
        List<Endpoint> attempted = new ArrayList<>();
        IllegalStateException boom = new IllegalStateException("not a connect failure");
        FailoverConnector connector = new FailoverConnector(firstAlive(), endpoint -> {
            attempted.add(endpoint);
            throw boom;
        });

        IllegalStateException thrown = assertThrows(IllegalStateException.class, connector::open);

        assertSame(boom, thrown, "a non-ConnectionException propagates unchanged");
        assertEquals(List.of(A), attempted, "no failover: later endpoints are not attempted");
    }

    /**
     * Minimal {@link ClickHouseConnection} test double. The failover tests only assert
     * connection identity and are never expected to invoke these methods, so every operation
     * fails loudly if the production path unexpectedly touches the returned connection.
     */
    private static final class StubConnection implements ClickHouseConnection {
        @Override
        public long executeScalar(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueryResult query(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Stream<T> query(String sql, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String sql) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
