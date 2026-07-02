package io.github.danielbunting.clickhouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.danielbunting.clickhouse.protocol.Block;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@link ClickHouseConnection} interface DEFAULT implementations — the
 * compatibility surface a lightweight/fake implementation gets for free when it only
 * implements the abstract methods. The combined {@code (sql, params, settings)}
 * overloads must compose the narrower overloads when either side is empty and throw
 * {@link UnsupportedOperationException} only for a genuinely combined call; the default
 * {@code ping()} must probe via {@code SELECT 1} and never throw. Also pins
 * {@link QueryResult#summary()}'s default: implementations without wire feedback
 * report {@link QuerySummary#EMPTY}.
 *
 * <p>No transport involved: a minimal recording stub implements only the abstract
 * methods plus the single-map overloads, so every delegation lands in an observable
 * recorder.
 */
class ClickHouseConnectionDefaultsTest {

    /** A QueryResult that implements only the abstract methods (no summary override). */
    private static final class MinimalResult implements QueryResult {
        @Override
        public List<String> columnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<String> columnTypes() {
            return Collections.emptyList();
        }

        @Override
        public Iterator<Block> blocks() {
            return Collections.emptyIterator();
        }

        @Override
        public void close() {
            // nothing to release
        }
    }

    /**
     * Implements the abstract methods and the two single-map overloads, recording each
     * call; deliberately does NOT override the combined {@code (sql, params, settings)}
     * overloads, {@code ping()}, or the params-only overloads — those defaults are
     * under test.
     */
    private static class RecordingConnection implements ClickHouseConnection {

        final List<String> calls = new ArrayList<>();
        long scalarResult = 1L;
        RuntimeException scalarFailure;

        @Override
        public long executeScalar(String sql) {
            calls.add("executeScalar:" + sql);
            if (scalarFailure != null) {
                throw scalarFailure;
            }
            return scalarResult;
        }

        @Override
        public void execute(String sql) {
            calls.add("execute:" + sql);
        }

        @Override
        public void execute(String sql, Map<String, String> settings) {
            calls.add("execute+settings:" + sql + ":" + settings);
        }

        @Override
        public QueryResult query(String sql) {
            calls.add("query:" + sql);
            return new MinimalResult();
        }

        @Override
        public QueryResult query(String sql, Map<String, String> settings) {
            calls.add("query+settings:" + sql + ":" + settings);
            return new MinimalResult();
        }

        @Override
        public <T> Stream<T> query(String sql, Class<T> type) {
            return Stream.empty();
        }

        @Override
        public <T> BulkInserter<T> createBulkInserter(String table, Class<T> type) {
            throw new UnsupportedOperationException("not exercised");
        }

        @Override
        public CompletableFuture<QueryResult> queryAsync(String sql) {
            return CompletableFuture.completedFuture(query(sql));
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    private static final Map<String, String> SETTINGS = Map.of("max_execution_time", "5");

    private static QueryParameters params() {
        return QueryParameters.of(Map.of("n", 1));
    }

    // ---- combined query(sql, params, settings) default ------------------------------

    @Test
    void combinedQueryWithEmptySettingsDelegatesToParamsOverload() {
        RecordingConnection conn = new RecordingConnection();
        // Empty settings AND empty params compose down to the plain query(sql).
        conn.query("SELECT 1", QueryParameters.EMPTY, Map.of());
        assertEquals(List.of("query:SELECT 1"), conn.calls,
                "empty params + empty settings must reach the plain query(sql)");
    }

    @Test
    void combinedQueryWithEmptySettingsAndRealParamsSurfacesUnsupported() {
        RecordingConnection conn = new RecordingConnection();
        // Settings are empty, so the default routes to query(sql, params) — whose own
        // default rejects real params on an implementation that never overrode it.
        assertThrows(UnsupportedOperationException.class,
                () -> conn.query("SELECT {n:UInt8}", params(), Map.of()),
                "a params-incapable connection must reject real bindings, not drop them");
        assertTrue(conn.calls.isEmpty(), "nothing may execute when the bindings are rejected");
    }

    @Test
    void combinedQueryWithEmptyParamsDelegatesToSettingsOverload() {
        RecordingConnection conn = new RecordingConnection();
        conn.query("SELECT 1", QueryParameters.EMPTY, SETTINGS);
        assertEquals(List.of("query+settings:SELECT 1:" + SETTINGS), conn.calls,
                "empty params must route the call to query(sql, settings), keeping the settings");

        conn.calls.clear();
        conn.query("SELECT 1", null, SETTINGS); // null params behave exactly like empty
        assertEquals(List.of("query+settings:SELECT 1:" + SETTINGS), conn.calls);
    }

    @Test
    void combinedQueryWithBothNonEmptyThrowsUnsupportedByDefault() {
        RecordingConnection conn = new RecordingConnection();
        assertThrows(UnsupportedOperationException.class,
                () -> conn.query("SELECT {n:UInt8}", params(), SETTINGS),
                "a genuinely combined call has no narrower overload to compose — the "
                        + "default must refuse rather than silently drop either side");
        assertTrue(conn.calls.isEmpty(), "the refusal must happen before any execution");
    }

    // ---- combined execute(sql, params, settings) default -----------------------------

    @Test
    void combinedExecuteWithEmptySettingsDelegatesToParamsOverload() {
        RecordingConnection conn = new RecordingConnection();
        conn.execute("DROP TABLE t", QueryParameters.EMPTY, Map.of());
        assertEquals(List.of("execute:DROP TABLE t"), conn.calls,
                "empty params + empty settings must reach the plain execute(sql)");
    }

    @Test
    void combinedExecuteWithEmptyParamsDelegatesToSettingsOverload() {
        RecordingConnection conn = new RecordingConnection();
        conn.execute("DROP TABLE t", null, SETTINGS);
        assertEquals(List.of("execute+settings:DROP TABLE t:" + SETTINGS), conn.calls,
                "empty params must route the call to execute(sql, settings), keeping the settings");
    }

    @Test
    void combinedExecuteWithBothNonEmptyThrowsUnsupportedByDefault() {
        RecordingConnection conn = new RecordingConnection();
        assertThrows(UnsupportedOperationException.class,
                () -> conn.execute("INSERT INTO t SELECT {n:UInt8}", params(), SETTINGS));
        assertTrue(conn.calls.isEmpty(), "the refusal must happen before any execution");
    }

    // ---- default ping() -------------------------------------------------------------

    @Test
    void defaultPingProbesWithSelectOneAndReportsTrue() {
        RecordingConnection conn = new RecordingConnection();
        assertTrue(conn.ping(), "SELECT 1 == 1 must report alive");
        assertEquals(List.of("executeScalar:SELECT 1"), conn.calls,
                "the default probe is the documented SELECT 1 fallback");
    }

    @Test
    void defaultPingNeverThrowsAndReportsFalseOnFailure() {
        RecordingConnection conn = new RecordingConnection();
        conn.scalarFailure = new IllegalStateException("socket gone");
        assertFalse(conn.ping(), "a failing probe must become false, not an exception");
    }

    @Test
    void defaultPingReportsFalseOnWrongScalar() {
        RecordingConnection conn = new RecordingConnection();
        conn.scalarResult = 2L;
        assertFalse(conn.ping(),
                "a probe that does not evaluate SELECT 1 to 1 proves the wire is not sane");
    }

    // ---- QueryResult default summary() ------------------------------------------------

    @Test
    void queryResultWithoutWireFeedbackReportsEmptySummary() {
        try (QueryResult result = new MinimalResult()) {
            assertSame(QuerySummary.EMPTY, result.summary(),
                    "implementations without Progress/ProfileInfo feedback must report "
                            + "QuerySummary.EMPTY, never null");
        }
    }
}
