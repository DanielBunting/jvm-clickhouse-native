package io.github.danielbunting.clickhouse.test;

import io.github.danielbunting.clickhouse.QueryResult;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A recording/scriptable {@link io.github.danielbunting.clickhouse.ClickHouseConnection} for
 * server-free unit tests of driver layers (the shared descendant of the JDBC module's per-class
 * {@code RecordingCore}/{@code ScriptedCore} fakes).
 *
 * <ul>
 *   <li><b>Recording:</b> every SQL string passed to {@code execute}/{@code query}/
 *       {@code executeScalar} is captured in {@link #executed}/{@link #queried}/{@link #scalars},
 *       and {@link #cancelCount}/{@link #closeCount} count those lifecycle calls.</li>
 *   <li><b>Scripted results:</b> {@link #respondTo(String, QueryResult)} answers any query whose
 *       SQL contains the given substring; otherwise {@link #enqueueResult(QueryResult)} results
 *       are consumed FIFO; otherwise the query fails loudly (no silent empty results).</li>
 *   <li><b>Scripted failures:</b> {@link #failNextQueryWith}/{@link #failNextExecuteWith} throw
 *       the given exception from the next {@code query(...)}/{@code execute(...)} call — pass a
 *       {@code ClickHouseException}, {@code UnsupportedTypeException}, … to drive error-mapping
 *       paths.</li>
 * </ul>
 */
public class ScriptedConnection extends FakeClickHouseConnection {

    public final List<String> executed = new ArrayList<>();
    public final List<String> queried = new ArrayList<>();
    public final List<String> scalars = new ArrayList<>();
    public int cancelCount;
    public int closeCount;

    private final Deque<QueryResult> queuedResults = new ArrayDeque<>();
    private final Map<String, QueryResult> responsesBySubstring = new LinkedHashMap<>();
    private RuntimeException nextQueryFailure;
    private RuntimeException nextExecuteFailure;
    private RuntimeException closeFailure;

    /** Queues a result consumed (FIFO) by the next otherwise-unmatched {@code query(...)}. */
    public ScriptedConnection enqueueResult(QueryResult result) {
        queuedResults.addLast(result);
        return this;
    }

    /** Answers every {@code query(sql)} whose SQL contains {@code sqlSubstring} with {@code result}. */
    public ScriptedConnection respondTo(String sqlSubstring, QueryResult result) {
        responsesBySubstring.put(sqlSubstring, result);
        return this;
    }

    /** Arms the next {@code query(...)} to throw {@code failure} (one-shot). */
    public ScriptedConnection failNextQueryWith(RuntimeException failure) {
        this.nextQueryFailure = failure;
        return this;
    }

    /** Arms the next {@code execute(...)} to throw {@code failure} (one-shot). */
    public ScriptedConnection failNextExecuteWith(RuntimeException failure) {
        this.nextExecuteFailure = failure;
        return this;
    }

    /** Arms every {@code close()} to throw {@code failure} (after counting the call). */
    public ScriptedConnection failCloseWith(RuntimeException failure) {
        this.closeFailure = failure;
        return this;
    }

    @Override
    public QueryResult query(String sql) {
        queried.add(sql);
        if (nextQueryFailure != null) {
            RuntimeException failure = nextQueryFailure;
            nextQueryFailure = null;
            throw failure;
        }
        for (Map.Entry<String, QueryResult> response : responsesBySubstring.entrySet()) {
            if (sql.contains(response.getKey())) {
                return response.getValue();
            }
        }
        if (!queuedResults.isEmpty()) {
            return queuedResults.removeFirst();
        }
        throw new IllegalStateException("Unscripted query in test: " + sql);
    }

    @Override
    public void execute(String sql) {
        executed.add(sql);
        if (nextExecuteFailure != null) {
            RuntimeException failure = nextExecuteFailure;
            nextExecuteFailure = null;
            throw failure;
        }
    }

    @Override
    public long executeScalar(String sql) {
        scalars.add(sql);
        return 0;
    }

    @Override
    public void cancel() {
        cancelCount++;
    }

    @Override
    public void close() {
        closeCount++;
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
