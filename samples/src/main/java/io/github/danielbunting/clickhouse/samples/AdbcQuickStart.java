package io.github.danielbunting.clickhouse.samples;

import io.github.danielbunting.clickhouse.ClickHouseConfig;
import io.github.danielbunting.clickhouse.adbc.ChAdbcDriver;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * ADBC quick-start: discover the ClickHouse ADBC driver via {@link AdbcDriverManager} — exactly
 * how an external Arrow consumer would wire it — run a {@code SELECT}, print the Arrow schema, and
 * stream the result back one {@link VectorSchemaRoot} batch at a time.
 *
 * <p>Connection settings come from the environment (see {@link ClickHouseEnv}). Run with:
 * {@code ./gradlew :samples:runAdbcQuickStart}.
 */
public final class AdbcQuickStart {

    private AdbcQuickStart() {}

    public static void main(String[] args) throws Exception {
        ClickHouseConfig config = ClickHouseEnv.config();

        // Register the driver under its name; consumers then connect purely through the manager.
        AdbcDriverManager.getInstance().registerDriver(ChAdbcDriver.DRIVER_NAME, ChAdbcDriver.FACTORY);

        Map<String, Object> parameters = new HashMap<>();
        AdbcDriver.PARAM_URI.set(
                parameters,
                "chnative://" + config.host() + ":" + config.port() + "/" + config.database());
        if (config.username() != null) {
            AdbcDriver.PARAM_USERNAME.set(parameters, config.username());
        }
        if (config.password() != null) {
            AdbcDriver.PARAM_PASSWORD.set(parameters, config.password());
        }

        try (BufferAllocator allocator = new RootAllocator()) {
            AdbcDatabase database = AdbcDriverManager.getInstance()
                    .connect(ChAdbcDriver.DRIVER_NAME, allocator, parameters);
            try (AdbcConnection connection = database.connect();
                    AdbcStatement statement = connection.createStatement()) {
                statement.setSqlQuery(
                        "SELECT toInt64(number) AS n, toString(number) AS s FROM numbers(5)");
                try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                    ArrowReader reader = result.getReader();
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    System.out.println("Arrow schema: " + root.getSchema());
                    int batch = 0;
                    while (reader.loadNextBatch()) {
                        System.out.println("-- batch " + (batch++) + " (" + root.getRowCount() + " rows) --");
                        System.out.println(root.contentToTSVString());
                    }
                }
            } finally {
                database.close();
            }
        }
    }
}
