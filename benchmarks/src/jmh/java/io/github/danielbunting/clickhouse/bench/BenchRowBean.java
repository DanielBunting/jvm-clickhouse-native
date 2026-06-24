package io.github.danielbunting.clickhouse.bench;

import java.time.Instant;

/**
 * JavaBean view of {@link BenchRow} for the official {@code client-v2} POJO
 * insert path.
 *
 * <p>client-v2's {@code Client.register(Class, TableSchema)} resolves one
 * serializer per column by looking for bean-style accessors ({@code getId()},
 * {@code getTs()}, ...); record-style accessors ({@code id()}, {@code ts()})
 * are not matched, so {@link BenchRow} cannot be registered directly. The
 * benchmark converts the shared dataset to this shape once per trial, outside
 * the measured region, giving client-v2 its preferred input.</p>
 *
 * @see BulkInsertBenchmark#clickhouseJavaV2Client
 */
public final class BenchRowBean {

    private final long id;
    private final Instant ts;
    private final String user;
    private final double value;
    private final int status;

    /**
     * @param row the canonical row to wrap
     */
    public BenchRowBean(BenchRow row) {
        this.id = row.id();
        this.ts = row.ts();
        this.user = row.user();
        this.value = row.value();
        this.status = row.status();
    }

    /** @return the row identifier ({@code UInt64} column {@code id}) */
    public long getId() {
        return id;
    }

    /** @return the event timestamp ({@code DateTime} column {@code ts}) */
    public Instant getTs() {
        return ts;
    }

    /** @return the user label ({@code String} column {@code user}) */
    public String getUser() {
        return user;
    }

    /** @return the numeric payload ({@code Float64} column {@code value}) */
    public double getValue() {
        return value;
    }

    /** @return the status code ({@code UInt8} column {@code status}) */
    public int getStatus() {
        return status;
    }
}
