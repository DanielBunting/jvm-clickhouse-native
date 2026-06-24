package io.github.danielbunting.clickhouse.bench;

/**
 * JavaBean view of {@link WideRow} for the official {@code client-v2} POJO
 * insert path, which resolves serializers via bean-style getters only
 * (same constraint as {@link BenchRowBean}).
 */
public final class WideRowBean {

    private final WideRow row;

    /**
     * @param row the canonical wide row to wrap
     */
    public WideRowBean(WideRow row) {
        this.row = row;
    }

    public long getL0() { return row.l0(); }
    public long getL1() { return row.l1(); }
    public long getL2() { return row.l2(); }
    public long getL3() { return row.l3(); }
    public long getL4() { return row.l4(); }
    public long getL5() { return row.l5(); }
    public long getL6() { return row.l6(); }
    public long getL7() { return row.l7(); }
    public long getL8() { return row.l8(); }
    public long getL9() { return row.l9(); }
    public long getL10() { return row.l10(); }
    public long getL11() { return row.l11(); }
    public long getL12() { return row.l12(); }
    public long getL13() { return row.l13(); }
    public long getL14() { return row.l14(); }
    public double getD0() { return row.d0(); }
    public double getD1() { return row.d1(); }
    public double getD2() { return row.d2(); }
    public double getD3() { return row.d3(); }
    public double getD4() { return row.d4(); }
    public double getD5() { return row.d5(); }
    public double getD6() { return row.d6(); }
    public double getD7() { return row.d7(); }
    public double getD8() { return row.d8(); }
    public double getD9() { return row.d9(); }
    public double getD10() { return row.d10(); }
    public double getD11() { return row.d11(); }
    public double getD12() { return row.d12(); }
    public double getD13() { return row.d13(); }
    public double getD14() { return row.d14(); }
}
