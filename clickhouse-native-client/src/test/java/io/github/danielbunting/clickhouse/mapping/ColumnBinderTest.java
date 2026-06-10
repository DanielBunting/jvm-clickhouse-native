package io.github.danielbunting.clickhouse.mapping;

import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.Float64Codec;
import io.github.danielbunting.clickhouse.types.codec.Int32Codec;
import io.github.danielbunting.clickhouse.types.codec.Int64Codec;
import io.github.danielbunting.clickhouse.types.codec.NullableColumnCodec;
import io.github.danielbunting.clickhouse.types.codec.UInt8Codec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the typed write-path binders ({@link ColumnBinder} and
 * {@link RowMappers#columnBinders}). Verify that numeric-only records and POJOs
 * bind via the primitive {@code setLong}/{@code setDouble} fast path (round-tripping
 * values into the column arrays), that nullable boxed fields route through the
 * null-map, and that type coercion (an {@code int} field into a {@code UInt8}
 * column) works through {@code setLong}.
 */
class ColumnBinderTest {

    /** A numeric-only record: long + int + double, plus an int that targets UInt8. */
    record NumericRecord(long id, int count, double price, int level) {}

    /** A numeric-only POJO with the same shape. */
    static final class NumericPojo {
        long id;
        int count;
        double price;
        int level;

        NumericPojo() {}

        NumericPojo(long id, int count, double price, int level) {
            this.id = id;
            this.count = count;
            this.price = price;
            this.level = level;
        }
    }

    /** A record with a nullable boxed Long field. */
    record NullableRecord(long id, Long maybe) {}

    private static ColumnCodec<?>[] numericCodecs() {
        return new ColumnCodec<?>[]{
                new Int64Codec(),   // id   (long)
                new Int32Codec(),   // count(int)
                new Float64Codec(), // price(double)
                new UInt8Codec()    // level(int -> UInt8 coercion)
        };
    }

    private static final String[] NUMERIC_COLS = {"id", "count", "price", "level"};

    @Test
    void record_numericOnly_bindsViaPrimitivePath() {
        ColumnCodec<?>[] codecs = numericCodecs();
        ColumnBinder[] binders = RowMappers.columnBinders(
                NumericRecord.class, NUMERIC_COLS, codecs, new boolean[4]);

        assertEquals(ColumnBinder.Kind.LONG, binders[0].kind());
        assertEquals(ColumnBinder.Kind.LONG, binders[1].kind());
        assertEquals(ColumnBinder.Kind.DOUBLE, binders[2].kind());
        assertEquals(ColumnBinder.Kind.LONG, binders[3].kind());

        long[] ids = (long[]) codecs[0].allocate(1);
        int[] counts = (int[]) codecs[1].allocate(1);
        double[] prices = (double[]) codecs[2].allocate(1);
        int[] levels = (int[]) codecs[3].allocate(1);

        NumericRecord row = new NumericRecord(Long.MAX_VALUE, -42, 3.14, 200);
        binders[0].bind(row, ids, 0, null);
        binders[1].bind(row, counts, 0, null);
        binders[2].bind(row, prices, 0, null);
        binders[3].bind(row, levels, 0, null);

        assertEquals(Long.MAX_VALUE, ids[0]);
        assertEquals(-42, counts[0]);
        assertEquals(3.14, prices[0], 0.0);
        // 200 fits in UInt8 [0,255]; coerced int->long->UInt8.
        assertEquals(200, levels[0]);
    }

    @Test
    void pojo_numericOnly_bindsViaPrimitivePath() {
        ColumnCodec<?>[] codecs = numericCodecs();
        ColumnBinder[] binders = RowMappers.columnBinders(
                NumericPojo.class, NUMERIC_COLS, codecs, new boolean[4]);

        assertEquals(ColumnBinder.Kind.LONG, binders[0].kind());
        assertEquals(ColumnBinder.Kind.LONG, binders[1].kind());
        assertEquals(ColumnBinder.Kind.DOUBLE, binders[2].kind());
        assertEquals(ColumnBinder.Kind.LONG, binders[3].kind());

        long[] ids = (long[]) codecs[0].allocate(1);
        int[] counts = (int[]) codecs[1].allocate(1);
        double[] prices = (double[]) codecs[2].allocate(1);
        int[] levels = (int[]) codecs[3].allocate(1);

        NumericPojo row = new NumericPojo(7L, 99, -2.5, 0);
        binders[0].bind(row, ids, 0, null);
        binders[1].bind(row, counts, 0, null);
        binders[2].bind(row, prices, 0, null);
        binders[3].bind(row, levels, 0, null);

        assertEquals(7L, ids[0]);
        assertEquals(99, counts[0]);
        assertEquals(-2.5, prices[0], 0.0);
        assertEquals(0, levels[0]);
    }

    @Test
    void nullableBoxedField_routesThroughNullMap() {
        ColumnCodec<?>[] codecs = {
                new Int64Codec(),
                new NullableColumnCodec(new Int64Codec())
        };
        String[] cols = {"id", "maybe"};
        boolean[] nullable = {false, true};

        ColumnBinder[] binders = RowMappers.columnBinders(
                NullableRecord.class, cols, codecs, nullable);

        // id is a primitive long -> LONG fast path; maybe is nullable -> OBJECT path.
        assertEquals(ColumnBinder.Kind.LONG, binders[0].kind());
        assertEquals(ColumnBinder.Kind.OBJECT, binders[1].kind());

        long[] ids = (long[]) codecs[0].allocate(2);
        Object[] maybes = (Object[]) codecs[1].allocate(2);
        boolean[] maybeNulls = new boolean[2];

        // row 0: maybe = null  -> null-map set, no value written.
        NullableRecord r0 = new NullableRecord(1L, null);
        binders[0].bind(r0, ids, 0, null);
        boolean wrote0 = binders[1].bind(r0, maybes, 0, maybeNulls);
        assertFalse(wrote0);
        assertTrue(maybeNulls[0]);
        assertNull(maybes[0]);

        // row 1: maybe = 55L   -> value written, null-map clear.
        NullableRecord r1 = new NullableRecord(2L, 55L);
        binders[0].bind(r1, ids, 1, null);
        boolean wrote1 = binders[1].bind(r1, maybes, 1, maybeNulls);
        assertTrue(wrote1);
        assertFalse(maybeNulls[1]);
        assertEquals(55L, maybes[1]);

        assertEquals(1L, ids[0]);
        assertEquals(2L, ids[1]);
    }
}
