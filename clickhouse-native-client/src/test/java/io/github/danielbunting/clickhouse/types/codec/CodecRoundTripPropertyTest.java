package io.github.danielbunting.clickhouse.types.codec;

import io.github.danielbunting.clickhouse.protocol.BinaryReader;
import io.github.danielbunting.clickhouse.testutil.Bytes;
import io.github.danielbunting.clickhouse.types.CodecRegistry;
import io.github.danielbunting.clickhouse.types.ColumnCodec;
import io.github.danielbunting.clickhouse.types.DefaultCodecRegistry;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-based round-trip test across the codec matrix.
 *
 * <p>Where {@code GoldenByteVectorsTest} pins <em>enumerated</em> values to exact bytes, this
 * suite explores the value <em>space</em>: for each type it generates many random rows, drives
 * them through the column codec's full {@code set → writeStatePrefix → write → readStatePrefix
 * → read → get} cycle, and asserts the decoded value equals what was generated. Running this on
 * every build catches encode/decode bugs (endianness, offset arithmetic, dictionary key widths,
 * tuple/array framing) that hand-picked cases miss — and keeps catching them as codecs change.
 *
 * <p>Codecs are resolved through the {@link CodecRegistry} from their CH type string, so the
 * type parser and the composite codec tree are exercised too. Generators produce values in the
 * <b>canonical decoded form</b> (e.g. a {@code BigDecimal} already at the column scale, an
 * {@code Instant} already truncated to the {@code DateTime64} tick, a {@code FixedString} with
 * no trailing NUL), so {@code get} on the decoded column equals the generated value exactly.
 *
 * <p>Nulls are not exercised here: {@code Nullable(T)} is a block-layer concern (the codec sees
 * the unwrapped {@code T}), covered by {@code BlockCodecTest}/{@code BlockNullMapTest}. The
 * lossy {@code BFloat16} type is excluded (its {@code set} narrows, so a random float is not its
 * own round-trip); it is covered by {@code BFloat16TypesIT}.
 *
 * <p>The RNG is seeded deterministically per type, so any failure reproduces exactly.
 */
class CodecRoundTripPropertyTest {

    private static final int ITERATIONS = 200;
    private static final int MAX_ROWS = 32;
    private static final long BASE_SEED = 0xC0FFEEL;

    private static final CodecRegistry REGISTRY = new DefaultCodecRegistry();

    @FunctionalInterface
    private interface ValueGen {
        Object gen(Random r);
    }

    /** One type under test: its CH type string and a canonical-value generator. */
    private static final class Case {
        final String type;
        final ValueGen gen;
        Case(String type, ValueGen gen) {
            this.type = type;
            this.gen = gen;
        }
    }

    @TestFactory
    Stream<DynamicTest> roundTripHoldsForEveryType() {
        return cases().stream().map(c ->
                DynamicTest.dynamicTest(c.type, () -> runCase(c)));
    }

    private static void runCase(Case c) {
        ColumnCodec<?> codec = REGISTRY.resolve(c.type);
        Random r = new Random(BASE_SEED ^ c.type.hashCode());
        for (int iter = 0; iter < ITERATIONS; iter++) {
            int n = r.nextInt(MAX_ROWS + 1);
            List<Object> values = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                values.add(c.gen.gen(r));
            }
            List<Object> decoded = roundTrip(codec, values);
            assertEquals(n, decoded.size(), () -> "row count mismatch for " + c.type);
            for (int i = 0; i < n; i++) {
                final int row = i;
                final int it = iter;
                assertEquals(values.get(i), decoded.get(i),
                        () -> c.type + " mismatch at iter " + it + " row " + row);
            }
        }
    }

    /** set → writeStatePrefix+write → readStatePrefix+read → get, returning the decoded values. */
    @SuppressWarnings("unchecked")
    private static List<Object> roundTrip(ColumnCodec<?> raw, List<Object> values) {
        ColumnCodec<Object> codec = (ColumnCodec<Object>) raw;
        int n = values.size();

        Object src = codec.allocate(n);
        for (int i = 0; i < n; i++) {
            codec.set(src, i, values.get(i));
        }

        byte[] wire = Bytes.capture(w -> {
            codec.writeStatePrefix(w);
            codec.write(w, src, n);
        });

        Object dst = codec.allocate(n);
        BinaryReader rd = Bytes.reader(wire);
        try {
            codec.readStatePrefix(rd);
            codec.read(rd, n, dst);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        List<Object> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(codec.get(dst, i));
        }
        return out;
    }

    // =====================================================================
    // The type matrix.
    // =====================================================================

    private static List<Case> cases() {
        List<Case> cases = new ArrayList<>();

        // --- signed / unsigned integers ---
        cases.add(new Case("Int8", r -> (byte) r.nextInt()));
        cases.add(new Case("Int16", r -> (short) r.nextInt()));
        cases.add(new Case("Int32", Random::nextInt));
        cases.add(new Case("Int64", Random::nextLong));
        cases.add(new Case("UInt8", r -> r.nextInt(256)));
        cases.add(new Case("UInt16", r -> r.nextInt(65536)));
        cases.add(new Case("UInt32", r -> r.nextLong() & 0xFFFFFFFFL));
        cases.add(new Case("UInt64", Random::nextLong)); // raw bits; get returns the Long bits

        // --- wide integers ---
        cases.add(new Case("Int128", r -> signedWide(r, 16)));
        cases.add(new Case("Int256", r -> signedWide(r, 32)));
        cases.add(new Case("UInt128", r -> unsignedWide(r, 16)));
        cases.add(new Case("UInt256", r -> unsignedWide(r, 32)));

        // --- floats / bool ---
        cases.add(new Case("Float32", CodecRoundTripPropertyTest::randomFloat));
        cases.add(new Case("Float64", CodecRoundTripPropertyTest::randomDouble));
        cases.add(new Case("Bool", Random::nextBoolean));

        // --- strings ---
        cases.add(new Case("String", r -> randomString(r, 24)));
        cases.add(new Case("FixedString(8)", r -> randomAscii(r, 8)));

        // --- decimals (generated already at the column scale) ---
        cases.add(new Case("Decimal(9, 2)", decimalGen(9, 2)));
        cases.add(new Case("Decimal(18, 6)", decimalGen(18, 6)));
        cases.add(new Case("Decimal(38, 10)", decimalGen(38, 10)));
        cases.add(new Case("Decimal(76, 20)", decimalGen(76, 20)));

        // --- date / time ---
        cases.add(new Case("Date", r -> LocalDate.ofEpochDay(r.nextInt(65536))));
        cases.add(new Case("Date32", r -> LocalDate.ofEpochDay(-25_567 + r.nextInt(145_567))));
        cases.add(new Case("DateTime", r -> Instant.ofEpochSecond(r.nextLong() & 0xFFFFFFFFL)));
        cases.add(new Case("DateTime64(3)", dateTime64Gen(3)));
        cases.add(new Case("DateTime64(9)", dateTime64Gen(9)));
        cases.add(new Case("Time64(3)", time64Gen(3)));

        // --- uuid / ip ---
        cases.add(new Case("UUID", r -> new UUID(r.nextLong(), r.nextLong())));
        cases.add(new Case("IPv4", r -> inet(randomBytes(r, 4))));
        cases.add(new Case("IPv6", CodecRoundTripPropertyTest::randomIpv6));

        // --- enums (generate from the declared name set) ---
        String[] enum8Names = {"red", "green", "blue"};
        cases.add(new Case("Enum8('red' = 1, 'green' = 2, 'blue' = 3)",
                r -> enum8Names[r.nextInt(enum8Names.length)]));
        String[] enum16Names = {"a", "b", "c"};
        cases.add(new Case("Enum16('a' = -5, 'b' = 1000, 'c' = 32000)",
                r -> enum16Names[r.nextInt(enum16Names.length)]));

        // --- composites ---
        cases.add(new Case("Array(Int32)", r -> randomList(r, Random::nextInt, 8)));
        cases.add(new Case("Array(String)", r -> randomList(r, rr -> randomString(rr, 12), 8)));
        cases.add(new Case("Array(Array(Int32))",
                r -> randomList(r, rr -> randomList(rr, Random::nextInt, 5), 5)));
        cases.add(new Case("Map(String, Int32)",
                r -> randomMap(r, rr -> randomString(rr, 8), Random::nextInt, 8)));
        cases.add(new Case("Map(Int32, String)",
                r -> randomMap(r, Random::nextInt, rr -> randomString(rr, 8), 8)));
        cases.add(new Case("Map(String, Array(Int32))",
                r -> randomMap(r, rr -> randomString(rr, 8), rr -> randomList(rr, Random::nextInt, 5), 6)));
        cases.add(new Case("Tuple(Int32, String, Float64)",
                r -> List.of(r.nextInt(), randomString(r, 10), randomDouble(r))));

        // --- low cardinality (small value pool to exercise the dictionary + key width) ---
        String[] lcPool = {"alpha", "beta", "gamma", "delta", "epsilon"};
        cases.add(new Case("LowCardinality(String)", r -> lcPool[r.nextInt(lcPool.length)]));

        return cases;
    }

    // =====================================================================
    // Generators (all produce canonical decoded-form values).
    // =====================================================================

    private static Float randomFloat(Random r) {
        switch (r.nextInt(16)) {
            case 0: return Float.NaN;
            case 1: return Float.POSITIVE_INFINITY;
            case 2: return Float.NEGATIVE_INFINITY;
            case 3: return 0.0f;
            case 4: return -0.0f;
            case 5: return Float.MAX_VALUE;
            case 6: return Float.MIN_VALUE;
            default: return (float) ((r.nextDouble() - 0.5) * 2.0e6);
        }
    }

    private static Double randomDouble(Random r) {
        switch (r.nextInt(16)) {
            case 0: return Double.NaN;
            case 1: return Double.POSITIVE_INFINITY;
            case 2: return Double.NEGATIVE_INFINITY;
            case 3: return 0.0;
            case 4: return -0.0;
            case 5: return Double.MAX_VALUE;
            case 6: return Double.MIN_VALUE;
            default: return (r.nextDouble() - 0.5) * 2.0e12;
        }
    }

    private static final char[] STRING_POOL =
            "abcXYZ012 _-/\\\n\t\"'é€中".toCharArray(); // includes quote/backslash/newline/multibyte
    private static final char[] ASCII_POOL = "abcdefXYZ0123456789".toCharArray();

    private static String randomString(Random r, int maxLen) {
        int len = r.nextInt(maxLen + 1);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(STRING_POOL[r.nextInt(STRING_POOL.length)]);
        }
        return sb.toString();
    }

    /** ASCII-only, length &le; maxLen, no trailing NUL: round-trips through FixedString padding. */
    private static String randomAscii(Random r, int maxLen) {
        int len = r.nextInt(maxLen + 1);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(ASCII_POOL[r.nextInt(ASCII_POOL.length)]);
        }
        return sb.toString();
    }

    /** Signed wide integer with magnitude &lt; 2^(bytes*8-1), within the type's range. */
    private static BigInteger signedWide(Random r, int bytes) {
        BigInteger m = new BigInteger(bytes * 8 - 1, r); // [0, 2^(bits-1)-1]
        return r.nextBoolean() ? m : m.negate();
    }

    /** Unsigned wide integer in [0, 2^(bytes*8)-1]. */
    private static BigInteger unsignedWide(Random r, int bytes) {
        return new BigInteger(bytes * 8, r);
    }

    private static ValueGen decimalGen(int precision, int scale) {
        // Magnitude bits chosen so |unscaled| < 10^precision (10^p ~= 2^(3.3219*p)).
        int magBits = Math.max(1, (int) Math.floor(precision * 3.321928) - 1);
        return r -> {
            BigInteger unscaled = new BigInteger(magBits, r);
            if (r.nextBoolean()) {
                unscaled = unscaled.negate();
            }
            return new BigDecimal(unscaled, scale);
        };
    }

    private static ValueGen dateTime64Gen(int precision) {
        long ticksPerSecond = pow10(precision);
        long nanosPerTick = pow10(9 - precision);
        return r -> {
            long epochSecond = r.nextInt(); // modest signed range incl. pre-epoch; *1e9 fits long
            long subTick = (long) (r.nextDouble() * ticksPerSecond); // [0, ticksPerSecond)
            return Instant.ofEpochSecond(epochSecond, subTick * nanosPerTick);
        };
    }

    private static ValueGen time64Gen(int precision) {
        long ticksPerSecond = pow10(precision);
        long nanosPerTick = pow10(9 - precision);
        return r -> {
            long seconds = r.nextInt(86_400); // within a day, non-negative
            long subTick = (long) (r.nextDouble() * ticksPerSecond);
            return Duration.ofSeconds(seconds, subTick * nanosPerTick);
        };
    }

    private static List<Object> randomList(Random r, ValueGen element, int maxSize) {
        int n = r.nextInt(maxSize + 1);
        List<Object> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(element.gen(r));
        }
        return list;
    }

    private static Map<Object, Object> randomMap(Random r, ValueGen keyGen, ValueGen valGen, int maxSize) {
        int target = r.nextInt(maxSize + 1);
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        int guard = 0;
        while (map.size() < target && guard++ < target * 4 + 4) {
            Object k = keyGen.gen(r);
            if (!map.containsKey(k)) {
                map.put(k, valGen.gen(r));
            }
        }
        return map;
    }

    private static byte[] randomBytes(Random r, int n) {
        byte[] b = new byte[n];
        r.nextBytes(b);
        return b;
    }

    private static InetAddress randomIpv6(Random r) {
        byte[] b = randomBytes(r, 16);
        b[0] = 0x20; // force a genuine global-unicast IPv6 (not IPv4-mapped / unspecified)
        return inet(b);
    }

    private static InetAddress inet(byte[] addr) {
        try {
            return InetAddress.getByAddress(addr);
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e); // only thrown for illegal length; 4/16 are valid
        }
    }

    private static long pow10(int e) {
        long v = 1;
        for (int i = 0; i < e; i++) {
            v *= 10;
        }
        return v;
    }
}
