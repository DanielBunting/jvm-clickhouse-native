package io.github.danielbunting.clickhouse.compress

/**
 * ClickHouse's frozen variant of Google's CityHash, producing a 128-bit hash.
 *
 * ClickHouse computes a CityHash128 checksum over the (method-marker +
 * compressed-size + decompressed-size + compressed-data) payload and prepends
 * the 16-byte result to every compressed block on the native wire protocol.
 * The server rejects any block whose checksum does not match, so this
 * implementation must reproduce ClickHouse's checksum byte-for-byte.
 *
 * **Important:** ClickHouse froze an *older* revision of CityHash
 * (the pre-v1.1 layout, as it shipped in `contrib/cityhash102`) and never
 * upgraded it, to keep on-disk and on-wire checksums stable across versions.
 * This class is a faithful port of that frozen revision (matching the CH.Native
 * .NET `CityHash` port), **not** upstream CityHash v1.1. The two
 * differ in `cityHash128WithSeed` and `cityMurmur`, so do not
 * substitute a stock CityHash library.
 *
 * All arithmetic is performed on 64-bit `Long` values interpreted as
 * unsigned where the original algorithm uses `uint64`. Multibyte loads
 * are little-endian, matching the x86-64 platforms ClickHouse targets.
 *
 * This class is stateless and thread-safe; all methods are static.
 */
public object CityHash128 {

    /** Some primes between 2^63 and 2^64 used by the CityHash mixing steps. */
    private const val K0 = -0x3c5a37a36834ced9L // 0xc3a5c85c97cb3127
    private const val K1 = -0x4b6d499041670d8dL // 0xb492b66fbe98f273
    private const val K2 = -0x651e95c4d06fbfb1L // 0x9ae16a3b2f90404f
    private const val K3 = -0x36b62838af619aa9L // 0xc949d7c7509e6557

    private val EMPTY = ByteArray(0)

    /**
     * Computes ClickHouse's 128-bit CityHash over [len] bytes of
     * [data] starting at [off].
     *
     * The returned 16 bytes are laid out as ClickHouse writes them: the low
     * 64 bits of the hash first, then the high 64 bits, each in little-endian
     * byte order. ClickHouse stores the checksum as two consecutive
     * little-endian `uint64` words (low word, then high word), which is
     * exactly this layout.
     *
     * @param data backing byte array (not modified)
     * @param off  start offset into [data]
     * @param len  number of bytes to hash
     * @return a fresh 16-byte array containing the checksum
     * @throws IndexOutOfBoundsException if `[off, off+len)` is out of range
     * @throws NullPointerException      if [data] is null
     */
    // VERIFY against CH.Native: output byte order is low 64 then high 64,
    // each little-endian. ClickHouse's CityHash128 type stores {low, high} as
    // two little-endian uint64 words, so this is the on-wire layout.
    @JvmStatic
    public fun hash128(data: ByteArray, off: Int, len: Int): ByteArray {
        if (off < 0 || len < 0 || off + len > data.size) {
            throw IndexOutOfBoundsException("off=$off len=$len arrayLength=${data.size}")
        }

        val hl = cityHash128(data, off, len)
        val low = hl[0]
        val high = hl[1]

        val out = ByteArray(16)
        writeLongLE(out, 0, low)
        writeLongLE(out, 8, high)
        return out
    }

    // ----------------------------------------------------------------------
    // Little-endian loads / stores.
    // ----------------------------------------------------------------------

    private fun fetch64(p: ByteArray, i: Int): Long {
        return (p[i].toLong() and 0xffL) or
            ((p[i + 1].toLong() and 0xffL) shl 8) or
            ((p[i + 2].toLong() and 0xffL) shl 16) or
            ((p[i + 3].toLong() and 0xffL) shl 24) or
            ((p[i + 4].toLong() and 0xffL) shl 32) or
            ((p[i + 5].toLong() and 0xffL) shl 40) or
            ((p[i + 6].toLong() and 0xffL) shl 48) or
            ((p[i + 7].toLong() and 0xffL) shl 56)
    }

    private fun fetch32(p: ByteArray, i: Int): Long {
        return (p[i].toLong() and 0xffL) or
            ((p[i + 1].toLong() and 0xffL) shl 8) or
            ((p[i + 2].toLong() and 0xffL) shl 16) or
            ((p[i + 3].toLong() and 0xffL) shl 24)
    }

    private fun writeLongLE(out: ByteArray, off: Int, v: Long) {
        out[off] = v.toByte()
        out[off + 1] = (v ushr 8).toByte()
        out[off + 2] = (v ushr 16).toByte()
        out[off + 3] = (v ushr 24).toByte()
        out[off + 4] = (v ushr 32).toByte()
        out[off + 5] = (v ushr 40).toByte()
        out[off + 6] = (v ushr 48).toByte()
        out[off + 7] = (v ushr 56).toByte()
    }

    // ----------------------------------------------------------------------
    // 64-bit mixing primitives.
    // ----------------------------------------------------------------------

    private fun rotate(`val`: Long, shift: Int): Long {
        // Avoid shifting by 64 (UB in C, well-defined but unwanted here).
        return if (shift == 0) `val` else java.lang.Long.rotateRight(`val`, shift)
    }

    /**
     * The frozen CityHash `Rotate` used inside `HashLen0to16` for
     * lengths 8..16. Upstream later replaced this with the unconditional
     * `Rotate`, but the frozen revision keeps both spellings identical.
     */
    private fun rotateByAtLeast1(`val`: Long, shift: Int): Long {
        return java.lang.Long.rotateRight(`val`, shift)
    }

    private fun shiftMix(`val`: Long): Long {
        return `val` xor (`val` ushr 47)
    }

    private fun hash128to64(lo: Long, hi: Long): Long {
        // Murmur-inspired hashing of a 128-bit value into 64 bits.
        val kMul = -0x622015f714c7d297L // 0x9ddfea08eb382d69
        var a = (lo xor hi) * kMul
        a = a xor (a ushr 47)
        var b = (hi xor a) * kMul
        b = b xor (b ushr 47)
        b *= kMul
        return b
    }

    private fun hashLen16(u: Long, v: Long): Long {
        return hash128to64(u, v)
    }

    // ----------------------------------------------------------------------
    // Short-input paths.
    // ----------------------------------------------------------------------

    private fun hashLen0to16(s: ByteArray, off: Int, len: Int): Long {
        if (len > 8) {
            val a = fetch64(s, off)
            val b = fetch64(s, off + len - 8)
            return hashLen16(a, rotateByAtLeast1(b + len, len)) xor b
        }
        if (len >= 4) {
            val a = fetch32(s, off)
            return hashLen16(len + (a shl 3), fetch32(s, off + len - 4))
        }
        if (len > 0) {
            val a = s[off].toInt() and 0xff
            val b = s[off + (len ushr 1)].toInt() and 0xff
            val c = s[off + len - 1].toInt() and 0xff
            val y = a + (b shl 8)
            val z = len + (c shl 2)
            return shiftMix((y * K2) xor (z * K3)) * K2
        }
        return K2
    }

    private fun hashLen17to32(s: ByteArray, off: Int, len: Int): Long {
        val a = fetch64(s, off) * K1
        val b = fetch64(s, off + 8)
        val c = fetch64(s, off + len - 8) * K2
        val d = fetch64(s, off + len - 16) * K0
        return hashLen16(
            rotate(a - b, 43) + rotate(c, 30) + d,
            a + rotate(b xor K3, 20) - c + len
        )
    }

    // ----------------------------------------------------------------------
    // 32-byte block helper used by the long-input path. Returns a pair.
    // ----------------------------------------------------------------------

    private fun weakHashLen32WithSeeds(
        w: Long, x: Long, y: Long, z: Long, aIn: Long, bIn: Long,
    ): LongArray {
        var a = aIn
        var b = bIn
        a += w
        b = rotate(b + a + z, 21)
        val c = a
        a += x
        a += y
        b += rotate(a, 44)
        return longArrayOf(a + z, b + c)
    }

    private fun weakHashLen32WithSeeds(s: ByteArray, off: Int, a: Long, b: Long): LongArray {
        return weakHashLen32WithSeeds(
            fetch64(s, off),
            fetch64(s, off + 8),
            fetch64(s, off + 16),
            fetch64(s, off + 24),
            a,
            b
        )
    }

    private fun hashLen33to64(s: ByteArray, off: Int, len: Int): Long {
        var z = fetch64(s, off + 24)
        var a = fetch64(s, off) + (len + fetch64(s, off + len - 16)) * K0
        var b = rotate(a + z, 52)
        var c = rotate(a, 37)
        a += fetch64(s, off + 8)
        c += rotate(a, 7)
        a += fetch64(s, off + 16)
        val vf = a + z
        val vs = b + rotate(a, 31) + c

        a = fetch64(s, off + 16) + fetch64(s, off + len - 32)
        z = fetch64(s, off + len - 8)
        b = rotate(a + z, 52)
        c = rotate(a, 37)
        a += fetch64(s, off + len - 24)
        c += rotate(a, 7)
        a += fetch64(s, off + len - 16)
        val wf = a + z
        val ws = b + rotate(a, 31) + c

        val r = shiftMix((vf + ws) * K2 + (wf + vs) * K0)
        return shiftMix(r * K0 + vs) * K2
    }

    // ----------------------------------------------------------------------
    // 128-bit paths.
    // ----------------------------------------------------------------------

    /**
     * CityMurmur, the frozen revision. Used for inputs shorter than 128 bytes
     * (and as the seed-mixing step). This is the layout ClickHouse froze; it
     * differs from upstream v1.1.
     */
    private fun cityMurmur(s: ByteArray, off: Int, len: Int, seedLow: Long, seedHigh: Long): LongArray {
        var a = seedLow
        var b = seedHigh
        var c: Long
        var d: Long
        var l = len - 16
        if (l <= 0) {
            // len <= 16
            a = shiftMix(a * K1) * K1
            c = b * K1 + hashLen0to16(s, off, len)
            d = shiftMix(a + (if (len >= 8) fetch64(s, off) else c))
        } else {
            // len > 16
            c = hashLen16(fetch64(s, off + len - 8) + K1, a)
            d = hashLen16(b + len, c + fetch64(s, off + len - 16))
            a += d
            var p = off
            do {
                a = a xor (shiftMix(fetch64(s, p) * K1) * K1)
                a *= K1
                b = b xor a
                c = c xor (shiftMix(fetch64(s, p + 8) * K1) * K1)
                c *= K1
                d = d xor c
                p += 16
                l -= 16
            } while (l > 0)
        }
        a = hashLen16(a, c)
        b = hashLen16(d, b)
        return longArrayOf(a xor b, hashLen16(b, a))
    }

    private fun cityHash128WithSeed(
        s: ByteArray, off: Int, len: Int, seedLow: Long, seedHigh: Long,
    ): LongArray {
        if (len < 128) {
            return cityMurmur(s, off, len, seedLow, seedHigh)
        }

        // We expect len >= 128 to be the common case. Keep 56 bytes of state:
        // v, w, x, y, and z.
        var v = LongArray(2)
        var w = LongArray(2)
        var x = seedLow
        var y = seedHigh
        var z = len * K1
        v[0] = rotate(y xor K1, 49) * K1 + fetch64(s, off)
        v[1] = rotate(v[0], 42) * K1 + fetch64(s, off + 8)
        w[0] = rotate(y + z, 35) * K1 + x
        w[1] = rotate(x + fetch64(s, off + 88), 53) * K1

        // This is the same inner loop as CityHash64(), manually unrolled to
        // process pairs of 64-byte chunks.
        var p = off
        var remaining = len
        do {
            x = rotate(x + y + v[0] + fetch64(s, p + 16), 37) * K1
            y = rotate(y + v[1] + fetch64(s, p + 48), 42) * K1
            x = x xor w[1]
            y = y xor v[0]
            z = rotate(z xor w[0], 33)
            v = weakHashLen32WithSeeds(s, p, v[1] * K1, x + w[0])
            w = weakHashLen32WithSeeds(s, p + 32, z + w[1], y)
            run { val t = z; z = x; x = t }
            p += 64

            x = rotate(x + y + v[0] + fetch64(s, p + 16), 37) * K1
            y = rotate(y + v[1] + fetch64(s, p + 48), 42) * K1
            x = x xor w[1]
            y = y xor v[0]
            z = rotate(z xor w[0], 33)
            v = weakHashLen32WithSeeds(s, p, v[1] * K1, x + w[0])
            w = weakHashLen32WithSeeds(s, p + 32, z + w[1], y)
            run { val t = z; z = x; x = t }
            p += 64
            remaining -= 128
        } while (remaining >= 128)

        y += rotate(w[0], 37) * K0 + z
        x += rotate(v[0] + z, 49) * K0

        // If 0 < remaining < 128, hash up to 4 chunks of 32 bytes each from
        // the end of s.
        var tailDone = 0
        while (tailDone < remaining) {
            tailDone += 32
            y = rotate(y - x, 42) * K0 + v[1]
            w[0] += fetch64(s, p + remaining - tailDone + 16)
            x = rotate(x, 49) * K0 + w[0]
            w[0] += v[0]
            v = weakHashLen32WithSeeds(s, p + remaining - tailDone, v[0], v[1])
        }

        // At this point our 48 bytes of state should contain more than enough
        // information for a strong 128-bit hash. We use two different
        // 48-byte-to-8-byte hashes to get a 16-byte final result.
        x = hashLen16(x, v[0])
        y = hashLen16(y, w[0])
        return longArrayOf(
            hashLen16(x + v[1], w[1]) + y,
            hashLen16(x + w[1], y + v[1])
        )
    }

    private fun cityHash128(s: ByteArray, off: Int, len: Int): LongArray {
        if (len >= 16) {
            return cityHash128WithSeed(
                s, off + 16, len - 16,
                fetch64(s, off) xor K3,
                fetch64(s, off + 8)
            )
        }
        if (len >= 8) {
            return cityHash128WithSeed(
                EMPTY, 0, 0,
                fetch64(s, off) xor (len * K0),
                fetch64(s, off + len - 8) xor K1
            )
        }
        return cityHash128WithSeed(s, off, len, K0, K1)
    }
}
