import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class BucketAccuracy {
    private BucketAccuracy() {}

    public static void main(String[] args) throws IOException {
        Path csvPath;
        if (args.length > 0) {
            csvPath = Paths.get(args[0]);
        } else {
            csvPath = Paths.get("test.csv");
        }
        evaluate(csvPath);
    }

    private static void evaluate(Path csvPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                System.out.println("No rows to evaluate.");
                return;
            }
            System.out.println("Using CSV header: " + header);

            int total = 0;
            int matches = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length < 2) {
                    continue;
                }
                String adid = parts[0].trim();
                String expectedRaw = parts[parts.length - 1].trim();
                if (adid.isEmpty() || expectedRaw.isEmpty()) {
                    continue;
                }
                int expected;
                try {
                    expected = Integer.parseInt(expectedRaw);
                } catch (NumberFormatException ignored) {
                    continue;
                }
                int predicted = bigQueryBucket(adid);
                if (predicted == expected) {
                    matches++;
                }
                total++;
                System.out.println("adid=" + adid + " expected=" + expected + " predicted=" + predicted);
            }

            if (total == 0) {
                System.out.println("No valid rows processed.");
                return;
            }
            double accuracy = (matches * 100.0) / total;
            System.out.printf("%nMatches: %d/%d  Accuracy: %.2f%%%n", matches, total, accuracy);
        }
    }

    private static int bigQueryBucket(String adid) {
        byte[] payload = (adid + "test:1" + "salt-2025").getBytes(StandardCharsets.UTF_8);
        long fingerprint = FarmHash.fingerprint64(payload);
        long absBits = fingerprint >= 0 ? fingerprint : (~fingerprint + 1);
        long remainder = Long.remainderUnsigned(absBits, 100L);
        return (int) remainder;
    }

    private static final class FarmHash {
        private static final long K0 = 0xC3A5C85C97CB3127L;
        private static final long K1 = 0xB492B66FBE98F273L;
        private static final long K2 = 0x9AE16A3B2F90404FL;
        private static final long KMUL = 0x9DDFEA08EB382D69L;

        private FarmHash() {}

        static long fingerprint64(byte[] data) {
            return hash64(data);
        }

        private static long hash64(byte[] data) {
            int length = data.length;
            if (length <= 32) {
                if (length <= 16) {
                    return hashLen0to16(data);
                }
                return hashLen17to32(data);
            }
            if (length <= 64) {
                return hashLen33to64(data);
            }

            long seed = 81L;
            long x = seed;
            long y = seed * K1 + 113L;
            long z = shiftMix(y * K2 + 113L) * K2;
            long[] v = {0L, 0L};
            long[] w = {0L, 0L};
            x = x * K2 + fetch64(data, 0);

            int end = ((length - 1) / 64) * 64;
            int offset = 0;
            while (offset < end) {
                long xRot = Long.rotateRight(x + y + v[0] + fetch64(data, offset + 8), 37);
                x = xRot * K1;
                long yRot = Long.rotateRight(y + v[1] + fetch64(data, offset + 48), 42);
                y = yRot * K1;
                x ^= w[1];
                y += v[0] + fetch64(data, offset + 40);
                long zRot = Long.rotateRight(z + w[0], 33);
                z = zRot * K1;
                v = weakHashLen32WithSeeds(data, offset, v[1] * K1, x + w[0]);
                w = weakHashLen32WithSeeds(data, offset + 32, z + w[1], y + fetch64(data, offset + 16));
                long tmp = x;
                x = z;
                z = tmp;
                offset += 64;
            }

            int last64 = length - 64;
            long mul = K1 + ((z & 0xFFL) << 1);
            long w0 = w[0] + ((length - 1) & 63);
            long v0 = v[0] + w0;
            w0 += v0;
            w[0] = w0;
            v[0] = v0;

            long xRot = Long.rotateRight(x + y + v[0] + fetch64(data, last64 + 8), 37);
            x = xRot * mul;
            long yRot = Long.rotateRight(y + v[1] + fetch64(data, last64 + 48), 42);
            y = yRot * mul;
            x ^= w[1] * 9;
            y += v[0] * 9 + fetch64(data, last64 + 40);
            long zRot = Long.rotateRight(z + w[0], 33);
            z = zRot * mul;
            v = weakHashLen32WithSeeds(data, last64, v[1] * mul, x + w[0]);
            w = weakHashLen32WithSeeds(data, last64 + 32, z + w[1], y + fetch64(data, last64 + 16));
            long tmp = x;
            x = z;
            z = tmp;

            long first = hashLen16(v[0], w[0], mul) + shiftMix(y) * K0 + z;
            long second = hashLen16(v[1], w[1], mul) + x;
            return hashLen16(first, second, mul);
        }

        private static long hashLen0to16(byte[] data) {
            int length = data.length;
            if (length >= 8) {
                long mul = K2 + length * 2L;
                long a = fetch64(data, 0) + K2;
                long b = fetch64(data, length - 8);
                long c = Long.rotateRight(b, 37) * mul + a;
                long d = (Long.rotateRight(a, 25) + b) * mul;
                return hashLen16(c, d, mul);
            }
            if (length >= 4) {
                long mul = K2 + length * 2L;
                long a = fetch32(data, 0);
                long b = fetch32(data, length - 4);
                return hashLen16(length + (a << 3), b, mul);
            }
            if (length > 0) {
                int a = Byte.toUnsignedInt(data[0]);
                int b = Byte.toUnsignedInt(data[length >> 1]);
                int c = Byte.toUnsignedInt(data[length - 1]);
                int y = a + (b << 8);
                int z = length + (c << 2);
                long mix = shiftMix((long) y * K2 ^ (long) z * K0);
                return mix * K2;
            }
            return K2;
        }

        private static long hashLen17to32(byte[] data) {
            int length = data.length;
            long mul = K2 + length * 2L;
            long a = fetch64(data, 0) * K1;
            long b = fetch64(data, 8);
            long c = fetch64(data, length - 8) * mul;
            long d = fetch64(data, length - 16) * K2;
            long first = Long.rotateRight(a + b, 43) + Long.rotateRight(c, 30) + d;
            long second = a + Long.rotateRight(b + K2, 18) + c;
            return hashLen16(first, second, mul);
        }

        private static long hashLen33to64(byte[] data) {
            int length = data.length;
            long mul = K2 + length * 2L;
            long a = fetch64(data, 0) * K2;
            long b = fetch64(data, 8);
            long c = fetch64(data, length - 8) * mul;
            long d = fetch64(data, length - 16) * K2;
            long y = Long.rotateRight(a + b, 43) + Long.rotateRight(c, 30) + d;
            long z = hashLen16(y, a + Long.rotateRight(b + K2, 18) + c, mul);
            long e = fetch64(data, 16) * mul;
            long f = fetch64(data, 24);
            long g = (y + fetch64(data, length - 32)) * mul;
            long h = (z + fetch64(data, length - 24)) * mul;
            long first = Long.rotateRight(e + f, 43) + Long.rotateRight(g, 30) + h;
            long second = e + Long.rotateRight(f + a, 18) + g;
            return hashLen16(first, second, mul);
        }

        private static long[] weakHashLen32WithSeeds(byte[] data, int offset, long a, long b) {
            long w = fetch64(data, offset);
            long x = fetch64(data, offset + 8);
            long y = fetch64(data, offset + 16);
            long z = fetch64(data, offset + 24);
            a += w;
            long bRot = Long.rotateRight(b + a + z, 21);
            long c = a;
            a += x + y;
            long bNew = bRot + Long.rotateRight(a, 44);
            long first = a + z;
            long second = bNew + c;
            return new long[] {first, second};
        }

        private static long hashLen16(long u, long v, long mul) {
            long a = (u ^ v) * mul;
            a ^= a >>> 47;
            long b = (v ^ a) * mul;
            b ^= b >>> 47;
            b *= mul;
            return b;
        }

        private static long shiftMix(long value) {
            return value ^ (value >>> 47);
        }

        private static long fetch64(byte[] data, int offset) {
            return ((long) data[offset] & 0xFF)
                | (((long) data[offset + 1] & 0xFF) << 8)
                | (((long) data[offset + 2] & 0xFF) << 16)
                | (((long) data[offset + 3] & 0xFF) << 24)
                | (((long) data[offset + 4] & 0xFF) << 32)
                | (((long) data[offset + 5] & 0xFF) << 40)
                | (((long) data[offset + 6] & 0xFF) << 48)
                | (((long) data[offset + 7] & 0xFF) << 56);
        }

        private static long fetch32(byte[] data, int offset) {
            return ((long) data[offset] & 0xFF)
                | (((long) data[offset + 1] & 0xFF) << 8)
                | (((long) data[offset + 2] & 0xFF) << 16)
                | (((long) data[offset + 3] & 0xFF) << 24);
        }
    }
}
