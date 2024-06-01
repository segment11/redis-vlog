package redis;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

public class KeyHash {
    private static final XXHash64 xxHash64Java = XXHashFactory.fastestJavaInstance().hash64();
    private static final XXHash32 xxHash32Java = XXHashFactory.fastestJavaInstance().hash32();

    private static final long seed = 0x9747b28c;
    private static final int seed32 = 0x9747b28c;

    public static long hash(byte[] keyBytes) {
        return xxHash64Java.hash(keyBytes, 0, keyBytes.length, seed);
    }

    public static long hashOffset(byte[] keyBytes, int offset, int length) {
        return xxHash64Java.hash(keyBytes, offset, length, seed);
    }

    public static int hash32(byte[] keyBytes) {
        return xxHash32Java.hash(keyBytes, 0, keyBytes.length, seed32);
    }

    public static int hash32Offset(byte[] keyBytes, int offset, int length) {
        return xxHash32Java.hash(keyBytes, offset, length, seed32);
    }

    // for split index re-calculating, avoid data skew
    public static byte splitIndex(long keyHash, byte splitNumber, int bucketIndex) {
        if (splitNumber == 1) {
            return 0;
        }
        return (byte) Math.abs(((keyHash >> 32) % splitNumber));
    }
}
