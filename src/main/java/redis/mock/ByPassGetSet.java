package redis.mock;

import redis.CompressedValue;
import redis.persist.OneSlot;

public interface ByPassGetSet {
    void put(short slot, String key, int bucketIndex, CompressedValue cv);

    boolean remove(short slot, String key);

    OneSlot.BufOrCompressedValue getBuf(short slot, byte[] keyBytes, int bucketIndex, long keyHash);
}
