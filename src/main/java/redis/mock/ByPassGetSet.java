package redis.mock;

import redis.CompressedValue;
import redis.persist.OneSlot;

public interface ByPassGetSet {
    void put(byte slot, String key, int bucketIndex, CompressedValue cv);

    boolean remove(byte slot, String key);

    OneSlot.BufOrCompressedValue getBuf(byte slot, byte[] keyBytes, int bucketIndex, long keyHash);
}
