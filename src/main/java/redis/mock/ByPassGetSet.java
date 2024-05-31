package redis.mock;

import redis.CompressedValue;
import redis.persist.OneSlot;

public interface ByPassGetSet {
    void put(byte slot, String key, int bucketIndex, CompressedValue cv);

    OneSlot.BufOrCompressedValue getBuf(byte slot, byte[] keyBytes, int bucketIndex, long keyHash);
}
