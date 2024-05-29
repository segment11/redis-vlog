package redis.mock;

import io.netty.buffer.ByteBuf;
import redis.CompressedValue;

public interface ByPassGetSet {
    void put(byte slot, String key, int bucketIndex, CompressedValue cv);

    ByteBuf getBuf(byte slot, byte[] keyBytes, int bucketIndex, long keyHash);
}
