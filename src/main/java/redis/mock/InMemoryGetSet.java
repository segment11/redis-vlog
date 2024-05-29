package redis.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import redis.CompressedValue;

import java.util.HashMap;

public class InMemoryGetSet implements ByPassGetSet {
    private HashMap<String, CompressedValue> map = new HashMap<>();

    @Override
    public void put(byte slot, String key, int bucketIndex, CompressedValue cv) {
        map.put(key, cv);
    }

    @Override
    public ByteBuf getBuf(byte slot, byte[] keyBytes, int bucketIndex, long keyHash) {
        var cv = map.get(new String(keyBytes));
        if (cv == null) {
            return null;
        }

        var encoded = cv.encode();
        return Unpooled.wrappedBuffer(encoded);
    }
}
