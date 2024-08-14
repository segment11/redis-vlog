package redis.mock;

import redis.CompressedValue;
import redis.persist.OneSlot;

import java.util.HashMap;

public class InMemoryGetSet implements ByPassGetSet {
    private final HashMap<String, CompressedValue> map = new HashMap<>();

    @Override
    public void put(byte slot, String key, int bucketIndex, CompressedValue cv) {
        map.put(key, cv);
    }

    @Override
    public boolean remove(byte slot, String key) {
        return map.remove(key) != null;
    }

    @Override
    public OneSlot.BufOrCompressedValue getBuf(byte slot, byte[] keyBytes, int bucketIndex, long keyHash) {
        var cv = map.get(new String(keyBytes));
        if (cv == null) {
            return null;
        }

        return new OneSlot.BufOrCompressedValue(null, cv);
    }
}
