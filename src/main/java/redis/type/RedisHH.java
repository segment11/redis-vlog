package redis.type;

import redis.KeyHash;

import java.nio.ByteBuffer;
import java.util.HashMap;

// key / value save together
public class RedisHH {
    public static final short PREFER_LESS_THAN_VALUE_LENGTH = 4096;

    public static final int PREFER_COMPRESS_FIELD_VALUE_LENGTH = 64;

    public static final byte[] PREFER_COMPRESS_FIELD_MAGIC_PREFIX = "r?h!h".getBytes();

    // hash size short + crc int
    private static final int HEADER_LENGTH = 2 + 4;

    private final HashMap<String, byte[]> map = new HashMap<>();

    public HashMap<String, byte[]> getMap() {
        return map;
    }

    public int size() {
        return map.size();
    }

    public void put(String key, byte[] value) {
        map.put(key, value);
    }

    public void putAll(HashMap<String, byte[]> map) {
        this.map.putAll(map);
    }

    public byte[] get(String key) {
        return map.get(key);
    }

    public byte[] encode() {
        int len = 0;
        for (var entry : map.entrySet()) {
            // key / value length use 2 bytes
            var key = entry.getKey();
            var value = entry.getValue();
            len += 2 + key.length() + 2 + value.length;
        }

        var buffer = ByteBuffer.allocate(len + HEADER_LENGTH);
        buffer.putShort((short) map.size());
        // tmp crc
        buffer.putInt(0);
        for (var entry : map.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            buffer.putShort((short) key.length());
            buffer.put(key.getBytes());
            buffer.putShort((short) value.length);
            buffer.put(value);
        }

        // crc
        if (len > 0) {
            var hb = buffer.array();
            int crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(2, crc);
        }

        return buffer.array();
    }

    public static RedisHH decode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();
        int crc = buffer.getInt();

        // check crc
        if (size > 0) {
            int crcCompare = KeyHash.hash32Offset(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        var r = new RedisHH();
        for (int i = 0; i < size; i++) {
            int keyLength = buffer.getShort();
            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.getShort();
            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            r.map.put(new String(keyBytes), valueBytes);
        }
        return r;
    }
}
