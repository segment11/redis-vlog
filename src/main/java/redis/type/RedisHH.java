package redis.type;

import redis.CompressedValue;
import redis.KeyHash;

import java.nio.ByteBuffer;
import java.util.HashMap;

// key / value save together
public class RedisHH {
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
        if (key.length() > CompressedValue.KEY_MAX_LENGTH) {
            throw new IllegalArgumentException("Key length too long, key length: " + key.length());
        }
        if (value.length > CompressedValue.VALUE_MAX_LENGTH) {
            throw new IllegalArgumentException("Value length too long, value length: " + value.length);
        }
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
        return decode(data, true);
    }

    public static RedisHH decode(byte[] data, boolean doCheckCrc32) {
        var buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();
        int crc = buffer.getInt();

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        var r = new RedisHH();
        for (int i = 0; i < size; i++) {
            int keyLength = buffer.getShort();
            if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
                throw new IllegalStateException("Key length error, key length: " + keyLength);
            }

            var keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            var valueLength = buffer.getShort();
            if (valueLength <= 0) {
                throw new IllegalStateException("Value length error, value length: " + valueLength);
            }

            var valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            r.map.put(new String(keyBytes), valueBytes);
        }
        return r;
    }
}
