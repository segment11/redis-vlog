package redis.type;

import redis.KeyHash;

import java.nio.ByteBuffer;
import java.util.HashSet;

// key save together, one field value save as single key
public class RedisHashKeys {
    // change here to limit hash size
    // keys encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose key length is 32, then 16KB / 32 = 512
    public static final short HASH_MAX_SIZE = 4096;

    public static final int SET_MEMBER_MAX_LENGTH = 255;

    // hash size short + crc int
    private static final int HEADER_LENGTH = 2 + 4;

    // may be length > CompressedValue.KEY_MAX_LENGTH
    public static String keysKey(String key) {
        // add hashtag to make sure all keys in one slot
        return "h_k_{" + key + "}";
    }

    // may be length > CompressedValue.KEY_MAX_LENGTH
    public static String fieldKey(String key, String field) {
        // add hashtag to make sure all keys in one slot
        // add . to make sure same key use the same dict when compress
        return "h_f_" + "{" + key + "}." + field;
    }

    private final HashSet<String> set = new HashSet<>();

    public HashSet<String> getSet() {
        return set;
    }

    public int size() {
        return set.size();
    }

    public boolean contains(String field) {
        return set.contains(field);
    }

    public boolean remove(String field) {
        return set.remove(field);
    }

    public boolean add(String field) {
        return set.add(field);
    }

    public byte[] encode() {
        int len = 0;
        for (var e : set) {
            // key length use 2 bytes short
            len += 2 + e.length();
        }

        var buffer = ByteBuffer.allocate(len + HEADER_LENGTH);
        buffer.putShort((short) set.size());
        // tmp crc
        buffer.putInt(0);
        for (var e : set) {
            buffer.putShort((short) e.length());
            buffer.put(e.getBytes());
        }

        // crc
        if (len > 0) {
            var hb = buffer.array();
            int crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(2, crc);
        }

        return buffer.array();
    }

    public static short setSize(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    public static RedisHashKeys decode(byte[] data) {
        return decode(data, true);
    }

    public static RedisHashKeys decode(byte[] data, boolean doCheckCrc32) {
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

        var r = new RedisHashKeys();
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException("Length error, length: " + len);
            }

            var bytes = new byte[len];
            buffer.get(bytes);
            r.set.add(new String(bytes));
        }
        return r;
    }
}
