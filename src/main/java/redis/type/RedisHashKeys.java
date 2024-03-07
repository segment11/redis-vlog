package redis.type;

import io.netty.buffer.Unpooled;
import redis.CompressedValue;
import redis.KeyHash;

import java.util.HashSet;

public class RedisHashKeys {
    // change here to limit hash size
    // keys encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose key length is 32, then 16KB / 32 = 512
    public static final short HASH_MAX_SIZE = 16 * 1024 / CompressedValue.KEY_MAX_LENGTH;

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
            // key length use 1 byte is enough
            len += 1 + e.length();
        }

        var buf = Unpooled.buffer(len + HEADER_LENGTH);
        buf.writeShort(set.size());
        // tmp crc
        buf.writeInt(0);
        for (var e : set) {
            buf.writeByte(e.length());
            buf.writeBytes(e.getBytes());
        }

        // crc
        if (len > 0) {
            var hb = buf.array();
            int crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buf.setInt(2, crc);
        }

        return buf.array();
    }

    public static RedisHashKeys decode(byte[] data) {
        var buf = Unpooled.wrappedBuffer(data);
        int size = buf.readShort();
        int crc = buf.readInt();

        // check crc
        if (size > 0) {
            int crcCompare = KeyHash.hash32Offset(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        var r = new RedisHashKeys();
        for (int i = 0; i < size; i++) {
            int len = buf.readByte();
            var bytes = new byte[len];
            buf.readBytes(bytes);
            r.set.add(new String(bytes));
        }
        return r;
    }
}
