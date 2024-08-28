package redis.type;

import com.github.luben.zstd.Zstd;
import org.jetbrains.annotations.VisibleForTesting;
import redis.*;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static redis.DictMap.TO_COMPRESS_MIN_DATA_LENGTH;

// key / value save together
public class RedisHH {
    public static final byte[] PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX = "h_not_hh_".getBytes();

    @VisibleForTesting
    // hash size short + dict seq int + raw bytes length int + crc int
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

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

    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    public void putAll(HashMap<String, byte[]> map) {
        this.map.putAll(map);
    }

    public byte[] get(String key) {
        return map.get(key);
    }

    public byte[] encode() {
        return encode(null);
    }

    public byte[] encode(Dict dict) {
        int len = 0;
        for (var entry : map.entrySet()) {
            // key / value length use 2 bytes
            var key = entry.getKey();
            var value = entry.getValue();
            len += 2 + key.length() + 2 + value.length;
        }

        var buffer = ByteBuffer.allocate(len + HEADER_LENGTH);
        buffer.putShort((short) map.size());
        // tmp no dict seq
        buffer.putInt(0);
        buffer.putInt(len);
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
        int crc = 0;
        if (len > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytes = buffer.array();
        if (rawBytes.length > TO_COMPRESS_MIN_DATA_LENGTH) {
            var dictSeq = dict == null ? Dict.SELF_ZSTD_DICT_SEQ : dict.getSeq();

            var dst = new byte[((int) Zstd.compressBound(len))];
            int compressedSize;
            if (dict == null) {
                compressedSize = (int) Zstd.compressByteArray(dst, 0, dst.length, rawBytes, HEADER_LENGTH, len, Zstd.defaultCompressionLevel());
            } else {
                compressedSize = (int) Zstd.compressUsingDict(dst, 0, rawBytes, HEADER_LENGTH, len, dict.getDictBytes(), Zstd.defaultCompressionLevel());
            }

            if (compressedSize < len * 0.9) {
                var compressedBytes = new byte[compressedSize + HEADER_LENGTH];
                System.arraycopy(dst, 0, compressedBytes, HEADER_LENGTH, compressedSize);
                ByteBuffer buffer1 = ByteBuffer.wrap(compressedBytes);
                buffer1.putShort((short) map.size());
                buffer1.putInt(dictSeq);
                buffer1.putInt(len);
                buffer1.putInt(crc);
                return compressedBytes;
            }
        }
        return rawBytes;
    }

    public static RedisHH decode(byte[] data) {
        return decode(data, true);
    }

    public static RedisHH decode(byte[] data, boolean doCheckCrc32) {
        var r = new RedisHH();
        iterate(data, doCheckCrc32, (field, valueBytes) -> {
            r.map.put(field, valueBytes);
            return false;
        });
        return r;
    }

    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    public interface IterateCallback {
        boolean onField(String field, byte[] valueBytes);
    }

    public static void iterate(byte[] data, boolean doCheckCrc32, IterateCallback callback) {
        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var rawBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            // decompress first
            if (dictSeq == Dict.SELF_ZSTD_DICT_SEQ) {
                var rawBytes = new byte[rawBytesLength];
                int decompressedSize = (int) Zstd.decompressByteArray(rawBytes, 0, rawBytes.length, data, HEADER_LENGTH, data.length - HEADER_LENGTH);
                if (decompressedSize <= 0) {
                    throw new IllegalStateException("Decompress error");
                }

                data = new byte[decompressedSize + HEADER_LENGTH];
                System.arraycopy(rawBytes, 0, data, HEADER_LENGTH, decompressedSize);
                buffer = ByteBuffer.wrap(data);
                buffer.position(HEADER_LENGTH);
            } else {
                var dict = DictMap.getInstance().getDictBySeq(dictSeq);
                if (dict == null) {
                    throw new DictMissingException("Dict not found, dict seq: " + dictSeq);
                }

                var rawBytes = new byte[rawBytesLength];
                int decompressedSize = (int) Zstd.decompressUsingDict(rawBytes, 0, data, HEADER_LENGTH, data.length - HEADER_LENGTH, dict.getDictBytes());
                if (decompressedSize <= 0) {
                    throw new IllegalStateException("Decompress error");
                }

                data = new byte[decompressedSize + HEADER_LENGTH];
                System.arraycopy(rawBytes, 0, data, HEADER_LENGTH, decompressedSize);
                buffer = ByteBuffer.wrap(data);
                buffer.position(HEADER_LENGTH);
            }
        }

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

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
            var isBreak = callback.onField(new String(keyBytes), valueBytes);
            if (isBreak) {
                break;
            }
        }
    }
}
