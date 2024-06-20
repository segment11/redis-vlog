package redis;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class Dict implements Serializable {
    private static final int BEGIN_SEQ = 100;

    public static final int SELF_ZSTD_DICT_SEQ = 1;

    public static final Dict SELF_ZSTD_DICT = new Dict();

    static AtomicInteger seqGenerator = new AtomicInteger(BEGIN_SEQ);

    public int getSeq() {
        return seq;
    }

    int seq;
    long createdTime;
    byte[] dictBytes;

    @Override
    public String toString() {
        return "Dict{" +
                "seq=" + seq +
                ", createdTime=" + new Date(createdTime) +
                ", dictBytes.length=" + (dictBytes == null ? 0 : dictBytes.length) +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(seq);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Dict dict = (Dict) obj;
        return seq == dict.seq;
    }

    public byte[] getDictBytes() {
        return dictBytes;
    }

    // seq int + create time long + key prefix length short + key prefix + dict bytes length short + dict bytes
    private static final int ENCODED_HEADER_LENGTH = 4 + 8 + 2 + 2;

    public int encodeLength(String keyPrefix) {
        return 4 + ENCODED_HEADER_LENGTH + keyPrefix.length() + dictBytes.length;
    }

    public byte[] encode(String keyPrefix) {
        int vLength = ENCODED_HEADER_LENGTH + keyPrefix.length() + dictBytes.length;

        var bytes = new byte[4 + vLength];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.putInt(vLength);
        buffer.putInt(seq);
        buffer.putLong(createdTime);
        buffer.putShort((short) keyPrefix.length());
        buffer.put(keyPrefix.getBytes());
        buffer.putShort((short) dictBytes.length);
        buffer.put(dictBytes);

        return bytes;
    }

    public record DictWithKeyPrefix(String keyPrefix, Dict dict) {
        @Override
        public String toString() {
            return "DictWithKey{" +
                    "keyPrefix='" + keyPrefix + '\'' +
                    ", dict=" + dict +
                    '}';
        }
    }

    public static DictWithKeyPrefix decode(DataInputStream is) throws IOException {
        if (is.available() < 4) {
            return null;
        }

        var vLength = is.readInt();
        if (vLength == 0) {
            return null;
        }

        var seq = is.readInt();
        var createdTime = is.readLong();
        var keyPrefixLength = is.readShort();
        if (keyPrefixLength > CompressedValue.KEY_MAX_LENGTH || keyPrefixLength <= 0) {
            throw new IllegalStateException("Key prefix length error, key length: " + keyPrefixLength);
        }

        var keyPrefixBytes = new byte[keyPrefixLength];
        is.readFully(keyPrefixBytes);
        var dictBytesLength = is.readShort();
        var dictBytes = new byte[dictBytesLength];
        is.readFully(dictBytes);

        if (vLength != ENCODED_HEADER_LENGTH + keyPrefixLength + dictBytesLength) {
            throw new IllegalStateException("Invalid length: " + vLength);
        }

        var dict = new Dict();
        dict.seq = seq;
        dict.createdTime = createdTime;
        dict.dictBytes = dictBytes;

        return new DictWithKeyPrefix(new String(keyPrefixBytes), dict);
    }

    private Dict() {
        this.dictBytes = null;
        this.seq = SELF_ZSTD_DICT_SEQ;
        this.createdTime = System.currentTimeMillis();
    }

    public Dict(byte[] dictBytes) {
        if (dictBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Dict bytes too long: " + dictBytes.length);
        }

        this.dictBytes = dictBytes;
        this.seq = seqGenerator.incrementAndGet();
        this.createdTime = System.currentTimeMillis();
    }
}
