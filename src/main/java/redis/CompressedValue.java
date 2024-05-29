package redis;

import com.github.luben.zstd.Zstd;
import io.activej.bytebuf.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CompressedValue {
    public static final long NO_EXPIRE = 0;
    public static final long EXPIRE_NOW = -1;
    public static final int NULL_DICT_SEQ = 0;
    public static final int SP_TYPE_NUM_BYTE = -1;
    public static final int SP_TYPE_NUM_SHORT = -2;
    public static final int SP_TYPE_NUM_INT = -4;
    public static final int SP_TYPE_NUM_LONG = -8;
    public static final int SP_TYPE_NUM_DOUBLE = -16;
    public static final int SP_TYPE_SHORT_STRING = -32;
    // if string length <= 10, need not write to chunk, just use key bucket
    public static final int SP_TYPE_SHORT_STRING_MIN_LEN = 10;

    // need save as a singe file
    public static final int SP_TYPE_BIG_STRING = -64;

    public static final byte SP_FLAG_DELETE_TMP = -128;

    public static final int SP_TYPE_HH = -512;
    public static final int SP_TYPE_HH_COMPRESSED = -513;
    public static final int SP_TYPE_HASH = -1024;
    public static final int SP_TYPE_HASH_COMPRESSED = -1025;
    public static final int SP_TYPE_LIST = -2048;
    public static final int SP_TYPE_LIST_COMPRESSED = -2049;
    public static final int SP_TYPE_SET = -4096;
    public static final int SP_TYPE_SET_COMPRESSED = -4097;
    public static final int SP_TYPE_ZSET = -8192;
    public static final int SP_TYPE_ZSET_COMPRESSED = -8193;
    public static final int SP_TYPE_STREAM = -16384;

    // change here to limit key size
    public static final short KEY_MAX_LENGTH = 256;
    // change here to limit value size
    // 8KB data compress should <= 4KB can store in one PAGE_SIZE
    public static final short VALUE_MAX_LENGTH = Short.MAX_VALUE;

    // seq long + expireAt long + dictSeq int + keyHash long + uncompressedLength int + cvEncodedLength int
    public static final int VALUE_HEADER_LENGTH = 8 + 8 + 4 + 8 + 4 + 4;
    // key length use short
    public static final int KEY_HEADER_LENGTH = 2;

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public long getExpireAt() {
        return expireAt;
    }

    public void setExpireAt(long expireAt) {
        this.expireAt = expireAt;
    }

    public long getKeyHash() {
        return keyHash;
    }

    public void setKeyHash(long keyHash) {
        this.keyHash = keyHash;
    }

    long seq;
    // milliseconds
    long expireAt = NO_EXPIRE;
    // dict seq or special type, is a union
    int dictSeqOrSpType = NULL_DICT_SEQ;

    public int getDictSeqOrSpType() {
        return dictSeqOrSpType;
    }

    public void setDictSeqOrSpType(int dictSeqOrSpType) {
        this.dictSeqOrSpType = dictSeqOrSpType;
    }

    long keyHash;
    int uncompressedLength;
    int compressedLength;

    public int getCompressedLength() {
        return compressedLength;
    }

    public boolean isTypeNumber() {
        return dictSeqOrSpType <= SP_TYPE_NUM_BYTE && dictSeqOrSpType >= SP_TYPE_NUM_DOUBLE;
    }

    public static boolean isTypeNumber(int spType) {
        return spType <= SP_TYPE_NUM_BYTE && spType >= SP_TYPE_NUM_DOUBLE;
    }

    public byte[] encodeAsNumber() {
        return switch (dictSeqOrSpType) {
            case SP_TYPE_NUM_BYTE -> {
                var buf = ByteBuffer.allocate(1 + 8 + 1);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData[0]);
                yield buf.array();
            }
            case SP_TYPE_NUM_SHORT -> {
                var buf = ByteBuffer.allocate(1 + 8 + 2);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData);
                yield buf.array();
            }
            case SP_TYPE_NUM_INT -> {
                var buf = ByteBuffer.allocate(1 + 8 + 4);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData);
                yield buf.array();
            }
            case SP_TYPE_NUM_LONG, SP_TYPE_NUM_DOUBLE -> {
                var buf = ByteBuffer.allocate(1 + 8 + 8);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData);
                yield buf.array();
            }
            default -> throw new IllegalStateException("Unexpected number type: " + dictSeqOrSpType);
        };
    }

    // not seq, may have a problem
    public byte[] encodeAsShortString() {
        var buf = ByteBuffer.allocate(1 + 8 + compressedData.length);
        buf.put((byte) SP_TYPE_SHORT_STRING);
        buf.putLong(seq);
        buf.put(compressedData);
        return buf.array();
    }

    public Number numberValue() {
        return switch (dictSeqOrSpType) {
            case SP_TYPE_NUM_BYTE -> compressedData[0];
            case SP_TYPE_NUM_SHORT -> ByteBuffer.wrap(compressedData).getShort();
            case SP_TYPE_NUM_INT -> ByteBuffer.wrap(compressedData).getInt();
            case SP_TYPE_NUM_LONG -> ByteBuffer.wrap(compressedData).getLong();
            case SP_TYPE_NUM_DOUBLE -> ByteBuffer.wrap(compressedData).getDouble();
            default -> throw new IllegalStateException("Not a number type: " + dictSeqOrSpType);
        };
    }

    public boolean isBigString() {
        return dictSeqOrSpType == SP_TYPE_BIG_STRING;
    }

    public boolean isHash() {
        return dictSeqOrSpType == SP_TYPE_HH || dictSeqOrSpType == SP_TYPE_HH_COMPRESSED ||
                dictSeqOrSpType == SP_TYPE_HASH || dictSeqOrSpType == SP_TYPE_HASH_COMPRESSED;
    }

    public boolean isList() {
        return dictSeqOrSpType == SP_TYPE_LIST || dictSeqOrSpType == SP_TYPE_LIST_COMPRESSED;
    }

    public boolean isSet() {
        return dictSeqOrSpType == SP_TYPE_SET || dictSeqOrSpType == SP_TYPE_SET_COMPRESSED;
    }

    public boolean isZSet() {
        return dictSeqOrSpType == SP_TYPE_ZSET || dictSeqOrSpType == SP_TYPE_ZSET_COMPRESSED;
    }

    public boolean isStream() {
        return dictSeqOrSpType == SP_TYPE_STREAM;
    }

    @Override
    public String toString() {
        return "CompressedValue{" +
                "seq=" + seq +
                ", expireAt=" + expireAt +
                ", dictSeqOrSpType=" + dictSeqOrSpType +
                ", keyHash=" + keyHash +
                ", uncompressedLength=" + uncompressedLength +
                ", cvEncodedLength=" + compressedLength +
                '}';
    }

    byte[] compressedData;

    public byte[] getCompressedData() {
        return compressedData;
    }

    public boolean isExpired() {
        return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
    }

    public boolean noExpire() {
        return expireAt == NO_EXPIRE;
    }

    public boolean isCompressed() {
        return dictSeqOrSpType > NULL_DICT_SEQ ||
                dictSeqOrSpType == SP_TYPE_HH_COMPRESSED ||
                dictSeqOrSpType == SP_TYPE_HASH_COMPRESSED ||
                dictSeqOrSpType == SP_TYPE_LIST_COMPRESSED ||
                dictSeqOrSpType == SP_TYPE_SET_COMPRESSED ||
                dictSeqOrSpType == SP_TYPE_ZSET_COMPRESSED;
    }

    public static boolean preferCompress(int spType) {
        return spType == SP_TYPE_HH_COMPRESSED ||
                spType == SP_TYPE_HASH_COMPRESSED ||
                spType == SP_TYPE_LIST_COMPRESSED ||
                spType == SP_TYPE_SET_COMPRESSED ||
                spType == SP_TYPE_ZSET_COMPRESSED;
    }

    public static boolean isTypeString(int spType) {
        // number type also use string type
        return spType >= SP_TYPE_BIG_STRING;
    }

    public boolean isTypeString() {
        // number type also use string type
        return dictSeqOrSpType >= SP_TYPE_BIG_STRING;
    }

    public boolean isUseDict() {
        return dictSeqOrSpType > NULL_DICT_SEQ;
    }

    public byte[] decompress(Dict dict) {
        var dst = new byte[uncompressedLength];
        if (dict == null || dict == Dict.SELF_ZSTD_DICT) {
            Zstd.decompress(dst, compressedData);
        } else {
            Zstd.decompressUsingDict(dst, 0, compressedData, 0, compressedData.length, dict.dictBytes);
        }
        return dst;
    }

    public static CompressedValue compress(byte[] data, Dict dict, int level) {
        var cv = new CompressedValue();

        // memory copy too much, use direct buffer better
        var dst = new byte[((int) Zstd.compressBound(data.length))];
        int compressedSize;
        if (dict == null || dict == Dict.SELF_ZSTD_DICT) {
            compressedSize = (int) Zstd.compress(dst, data, level);
        } else {
            compressedSize = (int) Zstd.compressUsingDict(dst, 0, data, 0, data.length, dict.dictBytes, level);
        }

        // if waste too much space, copy to another
        if (dst.length != compressedSize) {
            // use heap buffer
            // memory copy too much
            var newDst = new byte[compressedSize];
            System.arraycopy(dst, 0, newDst, 0, compressedSize);
            cv.compressedData = newDst;
        } else {
            cv.compressedData = dst;
        }

        cv.compressedLength = compressedSize;
        cv.uncompressedLength = data.length;
        return cv;
    }

    public boolean isShortString() {
        return compressedData != null && compressedData.length <= CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN;
    }

    public static boolean isDeleted(byte[] encoded) {
        return encoded.length == 1 && encoded[0] == SP_FLAG_DELETE_TMP;
    }

    public byte[] encode() {
        int len = VALUE_HEADER_LENGTH;
        len += compressedLength;

        var bytes = new byte[len];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeInt(dictSeqOrSpType);
        buf.writeLong(keyHash);
        buf.writeInt(uncompressedLength);
        buf.writeInt(compressedLength);
        if (compressedData != null && compressedLength > 0) {
            buf.write(compressedData);
        }
        return bytes;
    }

    public void encodeTo(ByteBuf buf) {
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeInt(dictSeqOrSpType);
        buf.writeLong(keyHash);
        buf.writeInt(uncompressedLength);
        buf.writeInt(compressedLength);
        if (compressedData != null && compressedLength > 0) {
            buf.write(compressedData);
        }
    }

    public byte[] encodeAsBigStringMeta(long uuid) {
        int len = VALUE_HEADER_LENGTH;

        // uuid + dict int
        compressedLength = 8 + 4;

        len += compressedLength;

        compressedData = new byte[12];
        ByteBuffer.wrap(compressedData).putLong(uuid).putInt(dictSeqOrSpType);

        var bytes = new byte[len];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeInt(SP_TYPE_BIG_STRING);
        buf.writeLong(keyHash);
        buf.writeInt(uncompressedLength);
        buf.writeInt(compressedLength);
        buf.write(compressedData);

        return bytes;
    }


    public int compressedLength() {
        return compressedLength;
    }

    public int uncompressedLength() {
        return uncompressedLength;
    }

    private static final Logger log = LoggerFactory.getLogger(CompressedValue.class);

    public static CompressedValue decode(io.netty.buffer.ByteBuf buf, byte[] keyBytes, long keyHash, boolean isJustForCheck) {
        var cv = new CompressedValue();
        var firstByte = buf.getByte(0);
        if (firstByte < 0) {
            cv.dictSeqOrSpType = firstByte;
            buf.skipBytes(1);
            cv.seq = buf.readLong();
            cv.compressedData = new byte[buf.readableBytes()];
            buf.readBytes(cv.compressedData);
            cv.compressedLength = cv.compressedData.length;
            cv.uncompressedLength = cv.compressedLength;
            return cv;
        }

        cv.seq = buf.readLong();
        cv.expireAt = buf.readLong();
        cv.dictSeqOrSpType = buf.readInt();
        cv.keyHash = buf.readLong();

        if (keyHash == 0 && keyBytes != null) {
            keyHash = KeyHash.hash(keyBytes);
        }

        if (keyHash != 0 && cv.keyHash != keyHash) {
            cv.uncompressedLength = buf.readInt();
            cv.compressedLength = buf.readInt();
            if (cv.compressedLength > 0) {
                buf.skipBytes(cv.compressedLength);
            }

            // why ? todo: check
            log.warn("Key hash not match, key: {}, seq: {}, uncompressedLength: {}, cvEncodedLength: {}, keyHash: {}, persisted keyHash: {}",
                    new String(keyBytes), cv.seq, cv.uncompressedLength, cv.compressedLength, keyHash, cv.keyHash);
            throw new IllegalStateException("Key hash not match, key: " + new String(keyBytes) +
                    ", seq: " + cv.seq +
                    ", uncompressedLength: " + cv.uncompressedLength +
                    ", cvEncodedLength: " + cv.compressedLength +
                    ", keyHash: " + keyHash +
                    ", persisted keyHash: " + cv.keyHash);
        }

        cv.uncompressedLength = buf.readInt();
        cv.compressedLength = buf.readInt();
        if (cv.compressedLength > 0) {
            if (isJustForCheck) {
                buf.skipBytes(cv.compressedLength);
            } else {
                cv.compressedData = new byte[cv.compressedLength];
                buf.readBytes(cv.compressedData);
            }
        }
        return cv;
    }
}
