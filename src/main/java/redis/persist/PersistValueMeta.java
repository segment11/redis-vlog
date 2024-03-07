package redis.persist;

import io.activej.bytebuf.ByteBuf;

import static redis.CompressedValue.NO_EXPIRE;

public class PersistValueMeta {
    // worker id byte + slot byte  + batch index byte + skip 1 byte + length int + offset long + expire at long
    // may add type or other metadata in the future
    static final int ENCODED_LEN = 1 + 1 + 1 + 1 + 4 + 8 + 8;
    // + key masked value int + bucket index int
    static final int ENCODED_LEN_WITH_KEY_MASKED_VALUE = ENCODED_LEN + 4 + 4;

    // CompressedValue encoded length is much more than PersistValueMeta encoded length
    public static boolean isPvm(byte[] bytes) {
        // short string encoded
        // first byte is type, < 0 means special type
        return bytes[0] >= 0 && (bytes.length == ENCODED_LEN || bytes.length == ENCODED_LEN_WITH_KEY_MASKED_VALUE);
    }

    byte workerId;
    byte slot;
    byte batchIndex;
    int length;
    long offset;
    // need remove expired pvm in key loader to compress better, or reduce split
    long expireAt = NO_EXPIRE;

    // for reset compare with old CompressedValue
    byte[] keyBytes;
    long keyHash;
    int bucketIndex;

    public boolean isExpired() {
        return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "PersistValueMeta{" +
                "w=" + workerId +
                ", s=" + slot +
                ", b=" + batchIndex +
                ", l=" + length +
                ", o=" + offset +
                ", e=" + expireAt +
                '}';
    }

    public byte[] encode() {
        var bytes = new byte[ENCODED_LEN];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeByte(workerId);
        buf.writeByte(slot);
        buf.writeByte(batchIndex);
        buf.writeByte((byte) 0);
        buf.writeInt(length);
        buf.writeLong(offset);
        buf.writeLong(expireAt);
        return bytes;
    }

    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        pvm.workerId = buf.readByte();
        pvm.slot = buf.readByte();
        pvm.batchIndex = buf.readByte();
        buf.readByte();
        pvm.length = buf.readInt();
        pvm.offset = buf.readLong();
        pvm.expireAt = buf.readLong();
        return pvm;
    }

}
