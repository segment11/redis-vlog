package redis.persist;

import io.activej.bytebuf.ByteBuf;

import static redis.CompressedValue.NO_EXPIRE;

public class PersistValueMeta {
    // worker id byte + slot byte  + batch index byte + segment sub block index byte
    // + length int + segment index int + segment offset int
    // may add type or other metadata in the future
    static final int ENCODED_LEN = 1 + 1 + 1 + 1 + 4 + 4 + 4;

    // CompressedValue encoded length is much more than PersistValueMeta encoded length
    public static boolean isPvm(byte[] bytes) {
        // short string encoded
        // first byte is type, < 0 means special type
        return bytes[0] >= 0 && (bytes.length == ENCODED_LEN);
    }

    byte workerId;
    byte slot;
    byte batchIndex;
    byte subBlockIndex;
    int length;
    int segmentIndex;
    int segmentOffset;
    // need remove expired pvm in key loader to compress better, or reduce split
    long expireAt = NO_EXPIRE;

    // for reset compare with old CompressedValue
    byte[] keyBytes;
    long keyHash;
    int bucketIndex;

    @Override
    public String toString() {
        return "PersistValueMeta{" +
                "workerId=" + workerId +
                ", slot=" + slot +
                ", batchIndex=" + batchIndex +
                ", length=" + length +
                ", segmentIndex=" + segmentIndex +
                ", subBlockIndex=" + subBlockIndex +
                ", segmentOffset=" + segmentOffset +
                ", expireAt=" + expireAt +
                '}';
    }

    public byte[] encode() {
        var bytes = new byte[ENCODED_LEN];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeByte(workerId);
        buf.writeByte(slot);
        buf.writeByte(batchIndex);
        buf.writeByte(subBlockIndex);
        buf.writeInt(length);
        buf.writeInt(segmentIndex);
        buf.writeInt(segmentOffset);
        return bytes;
    }

    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        pvm.workerId = buf.readByte();
        pvm.slot = buf.readByte();
        pvm.batchIndex = buf.readByte();
        pvm.subBlockIndex = buf.readByte();
        pvm.length = buf.readInt();
        pvm.segmentIndex = buf.readInt();
        pvm.segmentOffset = buf.readInt();
        return pvm;
    }

}
