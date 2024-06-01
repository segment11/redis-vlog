package redis.persist;

import io.activej.bytebuf.ByteBuf;

import static redis.CompressedValue.NO_EXPIRE;

public class PersistValueMeta {
    // slot byte + segment sub block index byte
    // + length int + segment index int + segment offset int
    // may add type or other metadata in the future
    static final int ENCODED_LENGTH = 1 + 1 + 4 + 4 + 4;

    // CompressedValue encoded length is much more than PersistValueMeta encoded length
    public static boolean isPvm(byte[] bytes) {
        // short string encoded
        // first byte is type, < 0 means special type
        return bytes[0] >= 0 && (bytes.length == ENCODED_LENGTH);
    }

    byte slot;
    byte subBlockIndex;
    int length;
    int segmentIndex;
    int segmentOffset;
    // need remove expired pvm in key loader to compress better, or reduce split
    long expireAt = NO_EXPIRE;
    long seq;

    // for reset compare with old CompressedValue
    byte[] keyBytes;
    long keyHash;
    int bucketIndex;

    byte[] extendBytes;

    int cellCostInKeyBucket() {
        var valueLength = extendBytes != null ? extendBytes.length : ENCODED_LENGTH;
        if (valueLength > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Persist value meta extend bytes too long: " + valueLength);
        }
        return KeyBucket.KVMeta.calcCellCount((short) keyBytes.length, (byte) valueLength);
    }

    @Override
    public String toString() {
        return "PersistValueMeta{" +
                "slot=" + slot +
                ", length=" + length +
                ", segmentIndex=" + segmentIndex +
                ", subBlockIndex=" + subBlockIndex +
                ", segmentOffset=" + segmentOffset +
                ", expireAt=" + expireAt +
                '}';
    }

    public byte[] encode() {
        var bytes = new byte[ENCODED_LENGTH];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeByte(slot);
        buf.writeByte(subBlockIndex);
        buf.writeInt(length);
        buf.writeInt(segmentIndex);
        buf.writeInt(segmentOffset);
        return bytes;
    }

    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        pvm.slot = buf.readByte();
        pvm.subBlockIndex = buf.readByte();
        pvm.length = buf.readInt();
        pvm.segmentIndex = buf.readInt();
        pvm.segmentOffset = buf.readInt();
        return pvm;
    }

}
