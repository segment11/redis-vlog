package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class ToSlaveKeyBucketUpdate implements ReplContent {
    private final int bucketIndex;
    private final byte splitIndex;
    private final byte splitNumber;
    private final long seq;
    private final byte[] bytes;

    public ToSlaveKeyBucketUpdate(int bucketIndex, byte splitIndex, byte splitNumber, long seq, byte[] bytes) {
        this.bucketIndex = bucketIndex;
        this.splitIndex = splitIndex;
        this.splitNumber = splitNumber;
        this.seq = seq;
        this.bytes = bytes;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeInt(bucketIndex);
        toBuf.writeByte(splitIndex);
        toBuf.writeByte(splitNumber);
        toBuf.writeLong(seq);
        toBuf.writeInt(bytes.length);
        toBuf.write(bytes);
    }

    @Override
    public int encodeLength() {
        return 4 * 1 + 1 + 8 + 4 + bytes.length;
    }
}
