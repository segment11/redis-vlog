package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class ToSlaveKeyBucketSplit implements ReplContent {
    private final int bucketIndex;
    private final byte splitNumber;

    public ToSlaveKeyBucketSplit(int bucketIndex, byte splitNumber) {
        this.bucketIndex = bucketIndex;
        this.splitNumber = splitNumber;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeInt(bucketIndex);
        toBuf.writeByte(splitNumber);
    }

    @Override
    public int encodeLength() {
        return 4 + 1;
    }
}
