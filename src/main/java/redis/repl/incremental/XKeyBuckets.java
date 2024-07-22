package redis.repl.incremental;

import redis.repl.BinlogContent;

import java.nio.ByteBuffer;

public class XKeyBuckets implements BinlogContent<XKeyBuckets> {
    @Override
    public Type type() {
        return null;
    }

    @Override
    public int encodedLength() {
        return 0;
    }

    @Override
    public byte[] encodeWithType() {
        return new byte[0];
    }

    public static XKeyBuckets decodeFrom(ByteBuffer buffer) {
        return null;
    }

    @Override
    public void apply(byte slot) {

    }
}
