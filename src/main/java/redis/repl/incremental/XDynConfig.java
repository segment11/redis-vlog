package redis.repl.incremental;

import redis.repl.BinlogContent;
import redis.repl.ReplPair;

import java.nio.ByteBuffer;

public class XDynConfig implements BinlogContent {
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

    public static XDynConfig decodeFrom(ByteBuffer buffer) {
        return null;
    }

    @Override
    public void apply(byte slot, ReplPair replPair) {

    }
}
