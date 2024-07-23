package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class EmptyContent implements ReplContent {
    public static final EmptyContent INSTANCE = new EmptyContent();

    private EmptyContent() {
    }

    public static boolean isEmpty(byte[] contentBytes) {
        return contentBytes.length == 1 && contentBytes[0] == 0;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte((byte) 0);
    }

    @Override
    public int encodeLength() {
        return 1;
    }
}
