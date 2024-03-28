package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class EmptyContent implements ReplContent {
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte((byte) 0);
    }

    @Override
    public int encodeLength() {
        return 1;
    }
}
