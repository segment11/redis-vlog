package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class ToSlaveBigStringFileWrite implements ReplContent {
    private final long uuid;
    private final byte[] bytes;

    public ToSlaveBigStringFileWrite(long uuid, byte[] bytes) {
        this.uuid = uuid;
        this.bytes = bytes;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        // todo
        toBuf.put((byte) 0);
    }

    @Override
    public int encodeLength() {
        return 1;
    }
}
