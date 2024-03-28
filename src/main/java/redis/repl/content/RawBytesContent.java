package redis.repl.content;

import io.activej.bytebuf.ByteBuf;
import redis.repl.ReplContent;

public class RawBytesContent implements ReplContent {
    private final byte[] bytes;

    public RawBytesContent(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(bytes);
    }

    @Override
    public int encodeLength() {
        return bytes.length;
    }
}
