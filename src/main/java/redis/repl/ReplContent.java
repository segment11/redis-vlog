package redis.repl;

import io.activej.bytebuf.ByteBuf;

public interface ReplContent {
    void encodeTo(ByteBuf toBuf);

    int encodeLength();
}
