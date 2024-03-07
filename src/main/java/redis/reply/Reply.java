package redis.reply;

import io.activej.bytebuf.ByteBuf;

public interface Reply {
    ByteBuf buffer();
    ByteBuf bufferAsHttp();
}
