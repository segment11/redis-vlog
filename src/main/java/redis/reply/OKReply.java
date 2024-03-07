package redis.reply;

import io.activej.bytebuf.ByteBuf;

public class OKReply implements Reply {
    public static final OKReply INSTANCE = new OKReply();

    private static final byte[] OK = new byte[]{'+', 'O', 'K', '\r', '\n'};
    private static final byte[] OK_BYTES = "OK".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(OK);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(OK_BYTES);
    }
}
