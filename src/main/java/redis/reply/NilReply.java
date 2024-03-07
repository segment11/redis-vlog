package redis.reply;

import io.activej.bytebuf.ByteBuf;

public class NilReply implements Reply {
    public static final NilReply INSTANCE = new NilReply();

    private static final byte[] NIL = new BulkReply().buffer().asArray();
    private static final byte[] NULL_BYTES = "null".getBytes();

    @Override
    public ByteBuf buffer() {
        return ByteBuf.wrapForReading(NIL);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(NULL_BYTES);
    }
}
