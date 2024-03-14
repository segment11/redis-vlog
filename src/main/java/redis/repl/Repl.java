package redis.repl;

import io.netty.buffer.ByteBuf;
import redis.reply.Reply;

import java.nio.ByteBuffer;

public class Repl {
    public static final byte[] PROTOCOL_KEYWORD_BYTES = "X-REPL".getBytes();

    public static io.activej.bytebuf.ByteBuf buffer(long slaveUuid, byte slot, ReplType type, ReplContent content) {
        var encoded = content.encode();

        var bytes = new byte[PROTOCOL_KEYWORD_BYTES.length + 8 + 1 + 1 + 4 + encoded.length];
        var buf = io.activej.bytebuf.ByteBuf.wrapForWriting(bytes);

        buf.write(PROTOCOL_KEYWORD_BYTES);
        buf.writeLong(slaveUuid);
        buf.writeByte(slot);
        buf.writeByte(type.code);
        buf.writeInt(encoded.length);
        buf.write(encoded);
        return buf;
    }

    static class ReplReply implements Reply {
        private final io.activej.bytebuf.ByteBuf buf;

        ReplReply(io.activej.bytebuf.ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public io.activej.bytebuf.ByteBuf buffer() {
            return buf;
        }
    }

    public static Reply reply(byte slot, ReplPair replPair, ReplType type, ReplContent content) {
        return new ReplReply(buffer(replPair.getSlaveUuid(), slot, type, content));
    }

    public static Reply emptyReply() {
        return new ReplReply(io.activej.bytebuf.ByteBuf.empty());
    }

    public static byte[][] decode(ByteBuf buf) {
        if (buf.readableBytes() < PROTOCOL_KEYWORD_BYTES.length + 8 + 1 + 1 + 4) {
            return null;
        }
        buf.skipBytes(PROTOCOL_KEYWORD_BYTES.length);

        var slaveUuid = buf.readLong();
        var slot = buf.readByte();

        var replType = ReplType.fromCode(buf.readByte());
        if (replType == null) {
            return null;
        }

        var dataLength = buf.readInt();
        if (buf.readableBytes() < dataLength) {
            return null;
        }

        var data = new byte[4][];
        data[0] = new byte[8];
        ByteBuffer.wrap(data[0]).putLong(slaveUuid);

        data[1] = new byte[1];
        data[1][0] = slot;

        data[2] = new byte[1];
        data[2][0] = replType.code;

        var bytes = new byte[dataLength];
        buf.readBytes(bytes);
        data[3] = bytes;

        return data;
    }
}
