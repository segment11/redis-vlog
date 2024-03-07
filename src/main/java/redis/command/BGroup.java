package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.NilReply;
import redis.reply.OKReply;
import redis.reply.Reply;

public class BGroup extends BaseCommand {
    public BGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("bgsave".equals(cmd)) {
            return null;
        }

        return null;
    }

    public Reply handle() {
        if ("bgsave".equals(cmd)) {
            // already saved when handle request
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }
}
