package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.NilReply;
import redis.reply.OKReply;
import redis.reply.Reply;

import java.util.ArrayList;

public class BGroup extends BaseCommand {
    public BGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("bgsave".equals(cmd)) {
            // already saved when handle request
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }
}
