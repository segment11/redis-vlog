
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.NilReply;
import redis.reply.OKReply;
import redis.reply.Reply;

import java.util.ArrayList;

public class FGroup extends BaseCommand {
    public FGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        return null;
    }

    public Reply handle() {
        if ("flushdb".equals(cmd) || "flushall".equals(cmd)) {
            boolean isAsync = data.length == 2 && "async".equalsIgnoreCase(new String(data[1]));
            if (isAsync) {
                // todo
                for (int i = 0; i < slotNumber; i++) {
                    localPersist.flush((byte) i);
                }
            } else {
                for (int i = 0; i < slotNumber; i++) {
                    localPersist.flush((byte) i);
                }
            }
            return new OKReply();
        }

        return NilReply.INSTANCE;
    }
}
