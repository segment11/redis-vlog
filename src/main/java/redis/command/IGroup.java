
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.ErrorReply;
import redis.reply.NilReply;
import redis.reply.Reply;

import java.util.ArrayList;

public class IGroup extends BaseCommand {
    public IGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("incr".equals(cmd) || "incrby".equals(cmd) || "incrbyfloat".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("incr".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            return dGroup.decrBy(-1, 0);
        }

        if ("incrby".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            int by;
            try {
                by = Integer.parseInt(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            return dGroup.decrBy(-by, 0);
        }

        if ("incrbyfloat".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            double by;
            try {
                by = Double.parseDouble(new String(data[2]));
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_FLOAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            return dGroup.decrBy(0, -by);
        }

        return NilReply.INSTANCE;
    }
}
