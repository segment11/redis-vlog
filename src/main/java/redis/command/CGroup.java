
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.*;

import java.util.ArrayList;

public class CGroup extends BaseCommand {
    public CGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("copy".equals(cmd)) {
            if (data.length < 3) {
                return null;
            }
            var dstKeyBytes = data[2];
            return slot(dstKeyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("client".equals(cmd)) {
            return client();
        }

        if ("copy".equals(cmd)) {
            return copy();
        }

        return NilReply.INSTANCE;
    }

    private Reply client() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]).toLowerCase();
        if ("id".equals(subCmd)) {
            return new IntegerReply(socket.hashCode());
        }

        if ("setinfo".equals(subCmd)) {
            return OKReply.INSTANCE;
        }

        // todo
        return NilReply.INSTANCE;
    }

    private Reply copy() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var srcKeyBytes = data[1];
        var dstKeyBytes = data[2];

        boolean replace = false;
        for (int i = 3; i < data.length; i++) {
            if ("replace".equalsIgnoreCase(new String(data[i]))) {
                replace = true;
                break;
            }
        }

        var srcCv = getCv(srcKeyBytes);
        if (srcCv == null) {
            return IntegerReply.REPLY_0;
        }

        var dstSlotWithKeyHash = slotPreferParsed(dstKeyBytes);
        var existCv = getCv(dstKeyBytes, dstSlotWithKeyHash);
        if (existCv != null && !replace) {
            return IntegerReply.REPLY_0;
        }

        setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
        return IntegerReply.REPLY_1;
    }
}
