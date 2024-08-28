
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.dyn.CachedGroovyClassLoader;
import redis.dyn.RefreshLoader;
import redis.reply.ErrorReply;
import redis.reply.NilReply;
import redis.reply.Reply;

import java.util.ArrayList;
import java.util.HashMap;

public class IGroup extends BaseCommand {
    public IGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("incr".equals(cmd) || "incrby".equals(cmd) || "incrbyfloat".equals(cmd)) {
            if (data.length < 2) {
                return slotWithKeyHashList;
            }
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        return slotWithKeyHashList;
    }

    public Reply handle() {
        if ("incr".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            var dGroup = new DGroup(cmd, data, socket);
            dGroup.from(this);
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

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
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

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
            dGroup.setSlotWithKeyHashListParsed(this.slotWithKeyHashListParsed);

            return dGroup.decrBy(0, -by);
        }

        if ("info".equals(cmd)) {
            return info();
        }

        return NilReply.INSTANCE;
    }

    private Reply info() {
        if (data.length != 1 && data.length != 2) {
            return ErrorReply.FORMAT;
        }

        var scriptText = RefreshLoader.getScriptText("/dyn/src/script/InfoCommandHandle.groovy");

        var variables = new HashMap<String, Object>();
        variables.put("iGroup", this);
        return (Reply) CachedGroovyClassLoader.getInstance().eval(scriptText, variables);
    }
}
