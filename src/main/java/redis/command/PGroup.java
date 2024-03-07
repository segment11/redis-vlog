
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.ErrorReply;
import redis.reply.NilReply;
import redis.reply.Reply;

public class PGroup extends BaseCommand {
    public PGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("pexpire".equals(cmd) || "pexpireat".equals(cmd)) {
            if (data.length != 3 && data.length != 4) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        if ("pexpiretime".equals(cmd) || "pttl".equals(cmd)) {
            if (data.length != 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("pexpire".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(false, true);
        }

        if ("pexpireat".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expire(true, true);
        }

        if ("pexpiretime".equals(cmd)) {
            var eGroup = new EGroup(cmd, data, socket);
            eGroup.from(this);
            return eGroup.expiretime(true);
        }

        if ("pttl".equals(cmd)) {
            var tGroup = new TGroup(cmd, data, socket);
            tGroup.from(this);
            return tGroup.ttl(true);
        }

        if ("psetex".equals(cmd)) {
            if (data.length != 4) {
                return ErrorReply.FORMAT;
            }

            byte[][] dd = {null, data[1], data[3], "px".getBytes(), data[2]};
            var sGroup = new SGroup(cmd, dd, socket);
            sGroup.from(this);
            return sGroup.set(dd);
        }

        return NilReply.INSTANCE;
    }
}
