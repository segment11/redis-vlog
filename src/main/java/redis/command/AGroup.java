
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.ErrorReply;
import redis.reply.IntegerReply;
import redis.reply.NilReply;
import redis.reply.Reply;

public class AGroup extends BaseCommand {
    public AGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("append".equals(cmd)) {
            if (data.length < 3) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    @Override
    public Reply handle() {
        if ("append".equals(cmd)) {
            return append();
        }

        return NilReply.INSTANCE;
    }

    private Reply append() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var keyBytes = data[1];
        var slotWithKeyHash = slotPreferParsed(keyBytes);

        var valueBytes = data[2];

        int length;

        var existsValueBytes = get(keyBytes, slotWithKeyHash);
        if (existsValueBytes == null) {
            set(keyBytes, valueBytes, slotWithKeyHash);
            length = valueBytes.length;
        } else {
            var newValueBytes = new byte[existsValueBytes.length + valueBytes.length];
            System.arraycopy(existsValueBytes, 0, newValueBytes, 0, existsValueBytes.length);
            System.arraycopy(valueBytes, 0, newValueBytes, existsValueBytes.length, valueBytes.length);

            set(keyBytes, newValueBytes, slotWithKeyHash);
            length = newValueBytes.length;
        }

        return new IntegerReply(length);
    }
}
