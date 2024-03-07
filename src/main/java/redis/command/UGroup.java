
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.reply.NilReply;
import redis.reply.Reply;

public class UGroup extends BaseCommand {
    public UGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public Reply handle() {
        return NilReply.INSTANCE;
    }
}
