
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import redis.BaseCommand;
import redis.reply.*;

import java.util.ArrayList;

public class CGroup extends BaseCommand {
    public CGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("copy".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }

            var srcKeyBytes = data[1];
            var dstKeyBytes = data[2];

            slotWithKeyHashList.add(slot(srcKeyBytes, slotNumber));
            slotWithKeyHashList.add(slot(dstKeyBytes, slotNumber));
            return slotWithKeyHashList;
        }

//        if ("config".equals(cmd)) {
//            // all slots
//        }

        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        return null;
    }

    public Reply handle() {
        if ("client".equals(cmd)) {
            return client();
        }

        if ("config".equals(cmd)) {
            return config();
        }

        if ("copy".equals(cmd)) {
            return copy();
        }

        return NilReply.INSTANCE;
    }

    Reply client() {
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

    Reply config() {
        // todo
        return OKReply.INSTANCE;
    }

    Reply copy() {
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

        var srcCv = getCv(srcKeyBytes, slotWithKeyHashListParsed.getFirst());
        if (srcCv == null) {
            return IntegerReply.REPLY_0;
        }

        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
        if (isCrossRequestWorker) {
            // current net worker is src key slot's net worker
            var dstOneSlot = localPersist.oneSlot(dstSlotWithKeyHash.slot());

            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            var asyncReply = new AsyncReply(finalPromise);

            boolean finalReplace = replace;
            dstOneSlot.asyncRun(() -> {
                var dstCv = getCv(dstKeyBytes, dstSlotWithKeyHash);
                if (dstCv != null && !finalReplace) {
                    finalPromise.set(IntegerReply.REPLY_0);
                    return;
                }

                setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
                finalPromise.set(IntegerReply.REPLY_1);
            });

            return asyncReply;
        } else {
            var existCv = getCv(dstKeyBytes, dstSlotWithKeyHash);
            if (existCv != null && !replace) {
                return IntegerReply.REPLY_0;
            }

            setCv(dstKeyBytes, srcCv, dstSlotWithKeyHash);
            return IntegerReply.REPLY_1;
        }
    }
}
