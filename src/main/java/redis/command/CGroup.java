
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.SettablePromise;
import redis.BaseCommand;
import redis.MultiWorkerServer;
import redis.persist.LocalPersist;
import redis.reply.*;

import java.io.IOException;
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

            var s1 = slot(srcKeyBytes, slotNumber);
            var s2 = slot(dstKeyBytes, slotNumber);
            slotWithKeyHashList.add(s1);
            slotWithKeyHashList.add(s2);
            return slotWithKeyHashList;
        }

        if ("config".equals(cmd)) {
            // config always use the first slot
            var firstSlot = LocalPersist.getInstance().firstSlot();
            slotWithKeyHashList.add(new SlotWithKeyHash(firstSlot, 0, 1L));
            return slotWithKeyHashList;
        }

        // client can use any slot
        return slotWithKeyHashList;
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
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCommand = new String(data[1]).toLowerCase();
        if ("help".equals(subCommand)) {
            // todo
        }

        if ("set".equals(subCommand)) {
            if (data.length < 4) {
                return ErrorReply.SYNTAX;
            }

            var configKey = new String(data[2]).toLowerCase();
            var configValue = new String(data[3]);

            if ("max_connections".equals(configKey)) {
                int maxConnections;
                try {
                    maxConnections = Integer.parseInt(configValue);
                } catch (NumberFormatException e) {
                    return ErrorReply.INVALID_INTEGER;
                }
                MultiWorkerServer.staticGlobalV.socketInspector.setMaxConnections(maxConnections);
                log.warn("Global config set max_connections={}", maxConnections);

                var oneSlot = localPersist.currentThreadFirstOneSlot();
                try {
                    oneSlot.getDynConfig().update(configKey, maxConnections);
                } catch (IOException e) {
                    log.error("Global config update dyn config error", e);
                    return new ErrorReply("update dyn config error: " + e.getMessage());
                }
                return OKReply.INSTANCE;
            } else {
                // todo
            }
        }

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

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var srcCv = getCv(srcKeyBytes, slotWithKeyHash);
        if (srcCv == null) {
            return IntegerReply.REPLY_0;
        }

        var dstSlotWithKeyHash = slotWithKeyHashListParsed.getLast();
        if (isCrossRequestWorker) {
            // current net worker is src key slot's net worker
            var dstSlot = dstSlotWithKeyHash.slot();
            var dstOneSlot = localPersist.oneSlot(dstSlot);

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
