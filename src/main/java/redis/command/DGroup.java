
package redis.command;

import io.activej.net.socket.tcp.ITcpSocket;
import redis.BaseCommand;
import redis.CompressedValue;
import redis.reply.*;

import java.util.ArrayList;

public class DGroup extends BaseCommand {
    public DGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public static ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();
        slotWithKeyHashList.add(parseSlot(cmd, data, slotNumber));
        return slotWithKeyHashList;
    }

    public static SlotWithKeyHash parseSlot(String cmd, byte[][] data, int slotNumber) {
        if ("del".equals(cmd)) {
            if (data.length != 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        if ("decr".equals(cmd) || "decrby".equals(cmd)) {
            if (data.length < 2) {
                return null;
            }
            var keyBytes = data[1];
            return slot(keyBytes, slotNumber);
        }

        return null;
    }

    public Reply handle() {
        if ("del".equals(cmd)) {
            return del();
        }

        if ("dbsize".equals(cmd)) {
            return new IntegerReply(localPersist.getKeyCount());
        }

        if ("decr".equals(cmd)) {
            if (data.length != 2) {
                return ErrorReply.FORMAT;
            }

            return decrBy(1, 0);
        }

        if ("decrby".equals(cmd)) {
            if (data.length != 3) {
                return ErrorReply.FORMAT;
            }

            try {
                int by = Integer.parseInt(new String(data[2]));
                return decrBy(by, 0);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }
        }

        return NilReply.INSTANCE;
    }

    Reply del() {
        if (data.length == 2) {
            // need not lock, because it is always the same worker thread
            var keyBytes = data[1];
            var key = new String(keyBytes);

            var slotWithKeyHash = slotPreferParsed(keyBytes);
            var slot = slotWithKeyHash.slot();
            var bucketIndex = slotWithKeyHash.bucketIndex();
            var keyHash = slotWithKeyHash.keyHash();

            var oneSlot = localPersist.oneSlot(slot);

            // remove delay, perf better
            var isRemoved = oneSlot.remove(workerId, bucketIndex, key, keyHash, true);
            return isRemoved ? IntegerReply.REPLY_1 : IntegerReply.REPLY_0;
        }

        int[] nArr = {0};
        for (int i = 1; i < data.length; i++) {
            var keyBytes = data[i];
            if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
                return ErrorReply.KEY_TOO_LONG;
            }
            var key = new String(keyBytes);

            var slotWithKeyHash = slot(keyBytes);
            var slot = slotWithKeyHash.slot();
            var bucketIndex = slotWithKeyHash.bucketIndex();
            var keyHash = slotWithKeyHash.keyHash();

            var oneSlot = localPersist.oneSlot(slot);

            // remove delay, perf better
            var isRemoved = oneSlot.remove(workerId, bucketIndex, key, keyHash, true);
            if (isRemoved) {
                nArr[0]++;
            }
        }
        return new IntegerReply(nArr[0]);
    }

    Reply decrBy(int by, double byFloat) {
        var keyBytes = data[1];
        if (keyBytes.length > CompressedValue.KEY_MAX_LENGTH) {
            return ErrorReply.KEY_TOO_LONG;
        }

        var slotWithKeyHash = slotPreferParsed(keyBytes);

        boolean isByFloat = byFloat != 0;
        final var NOT_NUMBER_REPLY = isByFloat ? ErrorReply.NOT_FLOAT : ErrorReply.NOT_INTEGER;

        var cv = getCv(keyBytes, slotWithKeyHash);
        if (cv == null) {
            return NOT_NUMBER_REPLY;
        }

        long longValue = 0;
        double doubleValue = 0;
        if (cv.isTypeNumber()) {
            if (isByFloat) {
                doubleValue = cv.numberValue().doubleValue();
            } else {
                longValue = cv.numberValue().longValue();
            }
        } else {
            if (cv.isCompressed()) {
                return NOT_NUMBER_REPLY;
            }
            try {
                var numberStr = new String(cv.getCompressedData());
                if (isByFloat) {
                    doubleValue = Double.parseDouble(numberStr);
                } else {
                    longValue = Long.parseLong(numberStr);
                }
            } catch (NumberFormatException e) {
                return NOT_NUMBER_REPLY;
            }
        }

        if (isByFloat) {
            double newValue = doubleValue - byFloat;
            setNumber(keyBytes, newValue, slotWithKeyHash);
            // double use bulk reply
            return new BulkReply(String.valueOf(newValue).getBytes());
        } else {
            long newValue = longValue - by;
            setNumber(keyBytes, newValue, slotWithKeyHash);
            return new IntegerReply(newValue);
        }
    }
}
